package main

// Minimal in-cluster Kubernetes client.
//
// We deliberately do NOT depend on k8s.io/client-go — that's ~30MB of
// transitive deps and we only need to POST/GET 4 resource kinds. This
// file is ~200 lines and handles auth, TLS, and JSON marshaling using
// only the standard library.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	saCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	saNSPath    = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	apiHost     = "kubernetes.default.svc"
)

type kubeClient struct {
	base      string
	token     string
	namespace string
	http      *http.Client
	// streamHTTP is used for long-lived watch / log-follow requests. It
	// shares TLS config with http but has no overall timeout so a build
	// taking 5 minutes doesn't get cut off mid-stream.
	streamHTTP *http.Client
}

func newInClusterClient() (*kubeClient, error) {
	tok, err := os.ReadFile(saTokenPath)
	if err != nil {
		return nil, fmt.Errorf("read SA token: %w", err)
	}
	ca, err := os.ReadFile(saCAPath)
	if err != nil {
		return nil, fmt.Errorf("read SA CA: %w", err)
	}
	nsB, err := os.ReadFile(saNSPath)
	if err != nil {
		return nil, fmt.Errorf("read SA namespace: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("could not parse SA CA bundle")
	}

	tlsCfg := &tls.Config{RootCAs: pool}
	transport := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
	}
	streamTransport := &http.Transport{
		TLSClientConfig:       tlsCfg,
		MaxIdleConns:          8,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
	}
	return &kubeClient{
		base:       "https://" + apiHost,
		token:      strings.TrimSpace(string(tok)),
		namespace:  strings.TrimSpace(string(nsB)),
		http:       &http.Client{Timeout: 30 * time.Second, Transport: transport},
		streamHTTP: &http.Client{Transport: streamTransport},
	}, nil
}

// do executes a request against the kube API. body may be nil. status is
// returned even for non-2xx so callers can act on 404 vs 409 vs anything else.
func (k *kubeClient) do(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal body: %w", err)
		}
		reader = bytes.NewReader(buf)
	}
	u := k.base + path
	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := k.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}

// apply emulates `kubectl apply` for a single object: try create, on 409
// fetch the current resourceVersion and PUT to update. Good enough for
// our small set of resources without needing server-side apply.
func (k *kubeClient) apply(ctx context.Context, collectionPath, name string, obj map[string]any) error {
	status, body, err := k.do(ctx, "POST", collectionPath, obj)
	if err != nil {
		return err
	}
	if status >= 200 && status < 300 {
		return nil
	}
	if status != http.StatusConflict {
		return fmt.Errorf("create %s: status %d: %s", collectionPath, status, snippet(body))
	}
	// Conflict — fetch, copy resourceVersion, PUT.
	getPath := collectionPath + "/" + url.PathEscape(name)
	getStatus, getBody, err := k.do(ctx, "GET", getPath, nil)
	if err != nil {
		return err
	}
	if getStatus != http.StatusOK {
		return fmt.Errorf("get %s for update: status %d: %s", getPath, getStatus, snippet(getBody))
	}
	var existing map[string]any
	if err := json.Unmarshal(getBody, &existing); err != nil {
		return fmt.Errorf("decode existing: %w", err)
	}
	md, _ := existing["metadata"].(map[string]any)
	rv, _ := md["resourceVersion"].(string)
	mdNew, _ := obj["metadata"].(map[string]any)
	mdNew["resourceVersion"] = rv
	obj["metadata"] = mdNew

	putStatus, putBody, err := k.do(ctx, "PUT", getPath, obj)
	if err != nil {
		return err
	}
	if putStatus < 200 || putStatus >= 300 {
		return fmt.Errorf("update %s: status %d: %s", getPath, putStatus, snippet(putBody))
	}
	return nil
}

// --- Job ----------------------------------------------------------------

type jobPhase struct {
	Succeeded   int32
	Failed      int32
	Active      int32
	FailureMsg  string
	CompletedAt *time.Time
}

// createJob returns os.ErrExist (well, errJobExists) on 409 so the caller
// can decide whether to attach to it or replace it. Jobs are immutable
// after create which is why we don't use apply() here.
var errJobExists = errors.New("job already exists")

func (k *kubeClient) createJob(ctx context.Context, namespace string, job map[string]any) error {
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs", url.PathEscape(namespace))
	status, body, err := k.do(ctx, "POST", path, job)
	if err != nil {
		return err
	}
	if status == http.StatusConflict {
		return errJobExists
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("create job: status %d: %s", status, snippet(body))
	}
	return nil
}

func (k *kubeClient) deleteJob(ctx context.Context, namespace, name string) error {
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s?propagationPolicy=Foreground",
		url.PathEscape(namespace), url.PathEscape(name))
	status, body, err := k.do(ctx, "DELETE", path, nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("delete job: status %d: %s", status, snippet(body))
	}
	return nil
}

// getJobPhase returns counters that let the caller decide
// succeeded/failed/running. Returns nil if the Job doesn't exist yet.
func (k *kubeClient) getJobPhase(ctx context.Context, namespace, name string) (*jobPhase, error) {
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s",
		url.PathEscape(namespace), url.PathEscape(name))
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("get job: status %d: %s", status, snippet(body))
	}
	var j struct {
		Status struct {
			Succeeded      int32      `json:"succeeded"`
			Failed         int32      `json:"failed"`
			Active         int32      `json:"active"`
			CompletionTime *time.Time `json:"completionTime,omitempty"`
			Conditions     []struct {
				Type    string `json:"type"`
				Status  string `json:"status"`
				Reason  string `json:"reason"`
				Message string `json:"message"`
			} `json:"conditions,omitempty"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &j); err != nil {
		return nil, fmt.Errorf("decode job: %w", err)
	}
	p := &jobPhase{
		Succeeded:   j.Status.Succeeded,
		Failed:      j.Status.Failed,
		Active:      j.Status.Active,
		CompletedAt: j.Status.CompletionTime,
	}
	for _, c := range j.Status.Conditions {
		if c.Type == "Failed" && c.Status == "True" {
			p.FailureMsg = strings.TrimSpace(c.Reason + ": " + c.Message)
		}
	}
	return p, nil
}

// --- Secret -------------------------------------------------------------

// getSecretData reads a Secret and returns its base64-decoded `data` map.
// Returns (nil, nil) when the Secret doesn't exist.
func (k *kubeClient) getSecretData(ctx context.Context, namespace, name string) (map[string][]byte, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/secrets/%s",
		url.PathEscape(namespace), url.PathEscape(name))
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("get secret %s/%s: status %d: %s", namespace, name, status, snippet(body))
	}
	var s struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("decode secret: %w", err)
	}
	out := make(map[string][]byte, len(s.Data))
	for k, v := range s.Data {
		dec, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("decode key %s: %w", k, err)
		}
		out[k] = dec
	}
	return out, nil
}

// applyDockerConfigSecret creates a kubernetes.io/dockerconfigjson Secret
// in the target namespace, used by tenant pods' imagePullSecrets. Idempotent.
func (k *kubeClient) applyDockerConfigSecret(ctx context.Context, namespace, name string, dockerConfigJSON []byte) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/secrets", url.PathEscape(namespace))
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"type":       "kubernetes.io/dockerconfigjson",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels": map[string]any{
				"app.kubernetes.io/managed-by": "control-plane",
				"app.kubernetes.io/component":  "registry-pull",
			},
		},
		"data": map[string]any{
			".dockerconfigjson": base64.StdEncoding.EncodeToString(dockerConfigJSON),
		},
	}
	return k.apply(ctx, path, name, obj)
}

func (k *kubeClient) applySecret(ctx context.Context, namespace, name string, stringData map[string]string, labels map[string]string) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/secrets", url.PathEscape(namespace))
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"type":       "Opaque",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels":    labels,
		},
		"stringData": stringData,
	}
	return k.apply(ctx, path, name, obj)
}

// --- Deployment / Service / IngressRoute --------------------------------

func (k *kubeClient) applyDeployment(ctx context.Context, namespace, name string, obj map[string]any) error {
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments", url.PathEscape(namespace))
	return k.apply(ctx, path, name, obj)
}

// listDeployments returns the names of Deployments in `namespace` that
// match `labelSelector` (e.g. "paas.project=my-app"). Used by the
// Start/Stop endpoint to find every per-commit Deployment belonging to
// one project so we can scale them as a unit.
func (k *kubeClient) listDeployments(ctx context.Context, namespace, labelSelector string) ([]string, error) {
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments?labelSelector=%s",
		url.PathEscape(namespace), url.QueryEscape(labelSelector))
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list deployments: kube returned %d: %s", status, string(body))
	}
	var resp struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode deployments list: %w", err)
	}
	out := make([]string, 0, len(resp.Items))
	for _, it := range resp.Items {
		out = append(out, it.Metadata.Name)
	}
	return out, nil
}

// patchDeploymentScale sets a Deployment's replica count via the /scale
// subresource. Using the subresource (rather than mutating the full
// Deployment spec) sidesteps resourceVersion races and is the documented
// path for scaling — it's what `kubectl scale` does under the hood.
func (k *kubeClient) patchDeploymentScale(ctx context.Context, namespace, name string, replicas int) error {
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s/scale",
		url.PathEscape(namespace), url.PathEscape(name))
	obj := map[string]any{
		"apiVersion": "autoscaling/v1",
		"kind":       "Scale",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"spec": map[string]any{"replicas": replicas},
	}
	status, body, err := k.do(ctx, "PUT", path, obj)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("scale deployment: kube returned %d: %s", status, string(body))
	}
	return nil
}

// getDeploymentReplicas reads the current spec.replicas on a Deployment.
// Returns (0, false, nil) if the deployment doesn't exist — callers use
// the existence bool to distinguish "stopped" from "never deployed".
func (k *kubeClient) getDeploymentReplicas(ctx context.Context, namespace, name string) (int, bool, error) {
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s",
		url.PathEscape(namespace), url.PathEscape(name))
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return 0, false, err
	}
	if status == http.StatusNotFound {
		return 0, false, nil
	}
	if status != http.StatusOK {
		return 0, false, fmt.Errorf("get deployment: kube returned %d: %s", status, string(body))
	}
	var resp struct {
		Spec struct {
			Replicas int `json:"replicas"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, false, fmt.Errorf("decode deployment: %w", err)
	}
	return resp.Spec.Replicas, true, nil
}

func (k *kubeClient) applyService(ctx context.Context, namespace, name string, obj map[string]any) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/services", url.PathEscape(namespace))
	return k.apply(ctx, path, name, obj)
}

func (k *kubeClient) applyIngressRoute(ctx context.Context, namespace, name string, obj map[string]any) error {
	// Traefik CRD — GVR: traefik.io/v1alpha1, ingressroutes
	path := fmt.Sprintf("/apis/traefik.io/v1alpha1/namespaces/%s/ingressroutes", url.PathEscape(namespace))
	return k.apply(ctx, path, name, obj)
}

// --- Namespace / Quota / NetworkPolicy (tenant isolation, Slice B) ------

func (k *kubeClient) applyNamespace(ctx context.Context, name string, obj map[string]any) error {
	return k.apply(ctx, "/api/v1/namespaces", name, obj)
}

func (k *kubeClient) applyResourceQuota(ctx context.Context, namespace, name string, obj map[string]any) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/resourcequotas", url.PathEscape(namespace))
	return k.apply(ctx, path, name, obj)
}

func (k *kubeClient) applyLimitRange(ctx context.Context, namespace, name string, obj map[string]any) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/limitranges", url.PathEscape(namespace))
	return k.apply(ctx, path, name, obj)
}

func (k *kubeClient) applyNetworkPolicy(ctx context.Context, namespace, name string, obj map[string]any) error {
	path := fmt.Sprintf("/apis/networking.k8s.io/v1/namespaces/%s/networkpolicies", url.PathEscape(namespace))
	return k.apply(ctx, path, name, obj)
}

// --- helpers ------------------------------------------------------------

func nestedString(m map[string]any, keys ...string) (string, bool) {
	var cur any = m
	for _, k := range keys {
		mm, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur = mm[k]
	}
	s, ok := cur.(string)
	return s, ok
}

func snippet(b []byte) string {
	if len(b) > 400 {
		b = b[:400]
	}
	return strings.TrimSpace(string(b))
}

// --- Pod lookup + log streaming -----------------------------------------

// containerState is the minimal slice of pod.status.containerStatuses we care
// about: whether a given container is still pending startup, currently
// running, or already terminated.
type containerState struct {
	Waiting    bool
	Running    bool
	Terminated bool
	Reason     string
}

// findBuildPod returns the build pod name for a deployment, or "" if no
// pod with the matching paas.deployment label exists yet. Errors only on
// API transport / auth problems.
func (k *kubeClient) findBuildPod(ctx context.Context, namespace, deploymentID string) (string, error) {
	sel := fmt.Sprintf("paas.deployment=%s,app.kubernetes.io/component=build", deploymentID)
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s",
		url.PathEscape(namespace), url.QueryEscape(sel))
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return "", err
	}
	if status < 200 || status >= 300 {
		return "", fmt.Errorf("list pods: status %d: %s", status, snippet(body))
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return "", fmt.Errorf("decode pods: %w", err)
	}
	if len(list.Items) == 0 {
		return "", nil
	}
	// Most recent pod wins — relevant if we ever recreate the Job.
	best := list.Items[0]
	for _, it := range list.Items[1:] {
		if it.Metadata.CreationTimestamp.After(best.Metadata.CreationTimestamp) {
			best = it
		}
	}
	return best.Metadata.Name, nil
}

// findTenantPod returns the most relevant tenant pod for a deployment, or
// "" if none exists yet. It prefers Running pods (the rollout has landed)
// over Pending ones (still being created); among equal candidates the
// most-recently-created wins. Errors only on API transport/auth problems.
//
// Counterpart to findBuildPod; same label scheme but the tenant component
// and the tenant namespace are different.
func (k *kubeClient) findTenantPod(ctx context.Context, namespace, deploymentID string) (string, error) {
	sel := fmt.Sprintf("paas.deployment=%s,app.kubernetes.io/component=tenant", deploymentID)
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s",
		url.PathEscape(namespace), url.QueryEscape(sel))
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return "", err
	}
	if status < 200 || status >= 300 {
		return "", fmt.Errorf("list pods: status %d: %s", status, snippet(body))
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
				DeletionTimestamp *time.Time `json:"deletionTimestamp,omitempty"`
			} `json:"metadata"`
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return "", fmt.Errorf("decode pods: %w", err)
	}
	if len(list.Items) == 0 {
		return "", nil
	}
	// Score: Running > Pending; never pick Terminating pods.
	bestIdx := -1
	bestRank := -1
	for i, it := range list.Items {
		if it.Metadata.DeletionTimestamp != nil {
			continue
		}
		rank := 0
		if it.Status.Phase == "Running" {
			rank = 2
		} else if it.Status.Phase == "Pending" {
			rank = 1
		}
		if bestIdx < 0 || rank > bestRank ||
			(rank == bestRank && it.Metadata.CreationTimestamp.After(list.Items[bestIdx].Metadata.CreationTimestamp)) {
			bestIdx = i
			bestRank = rank
		}
	}
	if bestIdx < 0 {
		return "", nil
	}
	return list.Items[bestIdx].Metadata.Name, nil
}

// containerStatus reads pod.status.{init,}containerStatuses and returns the
// state of one named container.
func (k *kubeClient) containerStatus(ctx context.Context, namespace, pod, container string) (*containerState, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s",
		url.PathEscape(namespace), url.PathEscape(pod))
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("get pod: status %d: %s", status, snippet(body))
	}
	var p struct {
		Status struct {
			InitContainerStatuses []podContainerStatus `json:"initContainerStatuses"`
			ContainerStatuses     []podContainerStatus `json:"containerStatuses"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("decode pod: %w", err)
	}
	all := append([]podContainerStatus{}, p.Status.InitContainerStatuses...)
	all = append(all, p.Status.ContainerStatuses...)
	for _, cs := range all {
		if cs.Name != container {
			continue
		}
		st := &containerState{}
		if cs.State.Waiting != nil {
			st.Waiting = true
			st.Reason = cs.State.Waiting.Reason
		}
		if cs.State.Running != nil {
			st.Running = true
		}
		if cs.State.Terminated != nil {
			st.Terminated = true
			st.Reason = cs.State.Terminated.Reason
		}
		return st, nil
	}
	return nil, nil
}

type podContainerStatus struct {
	Name  string `json:"name"`
	State struct {
		Waiting    *struct{ Reason string `json:"reason"` } `json:"waiting,omitempty"`
		Running    *struct{}                                `json:"running,omitempty"`
		Terminated *struct{ Reason string `json:"reason"` } `json:"terminated,omitempty"`
	} `json:"state"`
}

// streamPodLog tails one container's stdout/stderr until the container
// terminates or ctx is cancelled. onLine is called for each line (without
// the trailing newline). The function blocks until done; it retries with a
// short backoff while the container is still in "Waiting" state, so callers
// can invoke it immediately after pod creation.
func (k *kubeClient) streamPodLog(ctx context.Context, namespace, pod, container string, onLine func(string)) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?follow=true&container=%s&timestamps=false",
		url.PathEscape(namespace), url.PathEscape(pod), url.QueryEscape(container))
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		req, err := http.NewRequestWithContext(ctx, "GET", k.base+path, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+k.token)
		// Deliberately no Accept header — the log endpoint returns text/plain
		// on success, but kube-apiserver content-negotiation refuses any
		// non-JSON Accept value with 406 NotAcceptable on the error path.

		resp, err := k.streamHTTP.Do(req)
		if err != nil {
			return err
		}
		// Container not started yet ⇒ 400 with body
		// `container "x" in pod "y" is waiting to start: ContainerCreating`.
		// Retry with backoff until it transitions out of Waiting.
		if resp.StatusCode == http.StatusBadRequest {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if !strings.Contains(string(b), "waiting to start") {
				return fmt.Errorf("log stream: status 400: %s", snippet(b))
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1500 * time.Millisecond):
			}
			continue
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("log stream: status %d: %s", resp.StatusCode, snippet(b))
		}

		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		for sc.Scan() {
			onLine(sc.Text())
		}
		err = sc.Err()
		resp.Body.Close()
		// follow=true returns EOF when the container terminates. If we got
		// here without a scanner error, that's normal completion.
		return err
	}
}
