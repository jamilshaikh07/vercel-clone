package main

// Minimal in-cluster Kubernetes client.
//
// We deliberately do NOT depend on k8s.io/client-go — that's ~30MB of
// transitive deps and we only need to POST/GET 4 resource kinds. This
// file is ~200 lines and handles auth, TLS, and JSON marshaling using
// only the standard library.

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
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

	return &kubeClient{
		base:      "https://" + apiHost,
		token:     strings.TrimSpace(string(tok)),
		namespace: strings.TrimSpace(string(nsB)),
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig:     &tls.Config{RootCAs: pool},
				MaxIdleConns:        16,
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
			},
		},
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

func (k *kubeClient) createJob(ctx context.Context, namespace string, job map[string]any) error {
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs", url.PathEscape(namespace))
	status, body, err := k.do(ctx, "POST", path, job)
	if err != nil {
		return err
	}
	if status == http.StatusConflict {
		// A leftover Job with this name still exists. Try to delete it
		// (with foreground propagation) and retry once.
		name, _ := nestedString(job, "metadata", "name")
		if name == "" {
			return errors.New("job conflict but no metadata.name to retry")
		}
		if err := k.deleteJob(ctx, namespace, name); err != nil {
			return fmt.Errorf("delete stale job for retry: %w", err)
		}
		// Wait briefly for finalization.
		time.Sleep(2 * time.Second)
		status, body, err = k.do(ctx, "POST", path, job)
		if err != nil {
			return err
		}
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

func (k *kubeClient) applyService(ctx context.Context, namespace, name string, obj map[string]any) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/services", url.PathEscape(namespace))
	return k.apply(ctx, path, name, obj)
}

func (k *kubeClient) applyIngressRoute(ctx context.Context, namespace, name string, obj map[string]any) error {
	// Traefik CRD — GVR: traefik.io/v1alpha1, ingressroutes
	path := fmt.Sprintf("/apis/traefik.io/v1alpha1/namespaces/%s/ingressroutes", url.PathEscape(namespace))
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
