package main

// Tenant namespace reconciler.
//
// Each user gets exactly one Kubernetes namespace named
// paas-tenant-<sanitised-login>. The namespace carries:
//
//   * Pod Security Admission labels enforcing the `restricted` profile,
//     so tenants can't run privileged pods, host-network, hostPath,
//     etc., even if they somehow got a Deployment past us.
//   * A ResourceQuota capping cumulative CPU/RAM/pod-count, so one
//     tenant cannot drain a node.
//   * A LimitRange providing default + max per-pod limits, so a tenant
//     pod that doesn't set requests still gets sensible bounds and
//     cannot exceed per-pod ceilings.
//   * A default-deny NetworkPolicy with two explicit allows:
//       - ingress from kube-system (so Traefik can reach tenant pods)
//       - egress to the public internet (RFC1918 ranges are excluded)
//     This is what stops a malicious tenant from reaching paas-db,
//     the registry, or another tenant's pods.
//
// Reconciliation is idempotent — the worker calls ensureTenant before
// every deploy. The cost on the warm path is four GETs; everything is
// only POST/PUT when the desired and actual specs diverge (handled by
// k8s.apply's create-or-update logic).

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

const (
	tenantNamespacePrefix = "paas-tenant-"

	// Per-tenant ceilings. Conservative defaults sized for "static landing
	// pages + a few dynamic apps" on retired hardware. Operators can edit
	// these by hand on a namespace if a specific tenant needs more.
	tenantQuotaCPU       = "2"     // total requests across all pods
	tenantQuotaCPULimit  = "4"     // total limits across all pods
	tenantQuotaMemory    = "2Gi"   // total requests
	tenantQuotaMemLimit  = "4Gi"   // total limits
	tenantQuotaPods      = "20"    // hard cap on pod count
	tenantQuotaServices  = "10"
	tenantQuotaPVCs      = "5"
	tenantQuotaSecrets   = "30"    // most apps need only a handful

	// Per-pod defaults from LimitRange. Applied to any pod that doesn't
	// declare its own requests/limits — keeps "I forgot to set limits"
	// from blowing through ResourceQuota at scheduling time.
	tenantPodDefaultCPU       = "100m"
	tenantPodDefaultMem       = "128Mi"
	tenantPodMaxCPU           = "1"
	tenantPodMaxMem           = "1Gi"
	tenantPodDefaultReqCPU    = "20m"
	tenantPodDefaultReqMem    = "32Mi"
)

// tenantSlugSanitizer ensures the per-user namespace name is a valid
// DNS-1123 label. GitHub logins are mostly compatible (alphanumeric +
// hyphen, max 39 chars) but we lowercase + strip just in case.
var tenantSlugSanitizer = regexp.MustCompile(`[^a-z0-9-]+`)

// tenantNamespaceFor maps a GitHub login to its namespace name. The
// result is always a valid DNS-1123 label of <= 63 chars.
func tenantNamespaceFor(login string) string {
	s := strings.ToLower(strings.TrimSpace(login))
	s = tenantSlugSanitizer.ReplaceAllString(s, "")
	s = strings.Trim(s, "-")
	if s == "" {
		// Defensive: never produce paas-tenant- with empty suffix.
		s = "unknown"
	}
	// 63 - len("paas-tenant-") = 51 chars of room for the login. GitHub
	// caps logins at 39 so this only kicks in for adversarial input.
	if len(s) > 51 {
		s = s[:51]
	}
	return tenantNamespacePrefix + s
}

// ensureTenant idempotently creates/updates the namespace and all the
// safety nets that belong with it. Returns the namespace name. Errors
// are bubbled up — without isolation we must not deploy.
func (w *worker) ensureTenant(ctx context.Context, login string) (string, error) {
	ns := tenantNamespaceFor(login)
	if err := w.k8s.applyNamespace(ctx, ns, tenantNamespaceManifest(ns, login)); err != nil {
		return "", fmt.Errorf("apply namespace %s: %w", ns, err)
	}
	if err := w.k8s.applyResourceQuota(ctx, ns, "tenant", tenantResourceQuotaManifest(ns)); err != nil {
		return "", fmt.Errorf("apply quota: %w", err)
	}
	if err := w.k8s.applyLimitRange(ctx, ns, "tenant", tenantLimitRangeManifest(ns)); err != nil {
		return "", fmt.Errorf("apply limitrange: %w", err)
	}
	if err := w.k8s.applyNetworkPolicy(ctx, ns, "default-deny", tenantDefaultDenyNetworkPolicyManifest(ns)); err != nil {
		return "", fmt.Errorf("apply default-deny: %w", err)
	}
	if err := w.k8s.applyNetworkPolicy(ctx, ns, "allow-traefik-ingress", tenantAllowTraefikIngressManifest(ns)); err != nil {
		return "", fmt.Errorf("apply allow-traefik: %w", err)
	}
	if err := w.k8s.applyNetworkPolicy(ctx, ns, "allow-public-egress", tenantAllowPublicEgressManifest(ns)); err != nil {
		return "", fmt.Errorf("apply allow-egress: %w", err)
	}
	if err := w.mirrorRegistryPullSecret(ctx, ns); err != nil {
		return "", fmt.Errorf("mirror registry pull secret: %w", err)
	}
	return ns, nil
}

// mirrorRegistryPullSecret copies paas-system/registry-dockercfg into the
// tenant namespace, so the kubelet has credentials to pull tenant images
// from the in-cluster registry. The source Secret is small and stable,
// so we just read+write on every call instead of caching.
func (w *worker) mirrorRegistryPullSecret(ctx context.Context, tenantNS string) error {
	data, err := w.k8s.getSecretData(ctx, buildNamespace, dockerCfgSecret)
	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("source secret %s/%s not found", buildNamespace, dockerCfgSecret)
	}
	body, ok := data[".dockerconfigjson"]
	if !ok || len(body) == 0 {
		return fmt.Errorf("source secret %s/%s missing .dockerconfigjson", buildNamespace, dockerCfgSecret)
	}
	return w.k8s.applyDockerConfigSecret(ctx, tenantNS, dockerCfgSecret, body)
}

// --- Manifests ----------------------------------------------------------

func tenantNamespaceManifest(name, login string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": name,
			"labels": map[string]any{
				"app.kubernetes.io/managed-by": "control-plane",
				"paas.tenant.login":            sanitizeLabelValue(login),
				// Pod Security Admission. `restricted` is the strictest
				// built-in profile — no root, no privilege escalation,
				// no host* mounts, drops all caps except NET_BIND_SERVICE.
				"pod-security.kubernetes.io/enforce":         "restricted",
				"pod-security.kubernetes.io/enforce-version": "latest",
				"pod-security.kubernetes.io/audit":           "restricted",
				"pod-security.kubernetes.io/warn":            "restricted",
			},
		},
	}
}

// sanitizeLabelValue keeps a label compatible with Kubernetes' subset
// of RFC1123 + dot/underscore. Falls back to the empty string rather
// than rejecting the namespace on a weird login.
func sanitizeLabelValue(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s) && i < 63; i++ {
		c := s[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'
		if ok {
			out = append(out, c)
		}
	}
	return string(out)
}

func tenantResourceQuotaManifest(namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "ResourceQuota",
		"metadata": map[string]any{
			"name":      "tenant",
			"namespace": namespace,
		},
		"spec": map[string]any{
			"hard": map[string]any{
				"requests.cpu":           tenantQuotaCPU,
				"requests.memory":        tenantQuotaMemory,
				"limits.cpu":             tenantQuotaCPULimit,
				"limits.memory":          tenantQuotaMemLimit,
				"pods":                   tenantQuotaPods,
				"services":               tenantQuotaServices,
				"persistentvolumeclaims": tenantQuotaPVCs,
				"secrets":                tenantQuotaSecrets,
			},
		},
	}
}

func tenantLimitRangeManifest(namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "LimitRange",
		"metadata": map[string]any{
			"name":      "tenant",
			"namespace": namespace,
		},
		"spec": map[string]any{
			"limits": []any{
				map[string]any{
					"type": "Container",
					// Default limits when a container omits them. With a
					// hard ResourceQuota in place the API server would
					// otherwise refuse the pod entirely.
					"default": map[string]any{
						"cpu":    tenantPodDefaultCPU,
						"memory": tenantPodDefaultMem,
					},
					"defaultRequest": map[string]any{
						"cpu":    tenantPodDefaultReqCPU,
						"memory": tenantPodDefaultReqMem,
					},
					"max": map[string]any{
						"cpu":    tenantPodMaxCPU,
						"memory": tenantPodMaxMem,
					},
				},
			},
		},
	}
}

// tenantDefaultDenyNetworkPolicyManifest denies all ingress + egress.
// The two policies below punch the minimum holes required.
func tenantDefaultDenyNetworkPolicyManifest(namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata": map[string]any{
			"name":      "default-deny",
			"namespace": namespace,
		},
		"spec": map[string]any{
			"podSelector": map[string]any{},
			"policyTypes": []any{"Ingress", "Egress"},
		},
	}
}

// tenantAllowTraefikIngressManifest lets Traefik reach tenant pods on
// the conventional 8080 application port. Traefik's pods live in
// kube-system in default Talos installs; we select by namespace name.
func tenantAllowTraefikIngressManifest(namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata": map[string]any{
			"name":      "allow-traefik-ingress",
			"namespace": namespace,
		},
		"spec": map[string]any{
			"podSelector": map[string]any{},
			"policyTypes": []any{"Ingress"},
			"ingress": []any{
				map[string]any{
					"from": []any{
						map[string]any{
							"namespaceSelector": map[string]any{
								"matchLabels": map[string]any{
									"kubernetes.io/metadata.name": "kube-system",
								},
							},
						},
					},
					"ports": []any{
						map[string]any{"protocol": "TCP", "port": tenantPort},
					},
				},
			},
		},
	}
}

// tenantAllowPublicEgressManifest allows tenant pods to reach DNS in
// kube-system and the public internet, but blocks RFC1918 ranges so a
// malicious tenant cannot reach paas-db, the registry, the kube
// apiserver, or another tenant's pods.
func tenantAllowPublicEgressManifest(namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata": map[string]any{
			"name":      "allow-public-egress",
			"namespace": namespace,
		},
		"spec": map[string]any{
			"podSelector": map[string]any{},
			"policyTypes": []any{"Egress"},
			"egress": []any{
				// DNS — needed for any outbound name resolution.
				map[string]any{
					"to": []any{
						map[string]any{
							"namespaceSelector": map[string]any{
								"matchLabels": map[string]any{
									"kubernetes.io/metadata.name": "kube-system",
								},
							},
						},
					},
					"ports": []any{
						map[string]any{"protocol": "UDP", "port": 53},
						map[string]any{"protocol": "TCP", "port": 53},
					},
				},
				// Public internet on standard ports. The except blocks
				// cover both the cluster's pod/service CIDRs (which are
				// in 10.x by default) and the homelab LAN.
				map[string]any{
					"to": []any{
						map[string]any{
							"ipBlock": map[string]any{
								"cidr": "0.0.0.0/0",
								"except": []any{
									"10.0.0.0/8",
									"172.16.0.0/12",
									"192.168.0.0/16",
									"169.254.0.0/16", // link-local + AWS-style metadata
								},
							},
						},
					},
				},
			},
		},
	}
}
