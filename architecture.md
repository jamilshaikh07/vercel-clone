Using a bare-metal Kubernetes cluster running an immutable, API-driven OS like Talos is an exceptional choice for building a self-hosted Vercel clone. It provides a production-grade foundation that can easily handle dynamic provisioning.

However, moving this from a private lab to an environment exposed to students introduces unique challenges—specifically around **secure multi-tenancy**, **untrusted code execution**, and **safe public exposure**.

Here is a breakdown of how the architecture will function on an on-premise infrastructure.

---

## The On-Prem Architectural Flow

When a student pushes code, the system needs to orchestrate the build and deployment entirely within the local cluster while seamlessly routing traffic from the outside world.

```
[Student Push] ──> [GitHub Webhook] ──> [Control Plane API]
                                                │
                                                ▼
[Wildcard Subdomain] ──> [Edge Proxy] ──> [K8s Ingress] ──> [Student Pod]
                                                ▲
                                                │ (Deploys App)
                                        [Kaniko Builder Pod]

```

### 1. Safe Public Exposure (The Edge Layer)

Exposing an on-premise cluster directly via residential or local commercial ISP port-forwarding carries significant risk. Instead, a hybrid edge model secures the infrastructure:

* **The Setup:** Run a cheap, lightweight Cloud VPS (e.g., a $5 instance on DigitalOcean, Linode, or Hetzner) to act as a public-facing entry point, or utilize a Cloudflare Tunnel.
* **The Routing:** The VPS runs a reverse proxy (like Nginx or HAProxy) and connects back to the on-premise cluster via a secure overlay network (such as WireGuard or Tailscale).
* **Wildcard DNS:** Point `*.students.yourdomain.com` to the public IP of the Cloud VPS. The VPS simply forwards all incoming HTTP/HTTPS traffic down the tunnel to the cluster's Ingress Controller. This hides the local home lab IP completely.

### 2. The Ingress & Routing Layer

Inside the Talos cluster, an Ingress controller (like Ingress-NGINX or Traefik) intercepts the forwarded traffic.

* **Certificates:** Use `cert-manager` configured with the Let's Encrypt **DNS-01 challenge** (via your DNS provider's API, like Cloudflare). This allows the cluster to automatically provision a wildcard certificate (`*.students.yourdomain.com`) completely internally, without needing to expose port 80/443 directly to the internet for HTTP-01 validation.
* **Dynamic Routing:** When the control plane creates a new deployment for a student, it applies a standard Kubernetes `Ingress` resource mapping `student-app.students.yourdomain.com` to the corresponding K8s `Service`.

### 3. Secure Multi-Tenancy (The Build & Execution Layer)

Because students will write code that you do not control, the cluster must be hardened to prevent cross-tenant interference or host exploitation.

| Phase | Standard Approach | On-Prem Secure Approach | Why It Matters |
| --- | --- | --- | --- |
| **Image Building** | Docker-in-Docker (DinD) | **Kaniko** or **Cloud Native Buildpacks** | Standard Docker building requires a privileged container. Kaniko builds container images from code entirely within user-space, requiring no root privileges on the Talos host. |
| **Container Runtime** | Default `containerd` | **gVisor** or **Kata Containers** | Standard containers share the host kernel. If a student runs malicious code, they could escape the container. gVisor intercepts syscalls, sandboxing the application completely. |
| **Networking** | Flat Cluster Network | **Calico / Cilium NetworkPolicies** | Default K8s allows pods to talk to each other. NetworkPolicies must be applied to isolate namespaces so Student A's app cannot hit Student B's app or your home storage arrays. |
| **Resource Control** | No Limits | **ResourceQuotas & LimitRanges** | A simple infinite loop (`while(true)`) in a student's app could starve the entire cluster. Strict CPU and Memory limits ensure fair sharing. |

---

## How the Control Plane Interacts with Talos

The control plane backend (the application you write to manage the platform) will interact directly with the Kubernetes API.

1. **Webhook Triggers Job:** The API receives a GitHub webhook, generates a unique deployment ID, and launches a **Kubernetes Job** running Kaniko.
2. **Build and Push:** Kaniko clones the student's repository, builds the image, and pushes it to an internal container registry running inside the cluster (like Harbor or a basic registry chart).
3. **App Deployment:** Once the job succeeds, the API uses the K8s client SDK to programmatically apply three manifests into a dedicated namespace for that student:
* A **Deployment** (referencing the newly built image, restricted by gVisor/ResourceQuotas).
* A **Service** (to expose the pods internally).
* An **Ingress** (to tie the service to `app-id.students.yourdomain.com`).



This layout keeps the control plane incredibly lightweight, as Kubernetes handles the heavy lifting of scaling, routing, and healing the student applications.

How do you plan to handle the edge routing for this project—are you leaning toward a Cloudflare-style tunnel approach, or do you prefer maintaining a dedicated proxy VPS to bridge traffic into the cluster?