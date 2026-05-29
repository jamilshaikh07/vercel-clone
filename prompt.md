Here is a comprehensive master prompt designed to instruct an AI—or an autonomous agent framework—to architect and build a self-hosted Vercel alternative. It is heavily geared toward modern CI/CD workflows and container orchestration.

You can copy and paste this directly into your AI tool of choice:

---

**System Role:** Act as a Principal Site Reliability Engineer (SRE) and Lead Platform Architect. Your objective is to design, architect, and write the foundational code for a self-hosted PaaS (Platform as a Service) that replicates the core deployment experience of Vercel.

**Project Overview:**
The platform must allow users to authenticate, connect a GitHub repository, and trigger automatic deployments upon a `git push`. The system will build the code, containerize it, and dynamically deploy it to a Kubernetes cluster, issuing a live URL and streaming the build logs in real-time.

**Phase 1: Architecture & Infrastructure Design**
Design the system using the following microservices architecture:

1. **API & Control Plane:** A backend service (Node.js/TypeScript or Go) handling GitHub webhooks, OAuth, and database interactions (PostgreSQL).
2. **Dashboard UI:** A Next.js frontend displaying projects, deployment histories, and live log streams.
3. **Build Fleet:** A scalable worker service connected to a message queue (Redis/BullMQ) that clones repos, detects frameworks, builds Docker images, and pushes them to a private container registry.
4. **Orchestration & Routing:** A Kubernetes-native deployment engine. The API must interact with the Kubernetes API to dynamically spin up Deployments and Services, updating an Ingress controller (e.g., Traefik or Nginx) to route wildcard subdomains (`*.yourdomain.com`) to the active pods. The K8s configurations should be optimized for a bare-metal environment (e.g., Talos Linux).

**Phase 2: Core Execution & Code Generation**
Please provide the following deliverables:

1. **Mermaid.js Diagram:** Map out the entire flow from a developer's `git push` to the live URL routing via K8s Ingress.
2. **Webhook & Queue Logic (API):** Write the backend code to receive the GitHub push event, extract the commit data, and push a structured job to the Redis queue.
3. **The Builder Worker (Build Service):** Write the core execution logic for the worker node that pulls the job, runs a standardized Docker build (using buildpacks or a standard Dockerfile template), and streams the `stdout/stderr` logs back to a WebSocket server.
4. **Dynamic K8s Manifests:** Provide the templated Kubernetes YAML files (Deployment, Service, Ingress) that the control plane will dynamically apply to the cluster for each new deployment.

**Constraints & Best Practices:**

* Write highly modular, production-ready code.
* Ensure the infrastructure design allows for high availability and clean teardowns of old deployment pods.
* Focus strictly on the automated pipeline and routing mechanics; skip basic boilerplate like user login UIs.

---