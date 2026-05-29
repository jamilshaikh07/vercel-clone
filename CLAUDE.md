# CLAUDE.md — guide for AI coding agents working in this repo

This file is the orientation an AI agent should read **first** before
touching code. It captures the invariants, conventions, and landmines
that aren't obvious from the file tree.

---

## What this project is

A self-hosted Vercel-style PaaS targeting a bare-metal Talos Linux
Kubernetes cluster. The goal is "developer pushes to GitHub → app
appears at a URL, no human in the loop." We are mid-build; the
end-to-end automation is not yet wired up (see the **What is NOT done**
section below before assuming a feature works).

Read `README.md` first for the architecture diagram and component
inventory. Read `architecture.md` for design decisions and trade-offs.
Read `prompt.md` for the original brief that scoped the project.

---

## Cluster context you cannot infer from the code

These are facts about the live cluster the manifests run against:

- **Kube context:** `homelab-cluster` (kubeconfig: `~/.kube/config-homelab`)
- **Storage class:** `local-path` is the default
- **CNPG operator** runs in `cnpg-system`, already installed
- **Traefik** is the ingress controller; we use the `traefik.io/v1alpha1`
  `IngressRoute` CRD — not stock `Ingress`
- **external-dns** watches `IngressRoute` Host rules and writes Cloudflare CNAME
  records to a fixed tunnel target via the annotation:
  ```
  external-dns.alpha.kubernetes.io/target: "3a067db9-77b1-49c9-a3d4-30f86d16c80d.cfargotunnel.com"
  ```
  Every public-facing `IngressRoute` we create needs this annotation.
- **Cloudflare Tunnel** terminates TLS at the Cloudflare edge; in-cluster
  Traefik only handles HTTP on the `web` entrypoint.
- **Namespaces and PSA levels:**
  | Namespace | PSA | Purpose |
  |---|---|---|
  | `paas-system` | `baseline` | Control plane + builders (Kaniko needs syscalls restricted PSA forbids) |
  | `paas-deployments` | `restricted` | Tenant workloads — keep this tight |

If you create a tenant `Deployment` in `paas-deployments`, you MUST set
all of: `runAsNonRoot: true`, numeric `runAsUser`, `allowPrivilegeEscalation: false`,
`capabilities.drop: [ALL]`, `seccompProfile.type: RuntimeDefault`, and
`readOnlyRootFilesystem: true` where the app allows it. The control
plane manifest is a good template.

---

## Repo layout in one sentence

`control-plane/` is the Go service, `manifests/` is the cluster state
(apply in numerical order), `samples/` is throwaway test apps, `tools/`
is one-shot CLIs that augment the control plane.

---

## Go conventions

- Targets **Go 1.22** (`go.mod`). Toolchain on the dev box is 1.22.2.
  Always run builds with `GOTOOLCHAIN=local` to avoid the auto-download
  fetch that fails in this environment.
- Stdlib only where reasonable. The only third-party deps allowed
  currently are `github.com/jackc/pgx/v5/...`. Resist adding more.
- The control plane is **single-package `main`**. Don't split into
  internal packages until there's a clear second consumer.
- Migrations live in `control-plane/migrations/*.sql` and are embedded
  with `//go:embed`. They run on every boot and are tracked via the
  `schema_migrations` table. Always add new migrations as `NNNN_*.sql`
  with a strictly increasing prefix — never rewrite an applied one.
- Logging is `log/slog` JSON to stdout. Use structured fields, never
  printf-style format strings.

---

## Cluster conventions

- **Image tagging:** while we're on `ttl.sh`, generate a fresh
  `ttl.sh/jamilshaikh-paas-<component>-<random-8>:24h` for each new
  image and bump the tag in `manifests/`. The control plane Job's
  destination flag and the Deployment's `image` must match.
- **Secrets are NEVER in git.** Even in a `.template.yaml` file with
  placeholders, we don't keep templates of secret manifests — we use
  `kubectl create secret ... --dry-run=client -o yaml | kubectl apply -f -`
  for the create path, and CNPG-managed secrets where the operator owns
  the values.
- **CNPG secrets:** the operator creates `<cluster>-app` automatically
  with the keys `uri`, `username`, `password`, `host`, `port`, `dbname`.
  Reference `uri` for `DATABASE_URL`.

---

## Big landmines (read these before doing similar work)

### 1. The `tools/out/` secrets leak that happened on 2026-05-29

`setup-gh-app` writes the GitHub App private key + webhook secret +
client secret to `tools/out/`. The first version of the `.gitignore`
rule lived inside `tools/setup-gh-app/.gitignore`, NOT at the repo root.
A `git add .` from the repo root committed everything in `tools/out/`
to a **public** GitHub repo. The leak window was ~60 seconds before
force-push, but the credentials still had to be fully rotated (delete
GitHub App, recreate).

**Lesson:** any file the build process or a helper tool generates that
contains a secret must be ignored at the **repo root**, not in a
subdirectory. The root `.gitignore` now has:

```
tools/out/
tools/setup-gh-app/setup-gh-app
**/*.pem
**/*-private-key*
```

Do not weaken these. If you add a new tool that writes secrets,
add its output dir to the root `.gitignore` in the same PR.

### 2. PSA `restricted` requires numeric `runAsUser`

Distroless `nonroot` images declare `USER nonroot` (a name). The
kubelet, when enforcing `runAsNonRoot: true`, cannot resolve the name
without running the image, so it refuses to start the container with
`container has runAsNonRoot and image has non-numeric user (nonroot)`.

Set `runAsUser: 65532` and `runAsGroup: 65532` explicitly for
distroless `nonroot` images.

### 3. Kaniko on Talos needs PSA `baseline` (not `restricted`)

Kaniko unpacks rootfs and runs the `RUN` directives, which requires
broader filesystem access than `restricted` allows. That's why the
build namespace is `paas-system` with `baseline`, and tenant workloads
live in `paas-deployments` with `restricted`. Don't mix them.

### 4. GitHub App manifest flow specifics

When automating with `tools/setup-gh-app`:

- `hook_attributes.active` must be a JSON `true`, not the string `"true"`.
- `installation` and `installation_repositories` cannot appear in
  `default_events` — they're auto-delivered to every App.
- The webhook secret we pre-set in K8s is NOT honored by the manifest
  flow; GitHub generates one and returns it. We must reconcile by
  overwriting `paas-system/github-webhook` with the returned value.

### 5. `external-dns` annotation is mandatory

Public `IngressRoute`s without the
`external-dns.alpha.kubernetes.io/target` annotation will be created in
the cluster, route correctly internally, and never get a public DNS
record. Symptom: `dig +short host.jamilshaikh.in` returns nothing.

### 6. Kaniko 1.23.x mishandles URL-embedded HTTPS credentials

`--context=git://x-access-token:TOKEN@github.com/...#SHA` parses but
silently drops the auth, then fails with `error resolving source
context: authentication required`. Verified with `alpine/git` that
the same URL clones fine. **Workaround in place:** the build Job uses
an `alpine/git` initContainer that clones into an `emptyDir`, then
Kaniko builds with `--context=dir:///workspace`. Don't revert to
Kaniko's native git fetcher unless you've re-verified the bug is fixed
upstream.

### 7. Tenant port is hardcoded to 8080

`builder.go` const `tenantPort = 8080`. Most rootless images
(`nginx-unprivileged`, distroless, etc.) listen on 8080+. Repos that
use a different port will deploy but their `Service`/`IngressRoute`
won't route. Detect from the built image's `Config.ExposedPorts` once
we have a real registry — `ttl.sh` doesn't have a stable manifest API
worth bothering with.

### 8. Worker single-replica assumption

The `deployments` table has `FOR UPDATE SKIP LOCKED` semantics so the
schema is multi-worker-safe, but during a rollout the old pod's worker
can race the new one and strand a row at `building`. `RequeueStale`
fires every tick to rescue these after `buildTimeout` (15 min).
If we run >1 replica, expect occasional re-builds when both workers
target the same Job — the `ensureBuildJob` path handles it via attach.

### 9. `samples/hello-app` is part of THIS monorepo

There used to be a nested `.git` inside `samples/hello-app/` linked to
`github.com/jamilshaikh07/paas-sample-hello`. It was removed when this
repo was initialized. If you need to push the sample app to the
external repo, `git clone git@github.com:jamilshaikh07/paas-sample-hello.git`
into a tempdir first — do NOT add it as a submodule (that breaks the
Kaniko build context).

---

## What is and is NOT done

### Done
- HMAC-verified webhook ingestion
- Postgres persistence of installations, repos, projects, deliveries, deployments
- GitHub App manifest flow automation (`tools/setup-gh-app`)
- **Full push → build → deploy automation.** A push event creates a `queued` row;
  the in-process worker (`control-plane/builder.go`) claims it, mints a GitHub
  installation token, creates a Kaniko Job (with an `alpine/git` initContainer
  doing the clone), pushes the image to ttl.sh, then applies
  Deployment + Service + IngressRoute and marks `ready` with the live URL.
- Idempotent build path: `ensureBuildJob` attaches to existing Jobs across
  worker restarts; `RequeueStale` periodically rescues stranded rows.
- Three live deployments at `<slug>-<short-sha>.jamilshaikh.in` proving E2E.

### NOT done — don't claim these exist
- Real container registry. We use `ttl.sh` with 24h TTL — images expire daily.
- TLS issuance via cert-manager. We rely on Cloudflare Universal SSL
  at the tunnel edge.
- Auth on `/admin/*`. The endpoint is open to anyone who can hit
  `paas.jamilshaikh.in`. Add IP allow-list / basic-auth / OIDC before
  this goes anywhere near production.
- Build log streaming, framework auto-detect, dashboard UI, multi-region.

---

## Useful one-liners

```bash
# Tail control plane logs (excluding kube-probes)
kubectl -n paas-system logs deploy/control-plane -f --tail=50 | grep -v kube-probe

# psql into the CNPG primary as superuser (peer auth)
kubectl -n paas-system exec -it paas-db-1 -c postgres -- psql -U postgres -d paas

# Show recent webhook deliveries
kubectl -n paas-system exec paas-db-1 -c postgres -- psql -U postgres -d paas -c \
  "SELECT delivery_id, event, action, repo_full_name, received_at FROM webhook_deliveries ORDER BY received_at DESC LIMIT 20"

# Recent deployments
curl -s https://paas.jamilshaikh.in/admin/deployments | jq

# Force a control-plane re-roll
kubectl -n paas-system rollout restart deploy/control-plane

# Trigger a fresh control-plane build (must bump image tag in manifests/03 first)
kubectl -n paas-system delete job build-control-plane --ignore-not-found
kubectl apply -f manifests/03-build-control-plane.yaml
```

---

## Working agreement with the human

The repo owner prefers:

- **Minimal upstream fixes over downstream workarounds.** Find the root
  cause, fix it once.
- **Not being given terminal commands to run.** Run them yourself
  through the tool, unless the action is interactive (browser, manual
  GitHub UI clicks).
- **No editing `build.xml` / `build.properties`** — those files belong
  to a separate system.
- **Concise, fact-based progress updates.** Don't pad responses with
  acknowledgments or restatements of the obvious.
