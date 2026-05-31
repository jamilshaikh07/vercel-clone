---
description: ESCAPE HATCH — force-rebuild the control plane (auto-rebuild handles git push)
---

**Read this first:** as of commit `c4ede4f`, the control-plane has a
built-in self-rebuilder that polls GitHub every 90 seconds for new
commits on `main` and triggers a Kaniko build + rollout automatically.
A normal `git push` should be sufficient — the new revision lands
within ~3 minutes (90s poll + ~90s build + rollout).

Use this workflow only when the auto-rebuild is broken or you need an
immediate rebuild without waiting for the next poll:

* The new code crashes on startup so the rebuilder isn't running
* You want to force a rebuild at the same SHA (e.g. ttl.sh expired)
* Debugging the rebuilder itself

# Steps

1. Confirm the latest commit is pushed:
   // turbo
   ```bash
   cd /home/jamil-shaikh/workspace/homelab/100k/mvp/vercel-clone && git status && git log --oneline -1
   ```
   If there are unpushed commits, push them first (Kaniko clones from
   `refs/heads/main` on github.com, so unpushed local changes won't be
   included in the build).

2. Run the rebuild script (build + roll in one step):
   // turbo
   ```bash
   export KUBECONFIG=/home/jamil-shaikh/.kube/config-homelab
   /home/jamil-shaikh/workspace/homelab/100k/mvp/vercel-clone/scripts/rebuild-control-plane.sh
   ```

3. Verify the new revision is serving:
   // turbo
   ```bash
   export KUBECONFIG=/home/jamil-shaikh/.kube/config-homelab
   kubectl -n paas-system get pods -l app=control-plane -o wide
   ```

# Verifying auto-rebuild instead

After a normal `git push`, watch the rebuilder do its thing:

```bash
export KUBECONFIG=/home/jamil-shaikh/.kube/config-homelab
POD=$(kubectl -n paas-system get pod -l app=control-plane -o jsonpath='{.items[0].metadata.name}')
kubectl -n paas-system logs -f $POD | grep self-rebuilder
```

And check the recorded state:

```bash
kubectl -n paas-system get configmap control-plane-rebuilder-state -o jsonpath='{.data}'
```

The `last_sha` should match `git rev-parse HEAD` after the rebuild completes.
