---
description: Rebuild and redeploy the control plane after pushing changes
---

The control plane is NOT auto-rebuilt on git push. After committing &
pushing changes under `control-plane/`, run this workflow to:

1. Re-run the Kaniko build Job against current `main`
2. Wait for it to finish
3. Roll the control-plane Deployment so the new image is picked up

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

# When to use this

After ANY change under `control-plane/` (Go source, embedded static
assets, manifests that the binary cares about). The Kaniko build Job
uses subpath `control-plane/` and the Dockerfile inside it, so
changes elsewhere in the repo don't need a rebuild — but anything
under `control-plane/` requires this workflow.
