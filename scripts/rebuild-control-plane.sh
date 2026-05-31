#!/usr/bin/env bash
# Rebuild + redeploy the control plane from current main.
#
# The control-plane image lives at a 24-hour ttl.sh tag — we don't
# version-bump per commit, we just rebuild against the same tag and
# rotate the pod with imagePullPolicy: Always picking up the new layer.
#
# Why this isn't a webhook (yet): the control-plane is what receives
# webhooks. Rebuilding itself from its own webhook handler creates a
# kill-the-bishop scenario where a broken push prevents the next push
# from being received. A separate "rebuild me" path is safer. When the
# user wants zero-touch rebuilds, the right pattern is a GitHub Action
# that triggers this Job via `kubectl` against the cluster.
#
# Usage:
#   ./scripts/rebuild-control-plane.sh           # rebuild + roll
#   ./scripts/rebuild-control-plane.sh --no-roll # build only, don't restart pod
set -euo pipefail

ROLL=1
for a in "$@"; do
  case "$a" in
    --no-roll) ROLL=0 ;;
    -h|--help)
      sed -n '2,/^set -/p' "$0" | sed 's/^# \?//'
      exit 0 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MANIFEST="$REPO_ROOT/manifests/03-build-control-plane.yaml"

echo "[1/4] Deleting old build job (Jobs are immutable, can't re-apply in place)…"
kubectl -n paas-system delete job build-control-plane --ignore-not-found --wait=true

echo "[2/4] Applying build job…"
kubectl apply -f "$MANIFEST"

echo "[3/4] Waiting for Kaniko build to finish (up to 5 minutes)…"
if ! kubectl -n paas-system wait --for=condition=complete --timeout=300s job/build-control-plane; then
  echo ""
  echo "BUILD FAILED — last 80 lines of pod logs:"
  POD=$(kubectl -n paas-system get pod -l job-name=build-control-plane -o jsonpath='{.items[-1:].metadata.name}')
  kubectl -n paas-system logs "$POD" --tail=80 || true
  exit 1
fi

if [ "$ROLL" = "1" ]; then
  echo "[4/4] Rolling control-plane Deployment…"
  kubectl -n paas-system rollout restart deploy/control-plane
  kubectl -n paas-system rollout status deploy/control-plane --timeout=120s
  echo ""
  echo "Done. New revision is live at https://paas.jamilshaikh.in"
else
  echo "[4/4] Skipped pod restart (--no-roll). New image is at ttl.sh/...:24h"
  echo "      Run:  kubectl -n paas-system rollout restart deploy/control-plane"
fi
