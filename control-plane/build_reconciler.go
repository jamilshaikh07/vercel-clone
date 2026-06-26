package main

// Build reconciler — lightweight "operator" loop that watches in-flight
// deployments against cluster state and unsticks builds the worker can't
// see (deleted Jobs, Kaniko hung on CPU-throttled nodes, etc.).
//
// Runs independently of the single-threaded build worker so a wedged
// waitForJob poll can't block recovery.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

const (
	buildReconcileInterval = 30 * time.Second
	// Kaniko can sit on "Unpacking rootfs" for minutes under CPU pressure;
	// beyond this we treat the Job as stuck and retry.
	stuckKanikoThreshold = 8 * time.Minute
	// Grace before we assume a missing Job is orphaned (not a create race).
	missingJobGrace = 90 * time.Second
)

type buildReconciler struct {
	store *store
	k8s   *kubeClient
	log   *slog.Logger
}

func buildJobName(deploymentID string) string {
	return fmt.Sprintf("build-%s", strings.ReplaceAll(deploymentID[:8], "-", ""))
}

func (r *buildReconciler) Run(ctx context.Context) {
	r.log.Info("build reconciler started")
	t := time.NewTicker(buildReconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.log.Info("build reconciler stopping")
			return
		case <-t.C:
			if err := r.tick(ctx); err != nil {
				r.log.Error("build reconcile tick failed", "err", err)
			}
		}
	}
}

func (r *buildReconciler) tick(ctx context.Context) error {
	deps, err := r.store.ListInFlightDeployments(ctx)
	if err != nil {
		return err
	}
	for _, d := range deps {
		if d.Status != "building" {
			continue
		}
		age := time.Since(d.StartedAt)
		jobName := buildJobName(d.ID)
		phase, err := r.k8s.getJobPhase(ctx, buildNamespace, jobName)
		if err != nil {
			r.log.Warn("reconcile: get job phase failed", "deployment_id", d.ID, "err", err)
			continue
		}
		if phase == nil {
			if age > missingJobGrace {
				r.requeue(ctx, d.ID, "build job missing from cluster; retrying")
			}
			continue
		}
		if phase.Succeeded >= 1 || phase.Failed >= 1 || phase.FailureMsg != "" {
			continue
		}
		if age < stuckKanikoThreshold || phase.Active < 1 {
			continue
		}
		pod, err := r.k8s.findBuildPod(ctx, buildNamespace, d.ID)
		if err != nil || pod == "" {
			continue
		}
		st, err := r.k8s.containerStatus(ctx, buildNamespace, pod, "kaniko")
		if err != nil || st == nil || !st.Running {
			continue
		}
		r.log.Warn("reconcile: kaniko build appears stuck; deleting job and requeueing",
			"deployment_id", d.ID, "job", jobName, "age", age.Round(time.Second))
		if err := r.k8s.deleteJob(ctx, buildNamespace, jobName); err != nil {
			r.log.Error("reconcile: delete stuck job failed", "job", jobName, "err", err)
			continue
		}
		r.requeue(ctx, d.ID, fmt.Sprintf("kaniko stalled after %s; retrying", age.Round(time.Second)))
	}
	return nil
}

func (r *buildReconciler) requeue(ctx context.Context, deploymentID, reason string) {
	ok, err := r.store.RequeueDeployment(ctx, deploymentID, reason)
	if err != nil {
		r.log.Error("reconcile: requeue failed", "deployment_id", deploymentID, "err", err)
		return
	}
	if ok {
		r.log.Info("reconcile: deployment requeued", "deployment_id", deploymentID, "reason", reason)
	}
}
