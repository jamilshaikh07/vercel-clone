package main

// pull_request webhook → preview deployment enqueue.

import (
	"context"
	"encoding/json"
	"fmt"
)

type pullRequestPayload struct {
	Action string `json:"action"`
	Number int    `json:"number"`
	PullRequest struct {
		Head struct {
			SHA string `json:"sha"`
			Ref string `json:"ref"`
		} `json:"head"`
		Title string `json:"title"`
	} `json:"pull_request"`
}

func (s *server) handlePullRequest(ctx context.Context, body []byte, env envelope, deliveryID string) error {
	switch env.Action {
	case "opened", "synchronize", "reopened":
	default:
		return nil
	}
	var pr pullRequestPayload
	if err := json.Unmarshal(body, &pr); err != nil {
		return fmt.Errorf("parse pull_request: %w", err)
	}
	sha := pr.PullRequest.Head.SHA
	if sha == "" {
		return nil
	}
	ref := fmt.Sprintf("refs/pull/%d/head", pr.Number)
	msg := pr.PullRequest.Title
	if msg == "" {
		msg = fmt.Sprintf("PR #%d preview", pr.Number)
	}
	res, err := s.store.EnqueueDeployment(ctx,
		env.Installation.ID, env.Repository.ID,
		sha, ref, deliveryID, "preview",
		enqueueOptions{CommitMessage: msg, IsPreview: true, PRNumber: pr.Number},
	)
	if err != nil {
		return err
	}
	if res == nil {
		return nil
	}
	if res.Deduped {
		s.log.Info("preview deduped", "project_id", res.ProjectID, "pr", pr.Number, "sha", sha[:8])
		return nil
	}
	s.log.Info("preview deployment queued",
		"deployment_id", res.DeploymentID,
		"project_id", res.ProjectID,
		"pr", pr.Number,
		"sha", sha[:8],
	)
	return nil
}

func pushCommitMessage(body []byte) string {
	var p struct {
		HeadCommit struct {
			Message string `json:"message"`
		} `json:"head_commit"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return ""
	}
	return p.HeadCommit.Message
}
