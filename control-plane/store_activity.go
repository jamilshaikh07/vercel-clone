package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type deploymentStats struct {
	BuildsToday    int
	Failed24h      int
	SuccessRatePct float64
	AvgBuildSec    *int
}

// ListActivityForUser merges recent deployments and webhook events for the user's repos.
func (s *store) ListActivityForUser(ctx context.Context, userID string, limit int) ([]activityItem, error) {
	if limit <= 0 || limit > 100 {
		limit = 40
	}

	depRows, err := s.pool.Query(ctx, `
		SELECT 'deployment', to_char(d.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		       d.project_id::text, p.slug,
		       COALESCE(NULLIF(TRIM(p.display_name), ''), p.slug),
		       CASE
		         WHEN d.status = 'ready' THEN 'Deployment ready'
		         WHEN d.status = 'failed' THEN 'Deployment failed'
		         WHEN d.status = 'building' THEN 'Build started'
		         WHEN d.status = 'deploying' THEN 'Deploying'
		         ELSE 'Deployment queued'
		       END,
		       COALESCE(d.commit_message, ''),
		       d.status, d.ref, d.commit_sha, d.id::text,
		       '', '', d.is_preview
		  FROM deployments d
		  JOIN projects p ON p.id = d.project_id
		 WHERE p.owner_user_id = $1
		 ORDER BY d.created_at DESC
		 LIMIT $2
	`, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("deployment activity: %w", err)
	}
	defer depRows.Close()

	var items []activityItem
	for depRows.Next() {
		var it activityItem
		it.Kind = "deployment"
		if err := depRows.Scan(
			&it.Kind, &it.At, &it.ProjectID, &it.ProjectSlug, &it.ProjectName,
			&it.Title, &it.Detail, &it.Status, &it.Ref, &it.CommitSHA,
			&it.DeploymentID, &it.Event, &it.Action, &it.IsPreview,
		); err != nil {
			return nil, err
		}
		if it.Detail == "" && len(it.CommitSHA) >= 7 {
			it.Detail = it.CommitSHA[:7]
		}
		items = append(items, it)
	}
	if err := depRows.Err(); err != nil {
		return nil, err
	}

	whRows, err := s.pool.Query(ctx, `
		SELECT 'webhook', to_char(w.received_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		       COALESCE(p.id::text, ''), COALESCE(p.slug, ''), COALESCE(NULLIF(TRIM(p.display_name), ''), p.slug, split_part(w.repo_full_name, '/', 2)),
		       w.event || COALESCE(' · ' || NULLIF(w.action, ''), ''),
		       COALESCE(w.repo_full_name, ''),
		       '', '', '', '', w.event, COALESCE(w.action, ''), false
		  FROM webhook_deliveries w
		  LEFT JOIN projects p ON p.full_name = w.repo_full_name AND p.owner_user_id = $1
		 WHERE w.repo_full_name IN (SELECT full_name FROM projects WHERE owner_user_id = $1)
		 ORDER BY w.received_at DESC
		 LIMIT $2
	`, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("webhook activity: %w", err)
	}
	defer whRows.Close()

	for whRows.Next() {
		var it activityItem
		if err := whRows.Scan(
			&it.Kind, &it.At, &it.ProjectID, &it.ProjectSlug, &it.ProjectName,
			&it.Title, &it.Detail, &it.Status, &it.Ref, &it.CommitSHA,
			&it.DeploymentID, &it.Event, &it.Action, &it.IsPreview,
		); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	if err := whRows.Err(); err != nil {
		return nil, err
	}

	sort.Slice(items, func(i, j int) bool { return items[i].At > items[j].At })
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (s *store) DeploymentStatsForUser(ctx context.Context, userID string) (deploymentStats, error) {
	var st deploymentStats
	err := s.pool.QueryRow(ctx, `
		SELECT
		  COUNT(*) FILTER (WHERE d.created_at >= date_trunc('day', now()))::int,
		  COUNT(*) FILTER (WHERE d.status = 'failed' AND d.created_at >= now() - interval '24 hours')::int,
		  COALESCE(ROUND(
		    100.0 * COUNT(*) FILTER (WHERE d.status = 'ready' AND d.created_at >= now() - interval '30 days')
		    / NULLIF(COUNT(*) FILTER (WHERE d.status IN ('ready','failed') AND d.created_at >= now() - interval '30 days'), 0),
		    1
		  ), 100),
		  (AVG(EXTRACT(EPOCH FROM (COALESCE(d.build_ended_at, d.ready_at) - d.build_started_at)))::int)
		    FILTER (WHERE d.build_started_at IS NOT NULL
		              AND (d.build_ended_at IS NOT NULL OR d.ready_at IS NOT NULL))
		  FROM deployments d
		  JOIN projects p ON p.id = d.project_id
		 WHERE p.owner_user_id = $1
	`, userID).Scan(&st.BuildsToday, &st.Failed24h, &st.SuccessRatePct, &st.AvgBuildSec)
	if err != nil {
		return st, err
	}
	return st, nil
}

// enqueueOptions carries optional metadata for a new deployment row.
type enqueueOptions struct {
	CommitMessage string
	IsPreview     bool
	PRNumber      int
}

func branchFromRef(ref string) string {
	return strings.TrimPrefix(ref, "refs/heads/")
}
