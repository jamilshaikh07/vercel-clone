package main

import (
	"strings"
	"testing"
)

// Quantity parsing is the most error-prone part of telemetry — kubelet
// reports CPU as nanocores ("12345n"), metrics-server occasionally hands
// back millicores ("10m") for very-low-CPU pods, and bare cores show up
// in some kubelet versions. Memory is consistently Ki, but defensive
// coverage of Mi/Gi makes future-proofing free.
func TestParseCPUm(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"12345678n", 12},       // ≈12 millicores
		{"5000000n", 5},         //  5 millicores
		{"500m", 500},
		{"1", 1000},             // 1 core = 1000 millicores
		{"0.5", 500},
		{"2500u", 2},            // 2500 microcores ≈ 2 milli
		{"garbage", 0},
	}
	for _, c := range cases {
		got := parseCPUm(c.in)
		if got != c.want {
			t.Errorf("parseCPUm(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseMemMi(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"5120Ki", 5},         // 5120 KiB = 5 MiB
		{"7Mi", 7},
		{"2Gi", 2048},
		{"1Ti", 1024 * 1024},
		{"500M", 500},         // decimal megabytes
		{"104857600", 100},    // raw bytes
		{"garbage", 0},
	}
	for _, c := range cases {
		got := parseMemMi(c.in)
		if got != c.want {
			t.Errorf("parseMemMi(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// Verifies the Traefik metrics parser handles a representative snippet:
// multiple services, status-code labels, and the matching _sum/_count
// pair for latency. Errors must be counted only for 4xx/5xx.
func TestParseTraefikMetrics(t *testing.T) {
	input := `# HELP traefik_service_requests_total
# TYPE traefik_service_requests_total counter
traefik_service_requests_total{code="200",method="GET",service="paas-static-test-svc@kubernetescrd"} 100
traefik_service_requests_total{code="404",method="GET",service="paas-static-test-svc@kubernetescrd"} 7
traefik_service_requests_total{code="500",method="POST",service="paas-static-test-svc@kubernetescrd"} 3
traefik_service_requests_total{code="200",method="GET",service="other-svc@kubernetescrd"} 50
# HELP traefik_service_request_duration_seconds_sum
# TYPE traefik_service_request_duration_seconds_sum counter
traefik_service_request_duration_seconds_sum{code="200",method="GET",service="paas-static-test-svc@kubernetescrd"} 2.5
traefik_service_request_duration_seconds_count{code="200",method="GET",service="paas-static-test-svc@kubernetescrd"} 100
go_gc_duration_seconds_count 161
`
	snap := &TelemetrySnapshot{Services: map[string]*ServiceTraffic{}}
	if err := parseTraefikMetrics(strings.NewReader(input), snap); err != nil {
		t.Fatalf("parse: %v", err)
	}

	test, ok := snap.Services["paas-static-test-svc@kubernetescrd"]
	if !ok {
		t.Fatal("expected test service in snapshot")
	}
	if test.Requests != 110 {
		t.Errorf("Requests = %v, want 110 (100+7+3)", test.Requests)
	}
	if test.Errors != 10 {
		t.Errorf("Errors = %v, want 10 (7+3, only 4xx/5xx)", test.Errors)
	}
	if test.Duration != 2.5 {
		t.Errorf("Duration = %v, want 2.5", test.Duration)
	}
	if test.Count != 100 {
		t.Errorf("Count = %v, want 100", test.Count)
	}
	if _, ok := snap.Services["other-svc@kubernetescrd"]; !ok {
		t.Error("expected other-svc to also be present")
	}
}
