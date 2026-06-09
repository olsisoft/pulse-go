package pulse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fastRetry keeps backoff negligible so the tests don't actually sleep.
func fastRetry(max int, nonIdem bool) RetryPolicy {
	return RetryPolicy{
		MaxRetries:         max,
		Backoff:            time.Millisecond,
		MaxBackoff:         2 * time.Millisecond,
		RetryNonIdempotent: nonIdem,
	}
}

// seqServer replies with codes[i]/bodies[i] for the i-th request (clamped to the
// last). The returned counter reports how many requests it actually received.
func seqServer(codes []int, bodies []string) (*httptest.Server, *int64) {
	var n int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(atomic.AddInt64(&n, 1)) - 1
		if i >= len(codes) {
			i = len(codes) - 1
		}
		w.WriteHeader(codes[i])
		body := "{}"
		if i < len(bodies) {
			body = bodies[i]
		}
		_, _ = w.Write([]byte(body))
	}))
	return srv, &n
}

func TestRetry_OffByDefault(t *testing.T) {
	srv, n := seqServer([]int{503, 200}, []string{"{}", `{"ok":true}`})
	defer srv.Close()
	c, _ := NewClient(WithBaseURL(srv.URL)) // no WithRetry
	if _, err := c.Version(context.Background()); err == nil {
		t.Fatal("expected an error on 503 with retries off")
	}
	if got := atomic.LoadInt64(n); got != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", got)
	}
}

func TestRetry_IdempotentGetRetriedOn5xx(t *testing.T) {
	srv, n := seqServer([]int{503, 502, 200}, []string{"{}", "{}", `{"v":1}`})
	defer srv.Close()
	c, _ := NewClient(WithBaseURL(srv.URL), WithRetry(fastRetry(2, false)))
	if _, err := c.Version(context.Background()); err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if got := atomic.LoadInt64(n); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

func TestRetry_ExhaustsThenReturnsError(t *testing.T) {
	srv, n := seqServer([]int{503}, []string{"{}"})
	defer srv.Close()
	c, _ := NewClient(WithBaseURL(srv.URL), WithRetry(fastRetry(2, false)))
	var apiErr *APIError
	if _, err := c.Version(context.Background()); !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
		t.Fatalf("expected APIError 503 after exhausting retries, got %v", err)
	}
	if got := atomic.LoadInt64(n); got != 3 { // initial + 2 retries
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

func TestRetry_429RetriedForPostHonouringRetryAfter(t *testing.T) {
	srv, n := seqServer([]int{429, 201}, []string{`{"retryAfterSeconds":0}`, `{"id":"p1"}`})
	defer srv.Close()
	c, _ := NewClient(WithBaseURL(srv.URL), WithToken("t"), WithRetry(fastRetry(1, false)))
	if _, err := c.Pipelines.Create(context.Background(), map[string]any{"name": "x"}); err != nil {
		t.Fatalf("expected 429 retry to succeed, got %v", err)
	}
	if got := atomic.LoadInt64(n); got != 2 {
		t.Fatalf("expected 2 attempts (429 then 201), got %d", got)
	}
}

func TestRetry_Post5xxNotRetriedByDefault(t *testing.T) {
	srv, n := seqServer([]int{503, 201}, []string{"{}", `{"id":"p1"}`})
	defer srv.Close()
	c, _ := NewClient(WithBaseURL(srv.URL), WithToken("t"), WithRetry(fastRetry(3, false)))
	var apiErr *APIError
	if _, err := c.Pipelines.Create(context.Background(), map[string]any{"name": "x"}); !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
		t.Fatalf("expected POST 5xx to surface (not retried), got %v", err)
	}
	if got := atomic.LoadInt64(n); got != 1 {
		t.Fatalf("expected 1 attempt (POST 5xx not retried), got %d", got)
	}
}

func TestRetry_Post5xxRetriedWhenOptedIn(t *testing.T) {
	srv, n := seqServer([]int{503, 201}, []string{"{}", `{"id":"p1"}`})
	defer srv.Close()
	c, _ := NewClient(WithBaseURL(srv.URL), WithToken("t"), WithRetry(fastRetry(2, true)))
	if _, err := c.Pipelines.Create(context.Background(), map[string]any{"name": "x"}); err != nil {
		t.Fatalf("expected opted-in POST retry to succeed, got %v", err)
	}
	if got := atomic.LoadInt64(n); got != 2 {
		t.Fatalf("expected 2 attempts, got %d", got)
	}
}

func TestRetry_TerminalNotFoundNotRetried(t *testing.T) {
	srv, n := seqServer([]int{404}, []string{`{"error":"nope"}`})
	defer srv.Close()
	c, _ := NewClient(WithBaseURL(srv.URL), WithToken("t"), WithRetry(fastRetry(3, false)))
	var nf *NotFoundError
	if _, err := c.Pipelines.Get(context.Background(), "nope"); !errors.As(err, &nf) {
		t.Fatalf("expected NotFoundError, got %v", err)
	}
	if got := atomic.LoadInt64(n); got != 1 {
		t.Fatalf("expected 1 attempt (404 terminal), got %d", got)
	}
}

// failingRT returns a transport error for the first `fail` calls, then a 200.
type failingRT struct {
	calls int64
	fail  int64
}

func (rt *failingRT) RoundTrip(req *http.Request) (*http.Response, error) {
	if atomic.AddInt64(&rt.calls, 1) <= rt.fail {
		return nil, errors.New("simulated dial failure")
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(`{"pipelines":[]}`)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func TestRetry_TransportErrorRetriedForGet(t *testing.T) {
	rt := &failingRT{fail: 1}
	c, _ := NewClient(
		WithBaseURL("http://pulse.test:9090"),
		WithToken("t"),
		WithHTTPClient(&http.Client{Transport: rt}),
		WithRetry(fastRetry(2, false)),
	)
	if _, err := c.Pipelines.List(context.Background()); err != nil {
		t.Fatalf("expected transport error to be retried then succeed, got %v", err)
	}
	if got := atomic.LoadInt64(&rt.calls); got != 2 {
		t.Fatalf("expected 2 attempts (1 transport fail + 1 ok), got %d", got)
	}
}

func TestRetry_BackoffBoundedAndNonNegative(t *testing.T) {
	c, _ := NewClient(WithBaseURL("http://x"), WithRetry(RetryPolicy{
		MaxRetries: 1, Backoff: 500 * time.Millisecond, MaxBackoff: 4 * time.Second,
	}))
	for attempt := 0; attempt < 8; attempt++ {
		d := c.backoffDelay(attempt)
		if d < 0 || d > 4*time.Second {
			t.Fatalf("backoff out of [0,4s] at attempt %d: %v", attempt, d)
		}
	}
}
