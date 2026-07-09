// Package pulse is the official Go client for StreamFlow Pulse — the AI Agent
// Platform (https://github.com/olsisoft/pulse-go).
//
// Quick start:
//
//	ctx := context.Background()
//	client, err := pulse.NewClient(pulse.WithBaseURL("http://localhost:9090"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if _, err := client.Auth.Login(ctx, "alice", "secret"); err != nil {
//	    log.Fatal(err)
//	}
//
//	pipelines, err := client.Pipelines.List(ctx)
//	for _, p := range pipelines {
//	    fmt.Println(p["name"])
//	}
//
// The client is safe for concurrent use by multiple goroutines — the embedded
// http.Client pools connections. Create one per application, share it.
//
// Wire format: every method corresponds 1:1 to an endpoint in the Pulse
// OpenAPI 3.1 spec (streamflow-pulse/src/main/resources/openapi/openapi.yaml).
// Drift caught at PR time by the in-tree spec invariant tests (B-103).
package pulse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	userAgent      = "pulse-client-go/2.7.8"
	defaultTimeout = 30 * time.Second
)

// Client is the entry point for every Pulse REST call. Construct via
// NewClient, share across an entire application — safe for concurrent use.
type Client struct {
	baseURL string
	http    *http.Client
	timeout time.Duration

	tokenMu sync.RWMutex
	token   string

	// Opt-in retry policy (off by default — see WithRetry).
	retryMax        int
	retryBackoff    time.Duration
	retryMaxBackoff time.Duration
	retryStatuses   map[int]bool
	retryNonIdem    bool

	// Resource accessors — each one shares the same transport. Public fields
	// (not method-based) so usage reads as `client.Pipelines.List(ctx)`,
	// matching the AWS SDK v2 / Google Cloud Go SDK convention.
	Auth       *AuthService
	Pipelines  *PipelinesService
	Agents     *AgentsService
	Templates  *TemplatesService
	Users      *UsersService
	Events     *EventsService
	IQ         *IQService
	Streams    *StreamsService
	Models     *ModelsService
	Wasm       *WasmService
	Connectors *ConnectorsService
}

// Option configures a Client at construction time.
type Option func(*Client) error

// WithBaseURL sets the Pulse server URL. Required.
func WithBaseURL(url string) Option {
	return func(c *Client) error {
		if url == "" {
			return errors.New("pulse: baseURL cannot be empty")
		}
		c.baseURL = stripTrailingSlash(url)
		return nil
	}
}

// WithToken seeds the client with a pre-minted JWT. Optional — alternative is
// to call client.Auth.Login(...) which caches the token automatically.
func WithToken(token string) Option {
	return func(c *Client) error {
		c.token = token
		return nil
	}
}

// WithTimeout sets the per-request timeout. Default 30s.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) error {
		c.timeout = d
		return nil
	}
}

// WithHTTPClient lets the caller supply a custom *http.Client — useful for
// shared connection pools, custom TLS config (mTLS), proxies, or request
// tracing middleware.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) error {
		if h == nil {
			return errors.New("pulse: http.Client cannot be nil")
		}
		c.http = h
		return nil
	}
}

// RetryPolicy configures opt-in automatic retries. The zero value (MaxRetries 0)
// means retries are OFF — the client makes exactly one attempt per request.
type RetryPolicy struct {
	MaxRetries int           // 0 = off (default)
	Backoff    time.Duration // base backoff; default 200ms
	MaxBackoff time.Duration // per-attempt cap; default 10s
	OnStatus   []int         // retryable 5xx statuses; default 502, 503, 504
	// RetryNonIdempotent, when true, also retries non-idempotent methods
	// (POST/PATCH) on 5xx/transport. Default false → only GET/HEAD/PUT/DELETE
	// are retried on those, so a POST create is never silently duplicated.
	RetryNonIdempotent bool
}

// WithRetry enables opt-in, bounded, full-jitter exponential-backoff retries
// (off by default). 429 (rate limited) is always retried for any method,
// honouring Retry-After; OnStatus 5xx and transport errors are retried only for
// idempotent methods unless RetryNonIdempotent is set. Terminal 4xx are never
// retried.
func WithRetry(p RetryPolicy) Option {
	return func(c *Client) error {
		if p.MaxRetries < 0 {
			return errors.New("pulse: RetryPolicy.MaxRetries cannot be negative")
		}
		c.retryMax = p.MaxRetries
		c.retryBackoff = p.Backoff
		if c.retryBackoff <= 0 {
			c.retryBackoff = 200 * time.Millisecond
		}
		c.retryMaxBackoff = p.MaxBackoff
		if c.retryMaxBackoff <= 0 {
			c.retryMaxBackoff = 10 * time.Second
		}
		statuses := p.OnStatus
		if len(statuses) == 0 {
			statuses = []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout}
		}
		c.retryStatuses = make(map[int]bool, len(statuses))
		for _, s := range statuses {
			c.retryStatuses[s] = true
		}
		c.retryNonIdem = p.RetryNonIdempotent
		return nil
	}
}

// NewClient constructs a Client. Options are applied in order; the first
// non-nil error short-circuits and is returned.
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		timeout: defaultTimeout,
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	if c.baseURL == "" {
		return nil, errors.New("pulse: WithBaseURL is required")
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: c.timeout}
	}
	c.Auth = &AuthService{client: c}
	c.Pipelines = &PipelinesService{client: c}
	c.Agents = &AgentsService{client: c}
	c.Templates = &TemplatesService{client: c}
	c.Users = &UsersService{client: c}
	c.Events = &EventsService{client: c}
	c.IQ = &IQService{client: c}
	c.Streams = &StreamsService{client: c}
	c.Models = &ModelsService{client: c}
	c.Wasm = &WasmService{client: c}
	c.Connectors = &ConnectorsService{client: c}
	return c, nil
}

// Token returns the current bearer token, or "" if none is set.
func (c *Client) Token() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.token
}

// SetToken updates the bearer token used by subsequent authenticated requests.
// Safe for concurrent use.
func (c *Client) SetToken(token string) {
	c.tokenMu.Lock()
	c.token = token
	c.tokenMu.Unlock()
}

// Version returns the Pulse server's build + version metadata. Public —
// no JWT required.
func (c *Client) Version(ctx context.Context) (map[string]any, error) {
	return c.request(ctx, http.MethodGet, "/api/pulse/version", nil, false)
}

var idempotentMethods = map[string]bool{
	http.MethodGet: true, http.MethodHead: true, http.MethodPut: true,
	http.MethodDelete: true, http.MethodOptions: true,
}

// request runs doOnce under the opt-in retry policy. With retries off
// (retryMax == 0, the default) it makes exactly one attempt — identical to the
// pre-retry behaviour.
func (c *Client) request(ctx context.Context, method, path string, body any, authenticated bool) (map[string]any, error) {
	attempt := 0
	for {
		res, err := c.doOnce(ctx, method, path, body, authenticated)
		if err == nil || attempt >= c.retryMax {
			return res, err
		}
		retryable, wait := c.retryDecision(method, err, attempt)
		if !retryable {
			return res, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
		attempt++
	}
}

// retryDecision reports whether err is retryable for method at this attempt and
// how long to wait. 429 → any method (honour Retry-After); OnStatus 5xx +
// transport → idempotent methods only (unless RetryNonIdempotent); else not.
func (c *Client) retryDecision(method string, err error, attempt int) (bool, time.Duration) {
	var rle *RateLimitError
	if errors.As(err, &rle) {
		if rle.RetryAfterSeconds > 0 {
			return true, time.Duration(rle.RetryAfterSeconds) * time.Second
		}
		return true, c.backoffDelay(attempt)
	}
	if !idempotentMethods[strings.ToUpper(method)] && !c.retryNonIdem {
		return false, 0
	}
	var ae *APIError
	if errors.As(err, &ae) && c.retryStatuses[ae.StatusCode] {
		return true, c.backoffDelay(attempt)
	}
	var te *transportError
	if errors.As(err, &te) {
		return true, c.backoffDelay(attempt)
	}
	return false, 0
}

// backoffDelay returns full-jitter exponential backoff:
// uniform[0, min(MaxBackoff, Backoff*2^attempt)].
func (c *Client) backoffDelay(attempt int) time.Duration {
	ceiling := c.retryBackoff << uint(attempt) // base * 2^attempt
	if ceiling <= 0 || ceiling > c.retryMaxBackoff {
		ceiling = c.retryMaxBackoff
	}
	return time.Duration(rand.Int63n(int64(ceiling) + 1))
}

// doOnce is the internal HTTP execution + error-translation pipeline (one attempt).
func (c *Client) doOnce(ctx context.Context, method, path string, body any, authenticated bool) (map[string]any, error) {
	var reqBody io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("pulse: failed to marshal request body for %s: %w", path, err)
		}
		reqBody = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("pulse: failed to build request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if authenticated {
		token := c.Token()
		if token == "" {
			err := *errNoToken
			err.Path = path
			return nil, &err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &transportError{method: method, path: path, err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return map[string]any{}, nil
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("pulse: failed to read response body from %s: %w", path, err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if len(bodyBytes) == 0 {
			return map[string]any{}, nil
		}
		var parsed map[string]any
		if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
			return nil, fmt.Errorf("pulse: failed to parse JSON response from %s: %w", path, err)
		}
		if parsed == nil {
			return map[string]any{}, nil
		}
		return parsed, nil
	}

	return nil, translateError(resp, path, bodyBytes)
}

// requestMultipart POSTs a multipart/form-data body — used by the model-upload
// endpoint (B-112). fileField is the form field name for the file part,
// filename the part's filename, fileBytes the raw blob, and formFields the
// extra text fields. Always authenticated (the upload endpoint requires ADMIN).
func (c *Client) requestMultipart(ctx context.Context, path, fileField, filename string, fileBytes []byte, formFields map[string]string) (map[string]any, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	part, err := mw.CreateFormFile(fileField, filename)
	if err != nil {
		return nil, fmt.Errorf("pulse: failed to create multipart file part for %s: %w", path, err)
	}
	if _, err := part.Write(fileBytes); err != nil {
		return nil, fmt.Errorf("pulse: failed to write multipart file part for %s: %w", path, err)
	}
	for k, v := range formFields {
		if err := mw.WriteField(k, v); err != nil {
			return nil, fmt.Errorf("pulse: failed to write multipart field %q for %s: %w", k, path, err)
		}
	}
	if err := mw.Close(); err != nil {
		return nil, fmt.Errorf("pulse: failed to finalize multipart body for %s: %w", path, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, &buf)
	if err != nil {
		return nil, fmt.Errorf("pulse: failed to build request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", mw.FormDataContentType())

	token := c.Token()
	if token == "" {
		errCopy := *errNoToken
		errCopy.Path = path
		return nil, &errCopy
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pulse: HTTP transport failure on %s %s: %w", http.MethodPost, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return map[string]any{}, nil
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("pulse: failed to read response body from %s: %w", path, err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if len(bodyBytes) == 0 {
			return map[string]any{}, nil
		}
		var parsed map[string]any
		if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
			return nil, fmt.Errorf("pulse: failed to parse JSON response from %s: %w", path, err)
		}
		if parsed == nil {
			return map[string]any{}, nil
		}
		return parsed, nil
	}

	return nil, translateError(resp, path, bodyBytes)
}

func translateError(resp *http.Response, path string, bodyBytes []byte) error {
	var parsedBody map[string]any
	if len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, &parsedBody); err != nil {
			// Not JSON — wrap in a synthetic "error" field so callers still
			// see something via APIError.Body.
			text := string(bodyBytes)
			if len(text) > 200 {
				text = text[:200]
			}
			parsedBody = map[string]any{"error": text}
		}
	}

	base := APIError{
		StatusCode: resp.StatusCode,
		Path:       path,
		Body:       parsedBody,
	}

	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return &AuthError{APIError: base}
	case http.StatusNotFound:
		return &NotFoundError{APIError: base}
	case http.StatusBadRequest:
		return &ValidationError{APIError: base}
	case http.StatusTooManyRequests:
		retryAfter := 0
		if parsedBody != nil {
			switch v := parsedBody["retryAfterSeconds"].(type) {
			case float64:
				retryAfter = int(v)
			case int:
				retryAfter = v
			}
		}
		if retryAfter == 0 {
			if header := resp.Header.Get("Retry-After"); header != "" {
				if n, err := strconv.Atoi(header); err == nil {
					retryAfter = n
				}
			}
		}
		return &RateLimitError{APIError: base, RetryAfterSeconds: retryAfter}
	default:
		return &base
	}
}

func stripTrailingSlash(u string) string {
	for len(u) > 1 && u[len(u)-1] == '/' {
		u = u[:len(u)-1]
	}
	return u
}

// encodePathSegment URL-encodes a path-param value so ids containing slashes /
// spaces / etc. round-trip safely.
func encodePathSegment(s string) string {
	return url.PathEscape(s)
}
