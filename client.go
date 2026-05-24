// Package pulse is the official Go client for StreamFlow Pulse — the AI Agent
// Platform (https://github.com/olsisoft/streamflow).
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
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	userAgent      = "pulse-client-go/2.6.0"
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

	// Resource accessors — each one shares the same transport. Public fields
	// (not method-based) so usage reads as `client.Pipelines.List(ctx)`,
	// matching the AWS SDK v2 / Google Cloud Go SDK convention.
	Auth      *AuthService
	Pipelines *PipelinesService
	Agents    *AgentsService
	Templates *TemplatesService
	Users     *UsersService
	Events    *EventsService
	IQ        *IQService
	Streams   *StreamsService
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

// request is the internal HTTP execution + error-translation pipeline.
func (c *Client) request(ctx context.Context, method, path string, body any, authenticated bool) (map[string]any, error) {
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
		return nil, fmt.Errorf("pulse: HTTP transport failure on %s %s: %w", method, path, err)
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
