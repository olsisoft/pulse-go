package pulse

import (
	"context"
	"net/http"
)

// ---------------------------------------------------------------------------
// AuthService — client.Auth
// ---------------------------------------------------------------------------

// AuthService groups authentication + session-management endpoints.
type AuthService struct {
	client *Client
}

// Login exchanges username + password for a JWT (POST /api/auth/login).
//
// On success, the returned token is cached on the parent client so subsequent
// calls authenticate automatically. The full response (including refreshToken
// and activeOrg) is returned for downstream use.
func (s *AuthService) Login(ctx context.Context, username, password string) (map[string]any, error) {
	body := map[string]any{"username": username, "password": password}
	response, err := s.client.request(ctx, http.MethodPost, "/api/auth/login", body, false)
	if err != nil {
		return nil, err
	}
	cacheToken(s.client, response)
	return response, nil
}

// Refresh exchanges a refresh token for a fresh JWT (POST /api/auth/refresh).
// The new token is cached on the parent client.
func (s *AuthService) Refresh(ctx context.Context, refreshToken string) (map[string]any, error) {
	body := map[string]any{"refreshToken": refreshToken}
	response, err := s.client.request(ctx, http.MethodPost, "/api/auth/refresh", body, false)
	if err != nil {
		return nil, err
	}
	cacheToken(s.client, response)
	return response, nil
}

// Organizations returns the orgs the current user is a member of
// (GET /api/auth/organizations).
func (s *AuthService) Organizations(ctx context.Context) ([]map[string]any, error) {
	result, err := s.client.request(ctx, http.MethodGet, "/api/auth/organizations", nil, true)
	if err != nil {
		return nil, err
	}
	return unwrapList(result["organizations"]), nil
}

// SwitchOrg switches the active organisation (POST /api/auth/switch-org).
// The new JWT (with updated orgId claim) is cached on the parent client.
func (s *AuthService) SwitchOrg(ctx context.Context, orgID string) (map[string]any, error) {
	body := map[string]any{"orgId": orgID}
	response, err := s.client.request(ctx, http.MethodPost, "/api/auth/switch-org", body, true)
	if err != nil {
		return nil, err
	}
	cacheToken(s.client, response)
	return response, nil
}

func cacheToken(c *Client, response map[string]any) {
	if v, ok := response["token"].(string); ok && v != "" {
		c.SetToken(v)
	}
}

// ---------------------------------------------------------------------------
// PipelinesService — client.Pipelines
// ---------------------------------------------------------------------------

// PipelinesService groups pipeline create / list / inspect / delete endpoints.
type PipelinesService struct {
	client *Client
}

// List returns every pipeline in the current org (GET /api/pulse/pipelines).
func (s *PipelinesService) List(ctx context.Context) ([]map[string]any, error) {
	result, err := s.client.request(ctx, http.MethodGet, "/api/pulse/pipelines", nil, true)
	if err != nil {
		return nil, err
	}
	return unwrapList(result["pipelines"]), nil
}

// Get returns one pipeline by id (GET /api/pulse/pipelines/{id}).
func (s *PipelinesService) Get(ctx context.Context, pipelineID string) (map[string]any, error) {
	return s.client.request(ctx, http.MethodGet, "/api/pulse/pipelines/"+encodePathSegment(pipelineID), nil, true)
}

// Create creates + deploys a new pipeline (POST /api/pulse/pipelines).
//
// The definition must follow the CreatePipelineRequest schema (see
// openapi.yaml). At minimum: name + nodes.
func (s *PipelinesService) Create(ctx context.Context, definition map[string]any) (map[string]any, error) {
	return s.client.request(ctx, http.MethodPost, "/api/pulse/pipelines", definition, true)
}

// Delete tears down the pipeline (DELETE /api/pulse/pipelines/{id}).
func (s *PipelinesService) Delete(ctx context.Context, pipelineID string) error {
	_, err := s.client.request(ctx, http.MethodDelete, "/api/pulse/pipelines/"+encodePathSegment(pipelineID), nil, true)
	return err
}

// ---------------------------------------------------------------------------
// AgentsService — client.Agents
// ---------------------------------------------------------------------------

// AgentsService groups list / get / update / delete endpoints for deployed agents.
type AgentsService struct {
	client *Client
}

// List returns every deployed agent in the current org (GET /api/pulse/agents).
func (s *AgentsService) List(ctx context.Context) ([]map[string]any, error) {
	result, err := s.client.request(ctx, http.MethodGet, "/api/pulse/agents", nil, true)
	if err != nil {
		return nil, err
	}
	return unwrapList(result["agents"]), nil
}

// Get returns one agent by id (GET /api/pulse/agents/{id}).
func (s *AgentsService) Get(ctx context.Context, agentID string) (map[string]any, error) {
	return s.client.request(ctx, http.MethodGet, "/api/pulse/agents/"+encodePathSegment(agentID), nil, true)
}

// Update — B-115 Phase 1: PUT /api/pulse/agents/{id}: replace the agent's config.
//
// config is the FULL agent config (not a partial merge) — at minimum "name";
// optional fields ("engineType", "inputTopic", "outputTopic", "description",
// "instances", "monthlyBudget", "config") fall back to safe defaults when
// omitted. See the UpdateAgentRequest schema in openapi.yaml.
//
// Today this triggers a full stop + persist + start cycle on the engine side —
// the agent is briefly unavailable while the swap happens. Existing state in
// the agent's keyed store is preserved. Phase 2 (B-115-engine) will add atomic
// event-boundary swap so hot-reloadable changes apply with no downtime.
//
// Returns the post-update agent snapshot (same shape as Get). Returns
// *ValidationError on a bad config (self-loop, invalid streaming operators),
// *NotFoundError if the agent doesn't exist.
func (s *AgentsService) Update(ctx context.Context, agentID string, config map[string]any) (map[string]any, error) {
	return s.client.request(ctx, http.MethodPut, "/api/pulse/agents/"+encodePathSegment(agentID), config, true)
}

// Delete — DELETE /api/pulse/agents/{id}: stop the agent + remove its config row.
//
// The agent's keyed state store is also dropped. Requires the AGENT_DELETE
// permission. Returns *NotFoundError if the agent doesn't exist.
func (s *AgentsService) Delete(ctx context.Context, agentID string) error {
	_, err := s.client.request(ctx, http.MethodDelete, "/api/pulse/agents/"+encodePathSegment(agentID), nil, true)
	return err
}

// ---------------------------------------------------------------------------
// TemplatesService — client.Templates
// ---------------------------------------------------------------------------

// TemplatesService groups the first-party pipeline template catalog.
type TemplatesService struct {
	client *Client
}

// List returns the 223+ first-party templates (GET /api/pulse/templates).
func (s *TemplatesService) List(ctx context.Context) ([]map[string]any, error) {
	result, err := s.client.request(ctx, http.MethodGet, "/api/pulse/templates", nil, true)
	if err != nil {
		return nil, err
	}
	return unwrapList(result["templates"]), nil
}

// ---------------------------------------------------------------------------
// UsersService — client.Users
// ---------------------------------------------------------------------------

// UsersService groups user management endpoints (admin only).
type UsersService struct {
	client *Client
}

// List returns every user in the current org (GET /api/pulse/users).
//
// Requires the caller to have the USERS_LIST permission atom (Owner / Platform
// Admin personas by default — see B-105).
func (s *UsersService) List(ctx context.Context) ([]map[string]any, error) {
	result, err := s.client.request(ctx, http.MethodGet, "/api/pulse/users", nil, true)
	if err != nil {
		return nil, err
	}
	return unwrapList(result["users"]), nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// unwrapList safely extracts a []map[string]any from the JSON-decoded value.
// Returns an empty slice on missing / malformed envelopes — never nil — so
// callers can range over the result without a nil-check.
func unwrapList(v any) []map[string]any {
	if v == nil {
		return []map[string]any{}
	}
	raw, ok := v.([]any)
	if !ok {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}
