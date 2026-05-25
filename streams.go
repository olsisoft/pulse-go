package pulse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// StreamsService — client.Streams. B-107 Kafka-Streams-like declarative DSL.
//
// The DSL is server-side execution, client-side declaration: the operator
// chain is built in Go, compiled to the JSON pipeline shape that the Pulse
// server's StreamingOperatorValidator accepts, and POSTed to
// /api/pulse/pipelines. Stream processing then runs on the Pulse engine
// (3.6 M evt/s native throughput), not in the client process.
//
// This is the opposite of Kafka Streams (which runs in the caller's JVM).
// The trade-off: you can't do microsecond client-side compute, but you get
// infinite-scale stateful streaming, durable replicated state queryable via
// B-106 IQ, and the same DSL works from any of the 5 Pulse SDKs.
//
// Quick start:
//
//	builder := pulse.NewStreamBuilder("iot-temperature-aggregator").
//	    FromTopic("sensor-readings", pulse.FromTopicOptions{SourceEngine: "mqtt"}).
//	    KeyBy("deviceId").
//	    Window(pulse.WindowsTumbling("60s"), pulse.WindowOptions{
//	        Aggregations: map[string]string{"avgTemp": pulse.AggsAvg("temperature")},
//	    }).
//	    Filter("avgTemp > 75").
//	    ToTopic("sensor-minute-averages", pulse.ToTopicOptions{SinkChannel: "email"})
//
//	deployed, err := client.Streams.Deploy(ctx, builder)
//
// Conditions and field-expressions are passed as strings — lambdas / closures
// are NOT supported because they cannot be serialised to JSON.
type StreamsService struct {
	client *Client
}

// Compile turns the builder into a Pulse pipeline definition WITHOUT deploying.
func (s *StreamsService) Compile(builder *StreamBuilder) (map[string]any, error) {
	return builder.Build("")
}

// CompileWithName same as Compile but overrides the pipeline name.
func (s *StreamsService) CompileWithName(builder *StreamBuilder, name string) (map[string]any, error) {
	return builder.Build(name)
}

// Deploy compiles + POSTs to /api/pulse/pipelines. Returns the server response.
func (s *StreamsService) Deploy(ctx context.Context, builder *StreamBuilder) (map[string]any, error) {
	return s.DeployWithName(ctx, builder, "")
}

// DeployWithName compiles with a name override + POSTs to /api/pulse/pipelines.
func (s *StreamsService) DeployWithName(ctx context.Context, builder *StreamBuilder, name string) (map[string]any, error) {
	definition, err := builder.Build(name)
	if err != nil {
		return nil, err
	}
	return s.client.request(ctx, http.MethodPost, "/api/pulse/pipelines", definition, true)
}

// ---------------------------------------------------------------------------
// Window specs — typed wrappers that compile to the string form the server
// parser (WindowEngine.parseSpec) accepts.
// ---------------------------------------------------------------------------

// WindowSpec is a window specification. Construct via the WindowsXxx helpers.
type WindowSpec struct {
	Spec string
}

func (w WindowSpec) String() string { return w.Spec }

// WindowsTumbling — non-overlapping fixed windows: WindowsTumbling("60s").
func WindowsTumbling(size string) WindowSpec {
	if strings.TrimSpace(size) == "" {
		panic("WindowsTumbling: size must be non-empty")
	}
	return WindowSpec{Spec: "tumbling(" + size + ")"}
}

// WindowsSliding — overlapping windows: WindowsSliding("10m", "1m") = size, slide.
func WindowsSliding(size, slide string) WindowSpec {
	if strings.TrimSpace(size) == "" {
		panic("WindowsSliding: size must be non-empty")
	}
	if strings.TrimSpace(slide) == "" {
		panic("WindowsSliding: slide must be non-empty")
	}
	return WindowSpec{Spec: "sliding(" + size + "," + slide + ")"}
}

// WindowsSession — inactivity-bounded windows: WindowsSession("30s").
func WindowsSession(timeout string) WindowSpec {
	if strings.TrimSpace(timeout) == "" {
		panic("WindowsSession: timeout must be non-empty")
	}
	return WindowSpec{Spec: "session(" + timeout + ")"}
}

// WindowsGlobal — single unbounded window. Use for global aggregates.
func WindowsGlobal() WindowSpec {
	return WindowSpec{Spec: "global"}
}

// WindowsCount — event-count tumbling: closes after n events. WindowsCount(100).
func WindowsCount(n int) WindowSpec {
	if n <= 0 {
		panic(fmt.Sprintf("WindowsCount: size must be positive, got %d", n))
	}
	return WindowSpec{Spec: fmt.Sprintf("count(%d)", n)}
}

// WindowsCountSliding — event-count sliding: WindowsCountSliding(100, 10) = window, slide.
func WindowsCountSliding(size, slide int) WindowSpec {
	if size <= 0 || slide <= 0 {
		panic(fmt.Sprintf("WindowsCountSliding: positive size and slide required, got %d, %d", size, slide))
	}
	return WindowSpec{Spec: fmt.Sprintf("count_sliding(%d,%d)", size, slide)}
}

// ---------------------------------------------------------------------------
// Aggregators — string-template builders that compile to the form the server
// parser (Aggregators.parse) accepts inside window.aggregations.
// ---------------------------------------------------------------------------

// AggsCount — event count, no field required.
func AggsCount() string { return "count()" }

// AggsSum — sum of a numeric field: AggsSum("amount").
func AggsSum(field string) string {
	requireNonBlank("field", field)
	return "sum(" + field + ")"
}

// AggsAvg — average of a numeric field: AggsAvg("price").
func AggsAvg(field string) string {
	requireNonBlank("field", field)
	return "avg(" + field + ")"
}

// AggsMin — minimum value of a numeric field.
func AggsMin(field string) string {
	requireNonBlank("field", field)
	return "min(" + field + ")"
}

// AggsMax — maximum value of a numeric field.
func AggsMax(field string) string {
	requireNonBlank("field", field)
	return "max(" + field + ")"
}

// AggsCollectList — collect every value of field into a list.
func AggsCollectList(field string) string {
	requireNonBlank("field", field)
	return "collect_list(" + field + ")"
}

// AggsDistinctCount — cardinality of distinct values of field.
func AggsDistinctCount(field string) string {
	requireNonBlank("field", field)
	return "distinct_count(" + field + ")"
}

// ---------------------------------------------------------------------------
// Option carriers
// ---------------------------------------------------------------------------

// FromTopicOptions — options for StreamBuilder.FromTopic.
type FromTopicOptions struct {
	SourceEngine string         // default "kafka"
	SourceConfig map[string]any // extra config merged into the source node config
	Label        string         // display label for the source node
}

// ToTopicOptions — options for StreamBuilder.ToTopic. SinkChannel empty → no sink node.
type ToTopicOptions struct {
	SinkChannel string         // sink subType ("kafka", "email", "slack", …)
	SinkConfig  map[string]any // extra config merged into the sink node config
	Label       string         // display label for the sink node
}

// MapOptions — options for StreamBuilder.Map. At least one field must be set.
type MapOptions struct {
	Fields     map[string]string
	TargetType string
}

// WindowOptions — options for StreamBuilder.Window.
type WindowOptions struct {
	Aggregations map[string]string // use AggsXxx() for the right-hand side
	OutputTopic  string
	Trigger      any
}

// BranchSpec — one branch of StreamBuilder.Branch.
type BranchSpec struct {
	Condition string
	Topic     string
}

// EnrichAsyncOptions — options for StreamBuilder.EnrichAsync.
// Pointer fields are optional (nil = omit). URL is required.
type EnrichAsyncOptions struct {
	URL            string
	Parallelism    *int
	QueueSize      *int
	TimeoutMs      *int
	MaxRetries     *int
	RetryBackoffMs *int
	Ordering       string // "PRESERVE_INPUT" | "UNORDERED" | "" (omit)
	OnFailure      string // "EMIT_ERROR" | "DROP" | "PASS_THROUGH" | "" (omit)
}

// CepOptions — options for StreamBuilder.Cep.
type CepOptions struct {
	Within string
	Name   string
}

// BroadcastJoinOptions — options for StreamBuilder.BroadcastJoin.
type BroadcastJoinOptions struct {
	JoinKeyField   string
	StreamingTopic string
	Name           string
	MaxBytes       *int64
	RefreshMode    string // "cdc" | "periodic" | "explicit" | "" (omit)
	IntervalMillis *int
}

// CdcJoinOptions — options for StreamBuilder.CdcJoin.
type CdcJoinOptions struct {
	Source       string
	JoinKey      string
	Table        string
	StateBackend string
}

// MapLlmOptions — B-109 options for StreamBuilder.MapLlm. Pointer fields are
// optional (nil = omit). OutputField is required.
type MapLlmOptions struct {
	OutputField    string
	Model          string
	Temperature    *float64
	MaxTokens      *int
	Parallelism    *int
	Ordering       string // "PRESERVE_INPUT" | "UNORDERED" | "" (omit)
	OnFailure      string // "EMIT_ERROR" | "DROP" | "PASS_THROUGH" | "" (omit)
	MaxCallsPerSec *int
}

// ExtractOptions — B-109 options for StreamBuilder.Extract. Instruction +
// Schema are required.
type ExtractOptions struct {
	Instruction string
	Schema      map[string]string
	Model       string
	Temperature *float64
	MaxTokens   *int
	OnFailure   string
}

// McpCallOptions — B-109 Phase 2 options for StreamBuilder.McpCall.
type McpCallOptions struct {
	Args        map[string]any
	OutputField string
	Parallelism *int
	Ordering    string
	OnFailure   string
}

// ---------------------------------------------------------------------------
// StreamBuilder — fluent operator-chain → pipeline-JSON compiler.
// ---------------------------------------------------------------------------

// StreamBuilder builds a Pulse streaming pipeline declaratively. Construct
// via NewStreamBuilder, chain operator methods, then call Build (or pass to
// Streams.Deploy).
//
// All operator methods return the receiver so calls chain naturally. Methods
// that validate their inputs panic on obviously-bad arguments (blank required
// fields, non-positive counts, unknown enum values) so bugs are caught at
// call site, not after a 400 round-trip.
type StreamBuilder struct {
	name         string
	description  string
	agentLabel   string
	inputTopic   string
	sourceEngine string
	sourceConfig map[string]any
	sourceLabel  string
	outputTopic  string
	sinkChannel  string
	sinkConfig   map[string]any
	sinkLabel    string
	ops          []map[string]any
}

// NewStreamBuilder returns a builder with the given pipeline name. Pass "" if
// you want to set the name later via Named() or pass it to Build().
func NewStreamBuilder(name string) *StreamBuilder {
	b := &StreamBuilder{
		sourceConfig: map[string]any{},
		sinkConfig:   map[string]any{},
		ops:          []map[string]any{},
	}
	if name != "" {
		requireNonBlank("name", name)
		b.name = name
	}
	return b
}

// ------------------------------------------------------------------
// Source
// ------------------------------------------------------------------

// FromTopic sets the input topic + source node config.
func (b *StreamBuilder) FromTopic(topic string, options ...FromTopicOptions) *StreamBuilder {
	requireNonBlank("topic", topic)
	var opts FromTopicOptions
	if len(options) > 0 {
		opts = options[0]
	}
	b.inputTopic = topic
	if opts.SourceEngine != "" {
		b.sourceEngine = opts.SourceEngine
	} else {
		b.sourceEngine = "kafka"
	}
	b.sourceConfig = copyMap(opts.SourceConfig)
	b.sourceLabel = opts.Label
	return b
}

// ------------------------------------------------------------------
// Operators — each appends one entry to b.ops
// ------------------------------------------------------------------

// Filter operator. condition is a CEL-like expression string.
func (b *StreamBuilder) Filter(condition string) *StreamBuilder {
	requireNonBlank("condition", condition)
	b.ops = append(b.ops, map[string]any{"type": "filter", "condition": condition})
	return b
}

// Map operator. At least one of options.Fields / options.TargetType is required.
func (b *StreamBuilder) Map(options MapOptions) *StreamBuilder {
	if options.Fields == nil && options.TargetType == "" {
		panic("Map: operator does nothing — provide Fields or TargetType")
	}
	op := map[string]any{"type": "map"}
	if options.Fields != nil {
		copy := make(map[string]string, len(options.Fields))
		for k, v := range options.Fields {
			copy[k] = v
		}
		op["fields"] = copy
	}
	if options.TargetType != "" {
		op["targetType"] = options.TargetType
	}
	b.ops = append(b.ops, op)
	return b
}

// FlatMap — explode an array-valued field into one event per element.
func (b *StreamBuilder) FlatMap(splitField string) *StreamBuilder {
	requireNonBlank("splitField", splitField)
	b.ops = append(b.ops, map[string]any{"type": "flatMap", "splitField": splitField})
	return b
}

// KeyBy — group the stream by a top-level field value. Required before stateful operators.
func (b *StreamBuilder) KeyBy(field string) *StreamBuilder {
	requireNonBlank("field", field)
	b.ops = append(b.ops, map[string]any{"type": "keyBy", "field": field})
	return b
}

// Window operator. Aggregates events inside a window.
func (b *StreamBuilder) Window(spec WindowSpec, options ...WindowOptions) *StreamBuilder {
	if strings.TrimSpace(spec.Spec) == "" {
		panic("Window: spec must be non-empty")
	}
	var opts WindowOptions
	if len(options) > 0 {
		opts = options[0]
	}
	op := map[string]any{"type": "window", "spec": spec.Spec}
	if opts.Aggregations != nil {
		copy := make(map[string]string, len(opts.Aggregations))
		for k, v := range opts.Aggregations {
			copy[k] = v
		}
		op["aggregations"] = copy
	}
	if opts.OutputTopic != "" {
		op["outputTopic"] = opts.OutputTopic
	}
	if opts.Trigger != nil {
		op["trigger"] = opts.Trigger
	}
	b.ops = append(b.ops, op)
	return b
}

// WindowFromString — same as Window but takes the raw spec string. Useful when
// you've already validated the spec against WindowEngine.parseSpec.
func (b *StreamBuilder) WindowFromString(spec string, options ...WindowOptions) *StreamBuilder {
	requireNonBlank("spec", spec)
	return b.Window(WindowSpec{Spec: spec}, options...)
}

// Branch — route events to different topics by condition. Each event is sent
// to the FIRST branch whose condition matches.
func (b *StreamBuilder) Branch(branches []BranchSpec) *StreamBuilder {
	if len(branches) == 0 {
		panic("Branch: at least one branch is required")
	}
	normalised := make([]map[string]any, 0, len(branches))
	for i, br := range branches {
		if strings.TrimSpace(br.Condition) == "" {
			panic(fmt.Sprintf("Branch: branch[%d] requires a non-empty Condition", i))
		}
		if strings.TrimSpace(br.Topic) == "" {
			panic(fmt.Sprintf("Branch: branch[%d] requires a non-empty Topic", i))
		}
		normalised = append(normalised, map[string]any{
			"condition": br.Condition,
			"topic":     br.Topic,
		})
	}
	b.ops = append(b.ops, map[string]any{"type": "branch", "branches": normalised})
	return b
}

// Enrich — synchronous enrichment by joining against a state-store topic.
func (b *StreamBuilder) Enrich(lookupTopic, keyField string) *StreamBuilder {
	requireNonBlank("lookupTopic", lookupTopic)
	requireNonBlank("keyField", keyField)
	b.ops = append(b.ops, map[string]any{
		"type":        "enrich",
		"lookupTopic": lookupTopic,
		"keyField":    keyField,
	})
	return b
}

// EnrichAsync — asynchronous HTTP enrichment. options.URL supports {field}
// placeholders that get substituted from the event payload.
func (b *StreamBuilder) EnrichAsync(options EnrichAsyncOptions) *StreamBuilder {
	requireNonBlank("URL", options.URL)
	if options.Ordering != "" && options.Ordering != "PRESERVE_INPUT" && options.Ordering != "UNORDERED" {
		panic(fmt.Sprintf("EnrichAsync: Ordering must be PRESERVE_INPUT or UNORDERED, got %q", options.Ordering))
	}
	if options.OnFailure != "" && options.OnFailure != "EMIT_ERROR" && options.OnFailure != "DROP" && options.OnFailure != "PASS_THROUGH" {
		panic(fmt.Sprintf("EnrichAsync: OnFailure must be EMIT_ERROR, DROP or PASS_THROUGH, got %q", options.OnFailure))
	}
	op := map[string]any{"type": "enrichAsync", "url": options.URL}
	if options.Parallelism != nil {
		op["parallelism"] = *options.Parallelism
	}
	if options.QueueSize != nil {
		op["queueSize"] = *options.QueueSize
	}
	if options.TimeoutMs != nil {
		op["timeoutMs"] = *options.TimeoutMs
	}
	if options.MaxRetries != nil {
		op["maxRetries"] = *options.MaxRetries
	}
	if options.RetryBackoffMs != nil {
		op["retryBackoffMs"] = *options.RetryBackoffMs
	}
	if options.Ordering != "" {
		op["ordering"] = options.Ordering
	}
	if options.OnFailure != "" {
		op["onFailure"] = options.OnFailure
	}
	b.ops = append(b.ops, op)
	return b
}

// Cep — Complex Event Processing: match a sequence of conditions.
func (b *StreamBuilder) Cep(sequence []map[string]any, options ...CepOptions) *StreamBuilder {
	if len(sequence) == 0 {
		panic("Cep: requires a non-empty sequence")
	}
	var opts CepOptions
	if len(options) > 0 {
		opts = options[0]
	}
	copied := make([]map[string]any, 0, len(sequence))
	for _, step := range sequence {
		c := make(map[string]any, len(step))
		for k, v := range step {
			c[k] = v
		}
		copied = append(copied, c)
	}
	op := map[string]any{"type": "cep", "sequence": copied}
	if opts.Within != "" {
		op["within"] = opts.Within
	}
	if opts.Name != "" {
		op["name"] = opts.Name
	}
	b.ops = append(b.ops, op)
	return b
}

// MapLlm — B-109: enrich each event with an LLM completion. prompt supports
// {field} placeholders (and {__payload__}) substituted from the event
// server-side; the completion lands on the event under options.OutputField.
func (b *StreamBuilder) MapLlm(prompt string, options MapLlmOptions) *StreamBuilder {
	requireNonBlank("prompt", prompt)
	requireNonBlank("OutputField", options.OutputField)
	if options.Ordering != "" && options.Ordering != "PRESERVE_INPUT" && options.Ordering != "UNORDERED" {
		panic(fmt.Sprintf("MapLlm: Ordering must be PRESERVE_INPUT or UNORDERED, got %q", options.Ordering))
	}
	checkFailure("MapLlm", options.OnFailure)
	op := map[string]any{"type": "mapLlm", "prompt": prompt, "outputField": options.OutputField}
	if options.Model != "" {
		op["model"] = options.Model
	}
	if options.Temperature != nil {
		op["temperature"] = *options.Temperature
	}
	if options.MaxTokens != nil {
		op["maxTokens"] = *options.MaxTokens
	}
	if options.Parallelism != nil {
		op["parallelism"] = *options.Parallelism
	}
	if options.Ordering != "" {
		op["ordering"] = options.Ordering
	}
	if options.OnFailure != "" {
		op["onFailure"] = options.OnFailure
	}
	if options.MaxCallsPerSec != nil {
		op["maxCallsPerSec"] = *options.MaxCallsPerSec
	}
	b.ops = append(b.ops, op)
	return b
}

// Extract — B-109: LLM → typed structured fields merged into the event. The
// LLM is asked for a JSON object keyed by options.Schema's fields; missing /
// malformed fields become null server-side.
func (b *StreamBuilder) Extract(options ExtractOptions) *StreamBuilder {
	requireNonBlank("Instruction", options.Instruction)
	if len(options.Schema) == 0 {
		panic("Extract: requires a non-empty Schema")
	}
	checkFailure("Extract", options.OnFailure)
	schema := make(map[string]any, len(options.Schema))
	for k, v := range options.Schema {
		schema[k] = v
	}
	op := map[string]any{"type": "extract", "instruction": options.Instruction, "schema": schema}
	if options.Model != "" {
		op["model"] = options.Model
	}
	if options.Temperature != nil {
		op["temperature"] = *options.Temperature
	}
	if options.MaxTokens != nil {
		op["maxTokens"] = *options.MaxTokens
	}
	if options.OnFailure != "" {
		op["onFailure"] = options.OnFailure
	}
	b.ops = append(b.ops, op)
	return b
}

// McpCall — B-109 Phase 2: invoke an MCP tool per event. options.Args string
// values support {field} substitution. On success the tool output is written
// to options.OutputField (omit for a fire-and-forget side effect).
func (b *StreamBuilder) McpCall(tool string, options ...McpCallOptions) *StreamBuilder {
	requireNonBlank("tool", tool)
	op := map[string]any{"type": "mcpCall", "tool": tool}
	if len(options) > 0 {
		opts := options[0]
		if opts.Ordering != "" && opts.Ordering != "PRESERVE_INPUT" && opts.Ordering != "UNORDERED" {
			panic(fmt.Sprintf("McpCall: Ordering must be PRESERVE_INPUT or UNORDERED, got %q", opts.Ordering))
		}
		checkFailure("McpCall", opts.OnFailure)
		if opts.Args != nil {
			args := make(map[string]any, len(opts.Args))
			for k, v := range opts.Args {
				args[k] = v
			}
			op["args"] = args
		}
		if opts.OutputField != "" {
			op["outputField"] = opts.OutputField
		}
		if opts.Parallelism != nil {
			op["parallelism"] = *opts.Parallelism
		}
		if opts.Ordering != "" {
			op["ordering"] = opts.Ordering
		}
		if opts.OnFailure != "" {
			op["onFailure"] = opts.OnFailure
		}
	}
	b.ops = append(b.ops, op)
	return b
}

// BroadcastJoin — enrich the stream against a fully-replicated table.
func (b *StreamBuilder) BroadcastJoin(options BroadcastJoinOptions) *StreamBuilder {
	requireNonBlank("JoinKeyField", options.JoinKeyField)
	if options.RefreshMode != "" && options.RefreshMode != "cdc" && options.RefreshMode != "periodic" && options.RefreshMode != "explicit" {
		panic(fmt.Sprintf("BroadcastJoin: RefreshMode must be cdc, periodic or explicit, got %q", options.RefreshMode))
	}
	op := map[string]any{"type": "broadcastJoin", "joinKeyField": options.JoinKeyField}
	if options.StreamingTopic != "" {
		op["streamingTopic"] = options.StreamingTopic
	}
	if options.Name != "" {
		op["name"] = options.Name
	}
	if options.MaxBytes != nil {
		op["maxBytes"] = *options.MaxBytes
	}
	if options.RefreshMode != "" {
		op["refreshMode"] = options.RefreshMode
	}
	if options.IntervalMillis != nil {
		op["intervalMillis"] = *options.IntervalMillis
	}
	b.ops = append(b.ops, op)
	return b
}

// CdcJoin — stream-table join against a CDC-fed state table.
func (b *StreamBuilder) CdcJoin(options CdcJoinOptions) *StreamBuilder {
	requireNonBlank("Source", options.Source)
	op := map[string]any{"type": "cdcJoin", "source": options.Source}
	if options.JoinKey != "" {
		op["joinKey"] = options.JoinKey
	}
	if options.Table != "" {
		op["table"] = options.Table
	}
	if options.StateBackend != "" {
		op["stateBackend"] = options.StateBackend
	}
	b.ops = append(b.ops, op)
	return b
}

// ------------------------------------------------------------------
// Sink
// ------------------------------------------------------------------

// ToTopic sets the output topic + optional sink node config. SinkChannel empty
// (the default) → no sink node is emitted (downstream consumers subscribe).
func (b *StreamBuilder) ToTopic(topic string, options ...ToTopicOptions) *StreamBuilder {
	requireNonBlank("topic", topic)
	var opts ToTopicOptions
	if len(options) > 0 {
		opts = options[0]
	}
	b.outputTopic = topic
	b.sinkChannel = opts.SinkChannel
	b.sinkConfig = copyMap(opts.SinkConfig)
	b.sinkLabel = opts.Label
	return b
}

// ToState — terminate the stream in the agent's state store (queryable via
// B-106 IQ). No sink node is emitted and no output topic is set.
func (b *StreamBuilder) ToState() *StreamBuilder {
	b.outputTopic = ""
	b.sinkChannel = ""
	b.sinkConfig = map[string]any{}
	b.sinkLabel = ""
	return b
}

// ------------------------------------------------------------------
// Metadata
// ------------------------------------------------------------------

// Named sets / overrides the pipeline name.
func (b *StreamBuilder) Named(name string) *StreamBuilder {
	requireNonBlank("name", name)
	b.name = name
	return b
}

// DescribedAs sets the pipeline description.
func (b *StreamBuilder) DescribedAs(description string) *StreamBuilder {
	b.description = description
	return b
}

// WithAgentLabel sets the display label for the streaming agent node.
func (b *StreamBuilder) WithAgentLabel(label string) *StreamBuilder {
	requireNonBlank("label", label)
	b.agentLabel = label
	return b
}

// ------------------------------------------------------------------
// Compilation
// ------------------------------------------------------------------

// Operators returns a copy of the recorded operator chain.
func (b *StreamBuilder) Operators() []map[string]any {
	out := make([]map[string]any, len(b.ops))
	for i, op := range b.ops {
		c := make(map[string]any, len(op))
		for k, v := range op {
			c[k] = v
		}
		out[i] = c
	}
	return out
}

// Build compiles the chain into a Pulse pipeline dict ready for POST.
//
// overrideName overrides the pipeline name set via NewStreamBuilder/Named.
// Pass "" to use the preset name.
func (b *StreamBuilder) Build(overrideName string) (map[string]any, error) {
	pipelineName := overrideName
	if pipelineName == "" {
		pipelineName = b.name
	}
	if pipelineName == "" {
		return nil, errors.New("pulse: pipeline name required — pass to NewStreamBuilder or Build(name)")
	}
	if b.inputTopic == "" {
		return nil, errors.New("pulse: no source — call FromTopic(...) before Build()")
	}
	if len(b.ops) == 0 {
		return nil, errors.New("pulse: no operators — chain at least one of Filter/Map/KeyBy/... before Build()")
	}

	nodes := make([]map[string]any, 0, 3)

	// Source node
	srcConfig := map[string]any{
		"engine":     b.sourceEngine,
		"inputTopic": b.inputTopic,
	}
	for k, v := range b.sourceConfig {
		srcConfig[k] = v
	}
	srcLabel := b.sourceLabel
	if srcLabel == "" {
		srcLabel = b.sourceEngine + " source"
	}
	nodes = append(nodes, map[string]any{
		"type":   "source",
		"label":  srcLabel,
		"config": srcConfig,
	})

	// Agent node
	opCopy := make([]map[string]any, len(b.ops))
	for i, op := range b.ops {
		c := make(map[string]any, len(op))
		for k, v := range op {
			c[k] = v
		}
		opCopy[i] = c
	}
	agentConfig := map[string]any{
		"engine":     "streaming",
		"inputTopic": b.inputTopic,
		"operators":  opCopy,
	}
	if b.outputTopic != "" {
		agentConfig["outputTopic"] = b.outputTopic
	}
	agentLabel := b.agentLabel
	if agentLabel == "" {
		agentLabel = pipelineName
	}
	nodes = append(nodes, map[string]any{
		"type":   "agent",
		"label":  agentLabel,
		"config": agentConfig,
	})

	// Sink node — only when both output topic AND sink channel are set
	if b.outputTopic != "" && b.sinkChannel != "" {
		sinkConf := map[string]any{
			"channel":    b.sinkChannel,
			"inputTopic": b.outputTopic,
		}
		for k, v := range b.sinkConfig {
			sinkConf[k] = v
		}
		sinkLabel := b.sinkLabel
		if sinkLabel == "" {
			sinkLabel = b.sinkChannel + " sink"
		}
		nodes = append(nodes, map[string]any{
			"type":   "sink",
			"label":  sinkLabel,
			"config": sinkConf,
		})
	}

	pipeline := map[string]any{"name": pipelineName, "nodes": nodes}
	if b.description != "" {
		pipeline["description"] = b.description
	}
	return pipeline, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func requireNonBlank(name, value string) {
	if strings.TrimSpace(value) == "" {
		panic(fmt.Sprintf("%s must be a non-empty string, got %q", name, value))
	}
}

// checkFailure panics if onFailure is set to an invalid value (B-109).
func checkFailure(op, onFailure string) {
	if onFailure != "" && onFailure != "EMIT_ERROR" && onFailure != "DROP" && onFailure != "PASS_THROUGH" {
		panic(fmt.Sprintf("%s: OnFailure must be EMIT_ERROR, DROP or PASS_THROUGH, got %q", op, onFailure))
	}
}

func copyMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	c := make(map[string]any, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}
