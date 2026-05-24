package pulse

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// B-107 streams DSL tests. Mirrors pulse-py / pulse-js / pulse-java coverage:
// per-operator shape + constructor validation + iot-template round-trip +
// streams.Deploy HTTP integration.

// ---------------------------------------------------------------------------
// Window-spec factories
// ---------------------------------------------------------------------------

func TestWindows_TumblingEmitsExpectedString(t *testing.T) {
	if got := WindowsTumbling("60s").Spec; got != "tumbling(60s)" {
		t.Fatalf("got %q", got)
	}
}

func TestWindows_SlidingEmitsExpectedString(t *testing.T) {
	if got := WindowsSliding("10m", "1m").Spec; got != "sliding(10m,1m)" {
		t.Fatalf("got %q", got)
	}
}

func TestWindows_SessionEmitsExpectedString(t *testing.T) {
	if got := WindowsSession("30s").Spec; got != "session(30s)" {
		t.Fatalf("got %q", got)
	}
}

func TestWindows_GlobalEmitsExpectedString(t *testing.T) {
	if got := WindowsGlobal().Spec; got != "global" {
		t.Fatalf("got %q", got)
	}
}

func TestWindows_CountEmitsExpectedString(t *testing.T) {
	if got := WindowsCount(100).Spec; got != "count(100)" {
		t.Fatalf("got %q", got)
	}
}

func TestWindows_CountSlidingEmitsExpectedString(t *testing.T) {
	if got := WindowsCountSliding(100, 10).Spec; got != "count_sliding(100,10)" {
		t.Fatalf("got %q", got)
	}
}

func TestWindows_TumblingRejectsBlank(t *testing.T) {
	expectPanic(t, "size", func() { WindowsTumbling("") })
}

func TestWindows_SlidingRejectsBlank(t *testing.T) {
	expectPanic(t, "slide", func() { WindowsSliding("10m", "") })
	expectPanic(t, "size", func() { WindowsSliding("   ", "1m") })
}

func TestWindows_SessionRejectsBlank(t *testing.T) {
	expectPanic(t, "timeout", func() { WindowsSession("") })
}

func TestWindows_CountRejectsNonPositive(t *testing.T) {
	expectPanic(t, "positive", func() { WindowsCount(0) })
	expectPanic(t, "positive", func() { WindowsCount(-5) })
}

func TestWindows_CountSlidingRejectsNonPositive(t *testing.T) {
	expectPanic(t, "positive", func() { WindowsCountSliding(100, 0) })
	expectPanic(t, "positive", func() { WindowsCountSliding(0, 10) })
}

func TestWindows_StringMatchesSpec(t *testing.T) {
	if got := WindowsTumbling("60s").String(); got != "tumbling(60s)" {
		t.Fatalf("got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Aggregator factories
// ---------------------------------------------------------------------------

func TestAggs_AllEmitExpectedStrings(t *testing.T) {
	cases := map[string]string{
		AggsCount():                 "count()",
		AggsSum("amount"):           "sum(amount)",
		AggsAvg("price"):            "avg(price)",
		AggsMin("latency"):          "min(latency)",
		AggsMax("latency"):          "max(latency)",
		AggsCollectList("sku"):      "collect_list(sku)",
		AggsDistinctCount("userId"): "distinct_count(userId)",
	}
	for got, want := range cases {
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestAggs_RejectBlankField(t *testing.T) {
	for name, fn := range map[string]func(string) string{
		"AggsSum":           AggsSum,
		"AggsAvg":           AggsAvg,
		"AggsMin":           AggsMin,
		"AggsMax":           AggsMax,
		"AggsCollectList":   AggsCollectList,
		"AggsDistinctCount": AggsDistinctCount,
	} {
		fn := fn
		t.Run(name, func(t *testing.T) {
			expectPanic(t, "field", func() { fn("") })
			expectPanic(t, "field", func() { fn("   ") })
		})
	}
}

// ---------------------------------------------------------------------------
// StreamBuilder — per-operator shape
// ---------------------------------------------------------------------------

func TestStreamBuilder_FilterEmitsValidatorShape(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").Filter("amount > 1000")
	want := []map[string]any{{"type": "filter", "condition": "amount > 1000"}}
	if got := b.Operators(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestStreamBuilder_FilterRejectsBlank(t *testing.T) {
	expectPanic(t, "condition", func() {
		NewStreamBuilder("").FromTopic("in").Filter("")
	})
}

func TestStreamBuilder_MapWithFieldsOnly(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").Map(MapOptions{
		Fields: map[string]string{"alert": "concat(id, '!')"},
	})
	ops := b.Operators()
	if len(ops) != 1 || ops[0]["type"] != "map" {
		t.Fatalf("got %v", ops)
	}
	fields := ops[0]["fields"].(map[string]string)
	if fields["alert"] != "concat(id, '!')" {
		t.Fatalf("got %v", fields)
	}
}

func TestStreamBuilder_MapWithTargetTypeOnly(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").Map(MapOptions{TargetType: "alert"})
	want := []map[string]any{{"type": "map", "targetType": "alert"}}
	if got := b.Operators(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestStreamBuilder_MapRejectsEmpty(t *testing.T) {
	expectPanic(t, "does nothing", func() {
		NewStreamBuilder("").FromTopic("in").Map(MapOptions{})
	})
}

func TestStreamBuilder_FlatMap(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").FlatMap("items")
	want := []map[string]any{{"type": "flatMap", "splitField": "items"}}
	if got := b.Operators(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestStreamBuilder_FlatMapRejectsBlank(t *testing.T) {
	expectPanic(t, "splitField", func() {
		NewStreamBuilder("").FromTopic("in").FlatMap("")
	})
}

func TestStreamBuilder_KeyBy(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").KeyBy("deviceId")
	want := []map[string]any{{"type": "keyBy", "field": "deviceId"}}
	if got := b.Operators(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestStreamBuilder_KeyByRejectsBlank(t *testing.T) {
	expectPanic(t, "field", func() {
		NewStreamBuilder("").FromTopic("in").KeyBy("")
	})
}

func TestStreamBuilder_WindowWithAggregations(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").Window(
		WindowsTumbling("60s"),
		WindowOptions{Aggregations: map[string]string{"avgTemp": AggsAvg("temperature")}},
	)
	ops := b.Operators()
	if len(ops) != 1 || ops[0]["type"] != "window" {
		t.Fatalf("got %v", ops)
	}
	if ops[0]["spec"] != "tumbling(60s)" {
		t.Fatalf("got spec %v", ops[0]["spec"])
	}
	aggs := ops[0]["aggregations"].(map[string]string)
	if aggs["avgTemp"] != "avg(temperature)" {
		t.Fatalf("got %v", aggs)
	}
}

func TestStreamBuilder_WindowAcceptsRawStringSpec(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").WindowFromString("sliding(10m,1m)")
	want := []map[string]any{{"type": "window", "spec": "sliding(10m,1m)"}}
	if got := b.Operators(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestStreamBuilder_WindowWithOutputTopicAndTrigger(t *testing.T) {
	trig := map[string]any{"kind": "earlyFire", "afterEvents": 10}
	b := NewStreamBuilder("").FromTopic("in").Window(
		WindowsTumbling("60s"),
		WindowOptions{OutputTopic: "late-data", Trigger: trig},
	)
	op := b.Operators()[0]
	if op["outputTopic"] != "late-data" {
		t.Fatalf("got %v", op["outputTopic"])
	}
	if !reflect.DeepEqual(op["trigger"], trig) {
		t.Fatalf("got %v", op["trigger"])
	}
}

func TestStreamBuilder_WindowRejectsBlankSpecString(t *testing.T) {
	expectPanic(t, "spec", func() {
		NewStreamBuilder("").FromTopic("in").WindowFromString("")
	})
}

func TestStreamBuilder_BranchEmitsValidatorShape(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").Branch([]BranchSpec{
		{Condition: "tier == 'gold'", Topic: "vip-events"},
		{Condition: "tier == 'silver'", Topic: "std-events"},
	})
	ops := b.Operators()
	if len(ops) != 1 || ops[0]["type"] != "branch" {
		t.Fatalf("got %v", ops)
	}
	branches := ops[0]["branches"].([]map[string]any)
	if len(branches) != 2 || branches[0]["condition"] != "tier == 'gold'" {
		t.Fatalf("got %v", branches)
	}
}

func TestStreamBuilder_BranchRejectsEmpty(t *testing.T) {
	expectPanic(t, "at least one", func() {
		NewStreamBuilder("").FromTopic("in").Branch(nil)
	})
}

func TestStreamBuilder_BranchRejectsMissingFields(t *testing.T) {
	expectPanic(t, "Condition", func() {
		NewStreamBuilder("").FromTopic("in").Branch([]BranchSpec{{Topic: "x"}})
	})
	expectPanic(t, "Topic", func() {
		NewStreamBuilder("").FromTopic("in").Branch([]BranchSpec{{Condition: "x > 0"}})
	})
}

func TestStreamBuilder_Enrich(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").Enrich("customers", "customerId")
	want := []map[string]any{{
		"type":        "enrich",
		"lookupTopic": "customers",
		"keyField":    "customerId",
	}}
	if got := b.Operators(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestStreamBuilder_EnrichRejectsBlankArgs(t *testing.T) {
	expectPanic(t, "lookupTopic", func() {
		NewStreamBuilder("").FromTopic("in").Enrich("", "k")
	})
	expectPanic(t, "keyField", func() {
		NewStreamBuilder("").FromTopic("in").Enrich("t", "")
	})
}

func TestStreamBuilder_EnrichAsyncFullShape(t *testing.T) {
	p, q, tm, mr, rb := 8, 128, 5000, 3, 200
	b := NewStreamBuilder("").FromTopic("in").EnrichAsync(EnrichAsyncOptions{
		URL:            "https://x.example/lookup/{id}",
		Parallelism:    &p,
		QueueSize:      &q,
		TimeoutMs:      &tm,
		MaxRetries:     &mr,
		RetryBackoffMs: &rb,
		Ordering:       "PRESERVE_INPUT",
		OnFailure:      "EMIT_ERROR",
	})
	op := b.Operators()[0]
	want := map[string]any{
		"type":           "enrichAsync",
		"url":            "https://x.example/lookup/{id}",
		"parallelism":    8,
		"queueSize":      128,
		"timeoutMs":      5000,
		"maxRetries":     3,
		"retryBackoffMs": 200,
		"ordering":       "PRESERVE_INPUT",
		"onFailure":      "EMIT_ERROR",
	}
	if !reflect.DeepEqual(op, want) {
		t.Fatalf("got %v, want %v", op, want)
	}
}

func TestStreamBuilder_EnrichAsyncRejectsBadOrdering(t *testing.T) {
	expectPanic(t, "Ordering", func() {
		NewStreamBuilder("").FromTopic("in").EnrichAsync(EnrichAsyncOptions{
			URL: "https://x", Ordering: "SHUFFLED",
		})
	})
}

func TestStreamBuilder_EnrichAsyncRejectsBadOnFailure(t *testing.T) {
	expectPanic(t, "OnFailure", func() {
		NewStreamBuilder("").FromTopic("in").EnrichAsync(EnrichAsyncOptions{
			URL: "https://x", OnFailure: "EXPLODE",
		})
	})
}

func TestStreamBuilder_CepEmitsValidatorShape(t *testing.T) {
	seq := []map[string]any{
		{"name": "add", "match": "type == 'ADD_TO_CART'", "within": "10m"},
		{"name": "view", "match": "type == 'VIEW_CART'", "follow": "followedBy"},
	}
	b := NewStreamBuilder("").FromTopic("in").Cep(seq, CepOptions{Within: "20m", Name: "cart-flow"})
	op := b.Operators()[0]
	if op["type"] != "cep" || op["within"] != "20m" || op["name"] != "cart-flow" {
		t.Fatalf("got %v", op)
	}
	out := op["sequence"].([]map[string]any)
	if len(out) != 2 || out[0]["match"] != "type == 'ADD_TO_CART'" {
		t.Fatalf("got %v", out)
	}
}

func TestStreamBuilder_CepRejectsEmptySequence(t *testing.T) {
	expectPanic(t, "non-empty sequence", func() {
		NewStreamBuilder("").FromTopic("in").Cep(nil)
	})
}

func TestStreamBuilder_BroadcastJoinFullShape(t *testing.T) {
	mb := int64(10_000_000)
	im := 30_000
	b := NewStreamBuilder("").FromTopic("in").BroadcastJoin(BroadcastJoinOptions{
		JoinKeyField:   "userId",
		StreamingTopic: "users-table",
		Name:           "users-join",
		MaxBytes:       &mb,
		RefreshMode:    "cdc",
		IntervalMillis: &im,
	})
	want := map[string]any{
		"type":           "broadcastJoin",
		"joinKeyField":   "userId",
		"streamingTopic": "users-table",
		"name":           "users-join",
		"maxBytes":       int64(10_000_000),
		"refreshMode":    "cdc",
		"intervalMillis": 30_000,
	}
	if got := b.Operators()[0]; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestStreamBuilder_BroadcastJoinRejectsBadRefreshMode(t *testing.T) {
	expectPanic(t, "RefreshMode", func() {
		NewStreamBuilder("").FromTopic("in").BroadcastJoin(BroadcastJoinOptions{
			JoinKeyField: "k", RefreshMode: "random",
		})
	})
}

func TestStreamBuilder_CdcJoinFullShape(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").CdcJoin(CdcJoinOptions{
		Source:       "postgres://orders",
		JoinKey:      "orderId",
		Table:        "orders",
		StateBackend: "rocksdb",
	})
	want := map[string]any{
		"type":         "cdcJoin",
		"source":       "postgres://orders",
		"joinKey":      "orderId",
		"table":        "orders",
		"stateBackend": "rocksdb",
	}
	if got := b.Operators()[0]; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestStreamBuilder_CdcJoinMinimalShape(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").CdcJoin(CdcJoinOptions{Source: "postgres://orders"})
	want := []map[string]any{{"type": "cdcJoin", "source": "postgres://orders"}}
	if got := b.Operators(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

// ---------------------------------------------------------------------------
// StreamBuilder — full pipeline compilation
// ---------------------------------------------------------------------------

func TestStreamBuilder_BuildMinimalPipeline(t *testing.T) {
	out, err := NewStreamBuilder("p1").FromTopic("in").Filter("x > 0").Build("")
	if err != nil {
		t.Fatal(err)
	}
	if out["name"] != "p1" {
		t.Fatalf("name: %v", out["name"])
	}
	nodes := out["nodes"].([]map[string]any)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0]["type"] != "source" || nodes[1]["type"] != "agent" {
		t.Fatalf("wrong node types: %v / %v", nodes[0]["type"], nodes[1]["type"])
	}
	srcConfig := nodes[0]["config"].(map[string]any)
	if srcConfig["engine"] != "kafka" || srcConfig["inputTopic"] != "in" {
		t.Fatalf("src config: %v", srcConfig)
	}
	ac := nodes[1]["config"].(map[string]any)
	if ac["engine"] != "streaming" {
		t.Fatalf("agent engine: %v", ac["engine"])
	}
}

func TestStreamBuilder_BuildNameViaNamed(t *testing.T) {
	out, err := NewStreamBuilder("").Named("p2").FromTopic("in").Filter("x > 0").Build("")
	if err != nil {
		t.Fatal(err)
	}
	if out["name"] != "p2" {
		t.Fatalf("name: %v", out["name"])
	}
}

func TestStreamBuilder_BuildOverrideNameWins(t *testing.T) {
	out, _ := NewStreamBuilder("ignored").FromTopic("in").Filter("x > 0").Build("actual")
	if out["name"] != "actual" {
		t.Fatalf("name: %v", out["name"])
	}
}

func TestStreamBuilder_DescriptionPropagates(t *testing.T) {
	out, _ := NewStreamBuilder("p3").DescribedAs("desc").FromTopic("in").Filter("x > 0").Build("")
	if out["description"] != "desc" {
		t.Fatalf("desc: %v", out["description"])
	}
}

func TestStreamBuilder_AgentLabel(t *testing.T) {
	out, _ := NewStreamBuilder("p4").WithAgentLabel("Per-Device Average").FromTopic("in").Filter("x > 0").Build("")
	nodes := out["nodes"].([]map[string]any)
	if nodes[1]["label"] != "Per-Device Average" {
		t.Fatalf("label: %v", nodes[1]["label"])
	}
}

func TestStreamBuilder_EmitsSinkWhenChannelSet(t *testing.T) {
	out, _ := NewStreamBuilder("p5").FromTopic("in").Filter("x > 0").
		ToTopic("out", ToTopicOptions{SinkChannel: "email"}).Build("")
	nodes := out["nodes"].([]map[string]any)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}
	want := map[string]any{
		"type":   "sink",
		"label":  "email sink",
		"config": map[string]any{"channel": "email", "inputTopic": "out"},
	}
	if !reflect.DeepEqual(nodes[2], want) {
		t.Fatalf("got %v, want %v", nodes[2], want)
	}
}

func TestStreamBuilder_SkipsSinkWhenNoChannel(t *testing.T) {
	out, _ := NewStreamBuilder("p6").FromTopic("in").Filter("x > 0").ToTopic("out").Build("")
	nodes := out["nodes"].([]map[string]any)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	ac := nodes[1]["config"].(map[string]any)
	if ac["outputTopic"] != "out" {
		t.Fatalf("outputTopic: %v", ac["outputTopic"])
	}
}

func TestStreamBuilder_ToStateClearsOutputAndSink(t *testing.T) {
	out, _ := NewStreamBuilder("p7").FromTopic("in").Filter("x > 0").
		ToTopic("out", ToTopicOptions{SinkChannel: "email"}).ToState().Build("")
	nodes := out["nodes"].([]map[string]any)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	ac := nodes[1]["config"].(map[string]any)
	if _, has := ac["outputTopic"]; has {
		t.Fatalf("expected outputTopic absent, got %v", ac["outputTopic"])
	}
}

func TestStreamBuilder_SourceEngineAndLabel(t *testing.T) {
	out, _ := NewStreamBuilder("p8").FromTopic("in", FromTopicOptions{
		SourceEngine: "mqtt",
		SourceConfig: map[string]any{"qos": 1},
		Label:        "Sensor readings",
	}).Filter("x > 0").Build("")
	nodes := out["nodes"].([]map[string]any)
	if nodes[0]["label"] != "Sensor readings" {
		t.Fatalf("label: %v", nodes[0]["label"])
	}
	sc := nodes[0]["config"].(map[string]any)
	want := map[string]any{"engine": "mqtt", "inputTopic": "in", "qos": 1}
	if !reflect.DeepEqual(sc, want) {
		t.Fatalf("src config: %v", sc)
	}
}

func TestStreamBuilder_BuildRejectsMissingName(t *testing.T) {
	_, err := NewStreamBuilder("").FromTopic("in").Filter("x > 0").Build("")
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("got %v", err)
	}
}

func TestStreamBuilder_BuildRejectsMissingSource(t *testing.T) {
	_, err := NewStreamBuilder("p").Filter("x > 0").Build("")
	if err == nil || !strings.Contains(err.Error(), "source") {
		t.Fatalf("got %v", err)
	}
}

func TestStreamBuilder_BuildRejectsEmptyChain(t *testing.T) {
	_, err := NewStreamBuilder("p").FromTopic("in").Build("")
	if err == nil || !strings.Contains(err.Error(), "operators") {
		t.Fatalf("got %v", err)
	}
}

func TestStreamBuilder_NewBuilderRejectsBlankName(t *testing.T) {
	expectPanic(t, "name", func() { NewStreamBuilder(" ") })
}

func TestStreamBuilder_OperatorsReturnsCopyNotMutating(t *testing.T) {
	b := NewStreamBuilder("").FromTopic("in").Filter("x > 0")
	snapshot := b.Operators()
	snapshot[0]["type"] = "tampered"
	again := b.Operators()
	if again[0]["type"] != "filter" {
		t.Fatalf("internal chain was mutated: %v", again)
	}
}

func TestStreamBuilder_ChainOrderingPreserved(t *testing.T) {
	out, _ := NewStreamBuilder("p9").FromTopic("in").
		Filter("a > 0").
		KeyBy("k").
		Window(WindowsTumbling("60s"), WindowOptions{Aggregations: map[string]string{"cnt": AggsCount()}}).
		Filter("cnt > 5").
		Map(MapOptions{Fields: map[string]string{"out": "cnt"}}).
		Build("")
	nodes := out["nodes"].([]map[string]any)
	ac := nodes[1]["config"].(map[string]any)
	ops := ac["operators"].([]map[string]any)
	types := make([]string, len(ops))
	for i, op := range ops {
		types[i] = op["type"].(string)
	}
	want := []string{"filter", "keyBy", "window", "filter", "map"}
	if !reflect.DeepEqual(types, want) {
		t.Fatalf("got %v", types)
	}
}

func TestStreamBuilder_IotTemplateRoundTrip(t *testing.T) {
	out, err := NewStreamBuilder("iot-temperature-aggregator").
		WithAgentLabel("Per-Device Average").
		FromTopic("sensor-readings", FromTopicOptions{
			SourceEngine: "mqtt",
			Label:        "Sensor readings",
		}).
		KeyBy("deviceId").
		Window(WindowsTumbling("60s"), WindowOptions{
			Aggregations: map[string]string{"avgTemp": AggsAvg("temperature")},
		}).
		Filter("avgTemp > 75").
		ToTopic("sensor-minute-averages", ToTopicOptions{
			SinkChannel: "email",
			Label:       "Heat Alert",
		}).
		Build("")
	if err != nil {
		t.Fatal(err)
	}

	nodes := out["nodes"].([]map[string]any)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}
	types := []string{nodes[0]["type"].(string), nodes[1]["type"].(string), nodes[2]["type"].(string)}
	if !reflect.DeepEqual(types, []string{"source", "agent", "sink"}) {
		t.Fatalf("types: %v", types)
	}

	srcConfig := nodes[0]["config"].(map[string]any)
	if !reflect.DeepEqual(srcConfig, map[string]any{
		"engine": "mqtt", "inputTopic": "sensor-readings",
	}) {
		t.Fatalf("src config: %v", srcConfig)
	}
	if nodes[0]["label"] != "Sensor readings" {
		t.Fatalf("src label: %v", nodes[0]["label"])
	}

	if nodes[1]["label"] != "Per-Device Average" {
		t.Fatalf("agent label: %v", nodes[1]["label"])
	}
	ac := nodes[1]["config"].(map[string]any)
	if ac["engine"] != "streaming" || ac["inputTopic"] != "sensor-readings" || ac["outputTopic"] != "sensor-minute-averages" {
		t.Fatalf("agent config: %v", ac)
	}
	wantOps := []map[string]any{
		{"type": "keyBy", "field": "deviceId"},
		{"type": "window", "spec": "tumbling(60s)", "aggregations": map[string]string{"avgTemp": "avg(temperature)"}},
		{"type": "filter", "condition": "avgTemp > 75"},
	}
	if !reflect.DeepEqual(ac["operators"], wantOps) {
		t.Fatalf("operators: %v", ac["operators"])
	}

	if nodes[2]["label"] != "Heat Alert" {
		t.Fatalf("sink label: %v", nodes[2]["label"])
	}
	sinkConfig := nodes[2]["config"].(map[string]any)
	if !reflect.DeepEqual(sinkConfig, map[string]any{
		"channel": "email", "inputTopic": "sensor-minute-averages",
	}) {
		t.Fatalf("sink config: %v", sinkConfig)
	}
}

// ---------------------------------------------------------------------------
// StreamsService — compile + deploy
// ---------------------------------------------------------------------------

func TestStreams_AccessorExists(t *testing.T) {
	c := newClient(t, "http://example.test")
	if c.Streams == nil {
		t.Fatal("Streams accessor is nil")
	}
}

func TestStreams_CompileReturnsDictWithoutHttpCall(t *testing.T) {
	hits := 0
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) { hits++ })
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	b := NewStreamBuilder("p").FromTopic("in").Filter("x > 0")
	out, err := c.Streams.Compile(b)
	if err != nil {
		t.Fatal(err)
	}
	if out["name"] != "p" {
		t.Fatalf("name: %v", out["name"])
	}
	if hits != 0 {
		t.Fatalf("expected no HTTP call, got %d hits", hits)
	}
}

func TestStreams_DeployPostsBuiltDefinition(t *testing.T) {
	var receivedBody []byte
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/pulse/pipelines" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		receivedBody = readAll(t, r)
		writeJSON(t, w, 201, map[string]any{
			"id": "p-42", "name": "fraud-detector", "status": "running",
		})
	})
	defer stop()

	c := newClient(t, endpoint, WithToken("fake.jwt"))
	b := NewStreamBuilder("fraud-detector").
		FromTopic("payments").
		Filter("amount > 1000").
		KeyBy("customer_id").
		Window(WindowsTumbling("60s"), WindowOptions{
			Aggregations: map[string]string{"cnt": AggsCount()},
		}).
		Filter("cnt > 5").
		ToTopic("fraud-alerts")

	result, err := c.Streams.Deploy(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if result["id"] != "p-42" {
		t.Fatalf("id: %v", result["id"])
	}

	var body map[string]any
	if err := json.Unmarshal(receivedBody, &body); err != nil {
		t.Fatalf("body not JSON: %s", receivedBody)
	}
	if body["name"] != "fraud-detector" {
		t.Fatalf("name: %v", body["name"])
	}
	nodes := body["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes (no sink — no SinkChannel), got %d", len(nodes))
	}
	ac := nodes[1].(map[string]any)["config"].(map[string]any)
	ops := ac["operators"].([]any)
	if ops[2].(map[string]any)["type"] != "window" {
		t.Fatalf("op[2]: %v", ops[2])
	}
}

func TestStreams_DeployNameOverridePropagates(t *testing.T) {
	var receivedBody []byte
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		receivedBody = readAll(t, r)
		writeJSON(t, w, 201, map[string]any{"id": "p", "name": "renamed"})
	})
	defer stop()

	c := newClient(t, endpoint, WithToken("fake.jwt"))
	b := NewStreamBuilder("original").FromTopic("in").Filter("x > 0")
	if _, err := c.Streams.DeployWithName(context.Background(), b, "renamed"); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	_ = json.Unmarshal(receivedBody, &body)
	if body["name"] != "renamed" {
		t.Fatalf("expected name=renamed, got %v", body["name"])
	}
}

func TestStreams_DeployWithoutTokenReturnsAuthError(t *testing.T) {
	hits := 0
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) { hits++ })
	defer stop()

	c := newClient(t, endpoint) // no WithToken
	b := NewStreamBuilder("p").FromTopic("in").Filter("x > 0")
	_, err := c.Streams.Deploy(context.Background(), b)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if hits != 0 {
		t.Fatalf("server should not have been called, was hit %d time(s)", hits)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// expectPanic runs fn and asserts that it panics with a message containing
// substr. Required because the DSL panics on programmer errors (bad args).
func expectPanic(t *testing.T, substr string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q, got none", substr)
		}
		msg, ok := r.(string)
		if !ok {
			if e, isErr := r.(error); isErr {
				msg = e.Error()
			} else {
				msg = ""
			}
		}
		if !strings.Contains(msg, substr) {
			t.Fatalf("expected panic containing %q, got %q", substr, msg)
		}
	}()
	fn()
}
