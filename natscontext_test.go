package natscontext_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/alecmerdler/natscontext"
)

func startServer(t *testing.T) *server.Server {
	t.Helper()
	dir, err := os.MkdirTemp("", "natscontext-js")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  dir,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(s.Shutdown)
	return s
}

func newKV(t *testing.T, s *server.Server, bucket string) jetstream.KeyValue {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	kv, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket:  bucket,
		History: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return kv
}

func TestWithValueAndValue(t *testing.T) {
	s := startServer(t)
	kv := newKV(t, s, "KV_TEST")

	ctx, err := natscontext.New(context.Background(), kv, "session-1")
	if err != nil {
		t.Fatal(err)
	}

	const myKey natscontext.NatsContextValue = "user"

	ctx2, err := natscontext.WithValue(ctx, myKey, "alice")
	if err != nil {
		t.Fatal(err)
	}

	if got := ctx2.Value(myKey); got != "alice" {
		t.Fatalf("expected alice, got %v", got)
	}

	// The original ctx (same namespace) should observe the write too, since
	// reads always query the shared KV bucket.
	if got := ctx.Value(myKey); got != "alice" {
		t.Fatalf("expected original ctx to observe write, got %v", got)
	}
}

func TestCrossNodeVisibility(t *testing.T) {
	s := startServer(t)

	kvA := newKV(t, s, "KV_SHARED")
	kvB := newKV(t, s, "KV_SHARED")

	nodeA, err := natscontext.New(context.Background(), kvA, "session-shared")
	if err != nil {
		t.Fatal(err)
	}
	nodeB, err := natscontext.New(context.Background(), kvB, "session-shared")
	if err != nil {
		t.Fatal(err)
	}

	const k natscontext.NatsContextValue = "counter-seed"

	if _, err := natscontext.WithValue(nodeA, k, 42); err != nil {
		t.Fatal(err)
	}

	if got := nodeB.Value(k); got != float64(42) {
		t.Fatalf("expected node B to observe write from node A, got %v (%T)", got, got)
	}
}

func TestWithValueFuncCounter(t *testing.T) {
	s := startServer(t)
	kv := newKV(t, s, "KV_COUNTER")

	ctx, err := natscontext.New(context.Background(), kv, "session-counter")
	if err != nil {
		t.Fatal(err)
	}

	const counterKey natscontext.NatsContextValue = "counter"

	increment := func(old any, exists bool) (any, error) {
		if !exists {
			return float64(1), nil
		}
		return old.(float64) + 1, nil
	}

	const n = 20
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := natscontext.WithValueFunc(ctx, counterKey, increment)
			errs <- err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	val, _, found, err := natscontext.Load(ctx, counterKey)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected counter to be found")
	}
	if val != float64(n) {
		t.Fatalf("expected counter to be %d after %d concurrent increments, got %v", n, n, val)
	}
}

func TestWithValueOnNonDistributedContext(t *testing.T) {
	_, err := natscontext.WithValue(context.Background(), "k", "v")
	if err != natscontext.ErrNotDistributed {
		t.Fatalf("expected ErrNotDistributed, got %v", err)
	}
}

func TestValueFallsBackForNonNatsContextValueKeys(t *testing.T) {
	s := startServer(t)
	kv := newKV(t, s, "KV_FALLBACK")

	type plainKey struct{}

	parent := context.WithValue(context.Background(), plainKey{}, "parent-value")

	ctx, err := natscontext.New(parent, kv, "session-fallback")
	if err != nil {
		t.Fatal(err)
	}

	// plainKey{} is not a NatsContextValue, so Value must never consult the
	// KV bucket for it and should resolve it straight from the parent chain.
	if got := ctx.Value(plainKey{}); got != "parent-value" {
		t.Fatalf("expected parent-value, got %v", got)
	}

	// Even though natscontext's own key type can produce a matching string,
	// only an actual NatsContextValue is routed to the KV bucket.
	const distKey natscontext.NatsContextValue = "dist-key"
	if _, err := natscontext.WithValue(ctx, distKey, "kv-value"); err != nil {
		t.Fatal(err)
	}
	if got := ctx.Value(distKey); got != "kv-value" {
		t.Fatalf("expected kv-value, got %v", got)
	}
	if got := ctx.Value("dist-key"); got != nil {
		t.Fatalf("expected plain string key to bypass KV bucket and return nil, got %v", got)
	}
}

func TestWithValueSurvivesStandardComposition(t *testing.T) {
	s := startServer(t)
	kv := newKV(t, s, "KV_COMPOSE")

	ctx, err := natscontext.New(context.Background(), kv, "session-compose")
	if err != nil {
		t.Fatal(err)
	}

	// Derive via the standard library, not natscontext.
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	const k natscontext.NatsContextValue = "compose-key"

	if _, err := natscontext.WithValue(cancelCtx, k, "value-via-cancel-ctx"); err != nil {
		t.Fatal(err)
	}

	if got := cancelCtx.Value(k); got != "value-via-cancel-ctx" {
		t.Fatalf("got %v", got)
	}
}

func ExampleWithValue() {
	dir, _ := os.MkdirTemp("", "natscontext-example")
	defer os.RemoveAll(dir)

	s, _ := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dir})
	go s.Start()
	s.ReadyForConnections(10 * time.Second)
	defer s.Shutdown()

	nc, _ := nats.Connect(s.ClientURL())
	defer nc.Close()
	js, _ := jetstream.New(nc)
	kv, _ := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: "EXAMPLE"})

	root, _ := natscontext.New(context.Background(), kv, "example-session")

	const requestIDKey natscontext.NatsContextValue = "request-id"
	ctx, err := natscontext.WithValue(root, requestIDKey, "req-123")
	if err != nil {
		panic(err)
	}

	fmt.Println(ctx.Value(requestIDKey))
	// Output: req-123
}
