// Command leader-election is a minimal demo of using natscontext for
// distributed coordination: every node races to hold a single "leader"
// lease, renewing it while it holds the role and releasing it (or simply
// letting it expire) so another node can take over.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecmerdler/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const leaderKey natscontext.NatsContextValue = "leader"

// leaseDuration is how long a claimed leadership lease remains valid
// without being renewed. pollInterval is how often nodes attempt to
// renew/claim the lease; it must be comfortably shorter than leaseDuration
// so the current leader renews well before it would expire.
const (
	leaseDuration = 6 * time.Second
	pollInterval  = 2 * time.Second
)

// lease is the value stored under leaderKey. Expires is a Unix timestamp
// (seconds) after which the lease is considered abandoned and any node may
// claim it, whether or not the previous leader released it explicitly.
type lease struct {
	NodeID  string `json:"node_id"`
	Expires int64  `json:"expires"`
}

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID, _ = os.Hostname()
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	var nc *nats.Conn
	var err error
	for {
		nc, err = nats.Connect(
			natsURL,
			nats.Timeout(10*time.Second),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(time.Second),
		)
		if err == nil {
			break
		}
		log.Printf("[%s] connect: %v, retrying...", nodeID, err)
		time.Sleep(time.Second)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("[%s] jetstream: %v", nodeID, err)
	}

	ctx := context.Background()
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "leader-election",
	})
	if err != nil {
		log.Fatalf("[%s] create kv: %v", nodeID, err)
	}

	dctx, err := natscontext.New(ctx, kv, "election")
	if err != nil {
		log.Fatalf("[%s] new context: %v", nodeID, err)
	}

	log.Printf("[%s] started, racing for leadership", nodeID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			release(dctx, nodeID)
			log.Printf("[%s] shutting down, released leadership", nodeID)
			return
		case <-ticker.C:
			isLeader, err := campaign(dctx, nodeID)
			if err != nil {
				log.Printf("[%s] campaign failed: %v", nodeID, err)
				continue
			}
			if isLeader {
				log.Printf("[%s] I am the leader", nodeID)
				continue
			}
			leader, _, found, err := natscontext.Load(dctx, leaderKey)
			if err != nil {
				log.Printf("[%s] load leader failed: %v", nodeID, err)
				continue
			}
			if found {
				log.Printf("[%s] current leader is %v", nodeID, asLease(leader).NodeID)
			} else {
				log.Printf("[%s] no current leader", nodeID)
			}
		}
	}
}

// campaign attempts to claim or renew the leadership lease for nodeID. It
// returns true if nodeID holds the lease after the attempt. A node becomes
// (or remains) leader if no lease exists, the existing lease has expired, or
// it already holds the lease (in which case the lease is renewed).
func campaign(ctx *natscontext.Context, nodeID string) (bool, error) {
	now := time.Now()
	isLeader := false

	_, err := natscontext.WithValueFunc(ctx, leaderKey, func(old any, exists bool) (any, error) {
		current := asLease(old)
		if !exists || now.Unix() >= current.Expires || current.NodeID == nodeID {
			isLeader = true
			return lease{NodeID: nodeID, Expires: now.Add(leaseDuration).Unix()}, nil
		}
		return current, nil
	})
	if err != nil {
		return false, err
	}
	return isLeader, nil
}

// release abandons nodeID's lease immediately, if it currently holds one, so
// another node can take over without waiting for the lease to expire.
func release(ctx *natscontext.Context, nodeID string) {
	_, err := natscontext.WithValueFunc(ctx, leaderKey, func(old any, exists bool) (any, error) {
		current := asLease(old)
		if !exists || current.NodeID != nodeID {
			return current, nil
		}
		return lease{Expires: 0}, nil
	})
	if err != nil {
		log.Printf("[%s] release failed: %v", nodeID, err)
	}
}

// asLease converts the generic value returned by Context.Value/Load/
// WithValueFunc (decoded by JSONCodec into generic Go types) back into a
// lease. A nil or malformed value decodes to a zero-value lease, which is
// always treated as expired.
func asLease(v any) lease {
	var l lease
	data, err := json.Marshal(v)
	if err != nil {
		return l
	}
	_ = json.Unmarshal(data, &l)
	return l
}
