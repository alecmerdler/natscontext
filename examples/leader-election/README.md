# Leader election example

Three app containers race to hold a leadership lease through a
`natscontext.Context` backed by a NATS JetStream KV bucket. Leadership is a
time-limited lease (`leaseDuration`): the current leader renews it on every
poll, and if it stops renewing — because it crashed or was stopped — the
lease expires and any other node can claim it on its next poll. A node that
shuts down gracefully (SIGINT/SIGTERM) also releases the lease immediately
so a new leader can take over without waiting for it to expire.

All coordination goes through natscontext's public API
(`WithValueFunc`/`Load`) rather than the underlying NATS KV client directly,
so the storage backend could be swapped for a different natscontext
implementation without changing the election logic.

Run it:

```sh
docker compose up --build
```

Each node logs whether it is the leader every 3 seconds.
