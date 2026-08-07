# Bombay Communication

Priority-aware, bounded communication channels for Rust.

`bombay-communication` combines an unbounded control lane with a bounded user
lane behind one consumer. Control traffic can bypass a user backlog, while an
optional aging cap guarantees that user traffic cannot starve. Each lane
preserves FIFO order, blocked user producers receive backpressure, and shutdown
returns queued values without losing ownership.

## Install

```toml
[dependencies]
bombay-communication = "0.1"
```

The package is named `bombay-communication` on crates.io and imported as
`communication` in Rust.

## Example

```rust
use communication::{channel, Config, Received};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let (control, user, mut receiver) =
    channel::<&'static str, String>(Config::new(1024).with_aging_cap(64));

control.send("shutdown")?;
user.send("work item".to_owned()).await?;

match receiver.recv().await {
    Some(Received::Control(signal)) => println!("control: {signal}"),
    Some(Received::User(message)) => println!("message: {message}"),
    Some(Received::UserLaneClosed) => println!("user lane closed"),
    None => println!("channel closed"),
}
# Ok(())
# }
```

## Guarantees

- Control receive latency is independent of user-queue depth.
- FIFO ordering is preserved within each lane.
- The configurable aging cap prevents user starvation.
- Values are delivered exactly once and parked consumers cannot miss wakeups.
- `Consumer::drain` returns remaining values from both lanes in FIFO order.
- The steady-state user-lane `try_send` path performs no allocations.
- `UserAnchor` provides a non-owning capability that does not keep a lane open.

There is intentionally no total order across lanes: a control value may
overtake an older user value.

## Development

The project uses the same pinned Rust/Nix development model as Nexus.

```bash
nix develop
cargo test --workspace --all-targets
nix flake check
```

The workspace retains the production crate, its shared conformance suite, a
reference implementation, and the performance harness. The support packages
are unpublished; only `bombay-communication` is released.

## License

Licensed under either the Apache License, Version 2.0 or the MIT License, at
your option.
