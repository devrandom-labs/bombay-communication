# Codebase Map

| Directory | Purpose |
|---|---|
| `crates/communication` | Published `bombay-communication` library (import name `communication`) — two-lane priority channel; criterion bench, loom model-check, property/conformance test suites |
| `crates/communication-reference` | Unpublished reference implementation (import name `communication_reference`) used as the semantic oracle for the property suite |
| `crates/communication-testkit` | Unpublished shared conformance/property test suite consumed by both implementations |
| `crates/communication-perf` | Unpublished perf-harness binary `bombay-communication-perf` — card-3 contract metric lines |
| `docs` | GitHub Pages landing page redirect to the API docs |
| `.github/workflows` | CI — Nix flake check, Pages doc deploy, release-plz, release-branch pruning, crate reservation |
