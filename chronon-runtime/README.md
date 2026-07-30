# chronon-runtime

`Chronon`, `ChrononBuilder`, `CoordinatorService`, and runtime loop assembly — choose deployment shape via `.embedded()`, `.coordinator_only()`, `.worker()`, or `.remote_coordinator()`; scheduler + executor wiring and event persistence live here.

## Deployment shapes

Choose topology with builder methods:

| Method | Shape |
|--------|-------|
| `.embedded()` | Tick + execute in one process |
| `.coordinator_only()` | Tick + enqueue only |
| `.worker(pool_id)` | Claim + execute only |
| `.remote_coordinator(url)` | HTTP client shell |

## Documentation

```bash
cargo doc -p chronon-runtime --no-deps --open
```
