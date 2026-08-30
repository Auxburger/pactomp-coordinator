# PactOMP Coordinator

A dynamic resource manager (DRM) for co-scheduling parallel OpenMP-style
applications on a single machine.

Multiple processes running on the same node each assume they own the whole
machine, so each spawns as many threads as there are cores. The result is
oversubscription: more runnable threads than CPUs, context-switch churn, and
cache thrash. This daemon acts as a central arbiter — clients ask how many
threads they may use before forming a team, and the coordinator answers with a
thread count plus an optional exclusive CPU range to pin to.

It is a single binary with **no external dependencies** — pure `std`, Rust
edition 2024.

## Related repositories

- **Client counterpart** — [Auxburger/llvm-project @ `feature/PO-1-implement-llvm-pactomp-connection`](https://github.com/Auxburger/llvm-project/tree/feature/PO-1-implement-llvm-pactomp-connection):
  an LLVM fork whose OpenMP runtime speaks the protocol described below,
  querying the coordinator before forming a thread team and applying the
  returned thread count and CPU affinity. This is the reference implementation
  of the other end of the socket.
- **Evaluation artifacts** — [Auxburger/pactomp-benchmarks](https://github.com/Auxburger/pactomp-benchmarks):
  the benchmark harness, retained measurement data, experiment scripts, and
  analysis and figure-export pipeline used to evaluate the coordinator.
- **Thesis** — [Auxburger/master-thesis](https://github.com/Auxburger/master-thesis):
  the write-up motivating this design and interpreting the measurements.

## How it works

The coordinator listens on a Unix domain socket at `/tmp/omp-rm.sock`
(hard-coded, mode `0666` so unprivileged clients can connect). Each accepted
connection is handled by its own thread, which serves an unbounded sequence of
request/reply pairs over the same stream until the peer closes it.

For every request the coordinator:

1. Upserts the client's record in shared state, refreshing its `last_seen`.
2. Evicts clients not seen for 5 s (crash safety — a client that dies without
   closing its socket cleanly stops counting against everyone else's share).
3. Asks the scheduling strategy for a thread count.
4. Assigns (or reuses) a contiguous block of CPUs of that size from the pool.
5. Writes the reply.

On clean disconnect the client's state is removed immediately.

### Wire protocol

Fixed-size binary frames in **native byte order** — the coordinator is
intended to run on the same host as its clients, so no endianness conversion
is performed.

**Request — 16 bytes, of which 8 are currently used:**

| Offset | Type  | Field           | Meaning                                    |
|-------:|-------|-----------------|--------------------------------------------|
|    0–3 | `u32` | `pid`           | Client identity; the state key             |
|    4–5 | `u16` | `max_threads`   | Hard upper bound the grant is clamped to   |
|    6–7 | `u16` | `hint_threads`  | Desired count (recorded, see notes)        |
|   8–15 | —     | padding         | Reserved; ignored                          |

Clients must always send all 16 bytes — the read is a `read_exact` of the full
frame.

**Reply — 12 bytes:**

| Offset | Type  | Field      | Meaning                                          |
|-------:|-------|------------|--------------------------------------------------|
|    0–1 | `u16` | `granted`  | Thread count the client may use                  |
|    2–3 | `u16` | `ttl_ms`   | Validity of the grant in ms                      |
|    4–7 | `u32` | `epoch`    | Monotonic counter (informational)                |
|    8–9 | `u16` | `base_cpu` | First CPU of the assigned range, `0` if unpinned |
|  10–11 | `u16` | `num_cpus` | Length of the assigned range, `0` if unpinned    |

The client is expected to interpret the CPU grant as the contiguous range
`[base_cpu, base_cpu + num_cpus)` and call `sched_setaffinity(0, …)` on the
master thread before forking its team. `num_cpus == 0` means "do not pin".

### Scheduling

Strategy selection follows the GoF strategy pattern: `Scheduler` is the
context, `SchedulingStrategy` the interface, and swapping policy is a matter of
implementing the trait and calling `Scheduler::set_strategy`.

The only strategy shipped today is **`fair_share`** ([src/scheduler/fair_share.rs](src/scheduler/fair_share.rs)).
Given `capacity` cores and `n` clients seen within the 5 s active window, it
reserves one core per client and splits the rest evenly:

```
granted = min(1 + (capacity - n) / n,  max_threads)     when capacity > n
granted = 1                                             otherwise
```

The grant is deliberately *not* raised back toward `hint_threads` — doing so
would defeat fair sharing. `max_threads` only ever caps it.

### CPU assignment

When a CPU pool is configured, [src/state.rs](src/state.rs) hands out
contiguous blocks by **first fit** over the pool, skipping index ranges already
held by other live clients. A client that asks again and would get the same
size keeps its existing block, so affinity is stable across the repeated
requests of a benchmark run. If the size changes, the old block is released and
a new one searched. When the pool is full — or no pool is configured — the
reply carries `(0, 0)` and pinning is left to the client.

## Building and running

```bash
cargo build --release
./target/release/pactomp-coordinator
```

Or directly:

```bash
POMP_CAPACITY=16 POMP_CPU_LIST=0-15 cargo run --release
```

The socket is unlinked at startup if it already exists, so a stale file from a
previous crash does not block a restart.

## Configuration

Both knobs are read once at startup from the environment.

| Variable       | Default                       | Meaning |
|----------------|-------------------------------|---------|
| `POMP_CAPACITY` | `available_parallelism()`     | Total threads the coordinator hands out across all clients. |
| `POMP_CPU_LIST` | unset → **pinning disabled**  | Ordered pool of CPUs available for assignment. |

`POMP_CPU_LIST` accepts comma-separated integers and inclusive ranges, e.g.
`0,1,2-7,10-15`. The list is sorted and deduplicated. Unparsable entries are
tolerated rather than rejected. Note that capacity and the pool size are
independent — set them consistently (typically `POMP_CAPACITY` equal to the
number of CPUs in `POMP_CPU_LIST`) or the scheduler will grant blocks the pool
cannot satisfy.

Useful when the coordinator should own only part of a machine — e.g. one NUMA
node, or all cores except those reserved for the OS.

## Layout

| File | Contents |
|------|----------|
| [src/main.rs](src/main.rs) | Startup: env parsing, socket cleanup, wiring |
| [src/protocol.rs](src/protocol.rs) | Frame encode/decode, `POMP_CPU_LIST` parser |
| [src/server.rs](src/server.rs) | Unix socket accept loop, per-connection request loop |
| [src/state.rs](src/state.rs) | Client registry, staleness eviction, CPU block allocator |
| [src/scheduler/mod.rs](src/scheduler/mod.rs) | `Scheduler` context and `SchedulingStrategy` trait |
| [src/scheduler/fair_share.rs](src/scheduler/fair_share.rs) | Equal-split policy |

## Current limitations

The daemon is a research prototype; the following are known and intentional
for now:

- `ttl_ms` is a constant `10` and `epoch` a constant `1` in every reply —
  neither is yet derived from scheduler state, so clients cannot rely on the
  epoch to detect a re-decision.
- The 5 s active window and staleness timeout are hard-coded, as is the socket
  path.
- Existing grants are never revoked. A client that arrives while the pool is
  full is told to run unpinned rather than triggering a redistribution.
- `hint_threads` is recorded in client state but does not influence
  `fair_share`'s decision.
- There are no tests, and every operation serializes on a single state mutex —
  fine at the request rates a handful of OpenMP programs generate.

## Platform

Linux (and other Unix hosts with `UnixListener`). CPU pinning is meaningful
only where the client can call `sched_setaffinity`.

## License

Copyright 2026 Darius Augsburger.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this software except in compliance with the License. A copy of the full
license text is in [LICENSE](LICENSE), and you may also obtain it at
<http://www.apache.org/licenses/LICENSE-2.0>.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an **"AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND**, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

In short, and with the License text itself being authoritative: you may use,
modify, and redistribute this code, including commercially, provided you
retain the copyright and license notices, state significant changes you made,
and include the [NOTICE](NOTICE) file in redistributions. The License also
grants you a patent license from each contributor, and terminates that grant
if you initiate patent litigation over the work.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this work shall be licensed as above, without any
additional terms or conditions.
