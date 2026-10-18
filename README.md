# distributed-mean

Computes the per-index mean across F files of C random values each, on a fleet
of W workers that run at different speeds and can each handle at most 5 files at
a time. Many jobs run at once, workers can crash, and W can change mid-job.

**The core idea:** summing input files and summing partial results are the same
operation. An *operand* is a sum vector plus a count (an input file is an
operand with count 1). Every task adds up to K operands into one, partials merge
continuously as they appear, and the final task is simply the one whose counts
reach F. There is no separate reducer.

The full design, including the alternatives considered and why each was
rejected, is in [docs/design.pdf](docs/design.pdf).

## Quick start

Requires Docker with Compose v2.

```bash
cp .env.example .env
docker compose up -d --build
open http://localhost:3000          # the dashboard
```

Start a job and fetch its result:

```bash
curl -s -X POST localhost:3000/jobs -H 'Content-Type: application/json' -d '{"f": 100, "c": 1000}'
# {"id":"9c41e2aa-..."}
curl -s localhost:3000/jobs/9c41e2aa-...            # status and exact progress
curl -s localhost:3000/jobs/9c41e2aa-.../result     # one CSV line of C means, once DONE
```

Change the number of workers at any time, including mid-job:

```bash
curl -s -X PUT localhost:3000/workers -H 'Content-Type: application/json' -d '{"count": 8}'
curl -s localhost:3000/workers
```

To see the dashboard without the stack, open `ui/index.html?demo` through any
static server.

## How it fits together

| Component | Language | Responsibility |
| --- | --- | --- |
| `api` | TypeScript (Express 5) | jobs, input generation, results, `PUT /workers`, SSE dashboard |
| `coordinator` | Python | the only writer of task and operand state: groups operands into tasks, publishes them, finishes jobs, recovers after crashes |
| `supervisor` | Python | keeps W worker processes running; drains them on scale-in |
| workers | Python | pure compute: read up to K operands, add, write one; never touch the database |
| Postgres | | source of truth: jobs, tasks, the ready pool of partials, workers, events |
| RabbitMQ | | transport only: `tasks`, `completions`, `tasks.dlq` (quorum queues) |
| `./data` | | operand bytes: `files/`, `partials/`, `results/` |

A job moves `GENERATING → NEW → RUNNING → DONE`. The API generates the files
and flips the job to NEW; the coordinator publishes one leaf task per K files;
each completion inserts its output as a READY partial and, as soon as K are
ready, a merge task is created. When the remaining partials' counts sum to F,
the final task writes the result.

**Why it is exactly-once:** messages and object writes may repeat, but database
transitions cannot. Leaf tasks have deterministic IDs (`ON CONFLICT DO NOTHING`),
merges complete with `WHERE status = 'PENDING'`, an operand is consumed with
`WHERE status = 'READY'` under the job's row lock, and every task's output key
and bytes are deterministic, so a duplicate execution overwrites identical bytes
and its completion is ignored.

## Configuration

Every service reads its settings from environment variables (see
`docker-compose.yml` and `.env.example`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `K_LEAF` | 5 | files per leaf task; 5 matches "5 files at a time" |
| `K_MERGE` | 5 | partials per merge task |
| `MAX_WORKERS` | 32 | upper bound for `PUT /workers` |
| `DRAIN_GRACE_S` | 120 | seconds a draining worker gets to finish its task before it is killed; also Compose's stop grace period for the supervisor |
| `RABBITMQ_CONSUMER_TIMEOUT_MS` | 600000 | a worker holding a task longer than this (hung but connected) has its channel closed and the task redelivered |
| `GEN_THREADS` | 4 | worker threads the API uses to generate files |
| `GEN_RANGE_SIZE` | 100 | files per generation range |

Workers stream any number of operands with at most 5 buffers in memory, so
raising `K_LEAF` (for example to 1000) cuts partial I/O and coordinator load by
orders of magnitude if the limit is about memory rather than files per task.

## Tests

| Suite | Where it runs | What it proves |
| --- | --- | --- |
| `py/tests/coordinator` | anywhere | grouping, planning and the service logic, plus a **pipeline simulator**: 300 randomized runs in which tasks and completions are reordered and duplicated, workers crash, and the coordinator crashes between commit and publish; every run must end with the exact mean and exactly L + M tasks |
| `py/tests/worker`, `py/tests/supervisor` | anywhere | adders (memory bound, bit-identical reruns), executor, reconcile decisions, real process spawn/drain/kill |
| `py/tests/test_*.py`, `api/test` | anywhere | both codecs and the seeded generator agree byte for byte through shared fixtures |
| `py/tests/integration` | needs Postgres | the coordinator against the real schema; two coordinators racing to start one job |
| `py/tests/e2e` | needs the stack | the F = 12 example returns [6.5, 65]; generated jobs match an independent computation; concurrent jobs; killing a worker, restarting the coordinator and changing W mid-job leave the answer unchanged |

```bash
make test           # unit tests (Python and TypeScript)
make integration    # needs `docker compose up -d postgres` and migrations
make e2e            # needs `docker compose up -d --build`
```

## Layout

```text
docker-compose.yml     the whole system
migrations/            numbered SQL migrations and the runner that applies each once
contracts/             operand byte format, message schema, cross-language fixtures
docs/design.pdf        the design document
api/                   TypeScript API, generator and dashboard backend
ui/                    the dashboard (static HTML, CSS, JavaScript; no build step)
py/dmean/common/       codec, messages, storage, topology, logging, seeded RNG
py/dmean/coordinator/  pure core (grouping, planning) + service + Postgres/RabbitMQ adapters
py/dmean/worker/       adders, executor, consumer loop
py/dmean/supervisor/   pure reconcile + process table + loop
```

## Not built (documented in [the design doc](docs/design.pdf))

- Retention: partials and inputs are not deleted when a job finishes.
- The backup-task speculator for slow workers at the tail (off by default at K = 5).
- Generation option 2, a separate generator fleet (`generateRange` is the seam).
- Sharding by `job_id` for coordinators, RabbitMQ and Postgres (the scale-out path).
