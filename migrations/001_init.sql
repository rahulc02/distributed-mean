-- 001_init: the whole schema from the design doc.
-- Ownership: the API writes jobs (creation, generation status), config and events;
-- the coordinator writes tasks, operands and job progress; the supervisor writes workers.

-- ---------------------------------------------------------------- jobs
CREATE TABLE jobs (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    f                integer     NOT NULL CHECK (f > 1 AND f < 100000),    -- files
    c                integer     NOT NULL CHECK (c > 1 AND c < 1000000),   -- values per file
    seed             bigint      NOT NULL,                                 -- generation seed
    status           text        NOT NULL DEFAULT 'GENERATING'
                     CHECK (status IN ('GENERATING', 'NEW', 'RUNNING', 'DONE', 'FAILED')),

    -- Fan-in is fixed per job when it starts, so changing the config never
    -- changes the plan of a job already running.
    k_leaf           integer     CHECK (k_leaf >= 2),
    k_merge          integer     CHECK (k_merge >= 2),
    leaves_published boolean     NOT NULL DEFAULT false,

    -- Display-only progress: totals are exact (L + M tasks, F + L + M - 1 operands).
    total_tasks      integer,
    tasks_done       integer     NOT NULL DEFAULT 0,
    total_operands   bigint,
    operands_done    bigint      NOT NULL DEFAULT 0,

    result_key       text,
    error            text,
    cleaned_up       boolean     NOT NULL DEFAULT false,

    created_at       timestamptz NOT NULL DEFAULT now(),
    gen_started_at   timestamptz,
    gen_finished_at  timestamptz,
    started_at       timestamptz,
    finished_at      timestamptz
);

CREATE INDEX jobs_by_status ON jobs (status);

-- ---------------------------------------------------------------- tasks
-- Merge tasks get a row when created. Leaf tasks are virtual (id 'J:1:g',
-- inputs derived from g) and get a row only when their first completion arrives.
CREATE TABLE tasks (
    id             text        PRIMARY KEY,
    job_id         uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    level          integer     NOT NULL CHECK (level >= 1),     -- 1 = sums files
    inputs         jsonb,                                       -- merges: [{id, key, count}]
    input_count    integer,                                     -- sum of input counts
    final          boolean     NOT NULL DEFAULT false,          -- input counts sum to F
    output_key     text,
    status         text        NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'DONE', 'FAILED')),
    published_at   timestamptz,                                 -- NULL = committed, not yet queued
    republished_at timestamptz,
    worker_id      text,
    duration_ms    integer,
    created_at     timestamptz NOT NULL DEFAULT now(),
    completed_at   timestamptz
);

CREATE INDEX tasks_by_job ON tasks (job_id);
-- The startup and periodic scans look for committed-but-unpublished tasks.
CREATE INDEX tasks_unpublished ON tasks (created_at)
    WHERE status = 'PENDING' AND published_at IS NULL;

-- ---------------------------------------------------------------- operands
-- Partials only: input files are implicit from jobs.f and never wait in the pool.
CREATE TABLE operands (
    id          text    PRIMARY KEY REFERENCES tasks (id) ON DELETE CASCADE,  -- = producing task
    job_id      uuid    NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    key         text    NOT NULL,
    count       integer NOT NULL CHECK (count > 0),
    level       integer NOT NULL CHECK (level >= 1),
    status      text    NOT NULL DEFAULT 'READY' CHECK (status IN ('READY', 'CONSUMED')),
    consumed_by text    REFERENCES tasks (id) ON DELETE CASCADE,
    CHECK ((status = 'READY') = (consumed_by IS NULL))
);

-- The ready pool: at most a handful of rows per job between completions.
CREATE INDEX ready_pool ON operands (job_id, level DESC, id)
    WHERE status = 'READY';

-- ---------------------------------------------------------------- config
-- One row holding system-wide desired state.
CREATE TABLE config (
    id              boolean PRIMARY KEY DEFAULT true CHECK (id),
    desired_workers integer NOT NULL DEFAULT 4 CHECK (desired_workers >= 0)
);

INSERT INTO config DEFAULT VALUES;

-- ---------------------------------------------------------------- workers
-- Written only by the supervisor; workers themselves never touch Postgres.
CREATE TABLE workers (
    id           text        PRIMARY KEY,                       -- also the RabbitMQ consumer tag
    pid          integer,
    host         text,
    status       text        NOT NULL
                 CHECK (status IN ('STARTING', 'RUNNING', 'DRAINING', 'EXITED')),
    busy         boolean     NOT NULL DEFAULT false,
    started_at   timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    exited_at    timestamptz,
    exit_code    integer
);

-- ---------------------------------------------------------------- events
-- Lifecycle moments only, for the dashboard's event log. No foreign key, so
-- the log survives job cleanup.
CREATE TABLE events (
    id      bigserial   PRIMARY KEY,
    at      timestamptz NOT NULL DEFAULT now(),
    source  text        NOT NULL
            CHECK (source IN ('api', 'generator', 'coordinator', 'supervisor')),
    job_id  uuid,
    message text        NOT NULL
);

CREATE INDEX events_by_time ON events (at);
