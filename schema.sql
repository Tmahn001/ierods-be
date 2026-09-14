DROP TABLE IF EXISTS session_events, metrics_snapshots, recovery_plans,
  failure_events, heartbeat_log, batches, students, exam_sessions, workstations CASCADE;

-- ── Workstation inventory ────────────────────────────────────────────────
CREATE TABLE workstations (
  station_id        VARCHAR(30) PRIMARY KEY,      -- "ICT-A-R03-S12"
  hall_id           VARCHAR(15) NOT NULL,          -- "ICT-A"
  row_number        INT NOT NULL,
  seat_number       INT NOT NULL,
  ups_circuit_id    VARCHAR(20) NOT NULL,          -- for circuit-failure detection
  processor_score   FLOAT DEFAULT 1.0,             -- 0.0-1.0 hardware quality
  status            VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE',
  last_heartbeat    TIMESTAMPTZ,
  -- cumulative counters, used for the Chapter 3 utilisation formula
  cumulative_occupied_sec  BIGINT DEFAULT 0,
  cumulative_available_sec BIGINT DEFAULT 0,
  CONSTRAINT valid_status CHECK (status IN
    ('AVAILABLE','OCCUPIED','FAILED','DEGRADED','OFFLINE'))
);

-- ── Exam sessions (one per course per day) ───────────────────────────────
CREATE TABLE exam_sessions (
  session_id        SERIAL PRIMARY KEY,
  course_code       VARCHAR(15) NOT NULL,
  exam_date         DATE NOT NULL,
  total_enrolled    INT NOT NULL,
  exam_duration_min INT NOT NULL,
  expected_failure_rate FLOAT NOT NULL DEFAULT 0.11,   -- PROVISIONAL, see Section 9 note
  min_processor_score FLOAT NOT NULL DEFAULT 0.0,      -- hardware requirement (Ch.3 Hard Constraint 3 / COMPAT term)
  pv_alpha          FLOAT NOT NULL DEFAULT 0.5,        -- Priority Value weighting, Ch.3 Eq. 3.9 (wait-time term)
  pv_beta           FLOAT NOT NULL DEFAULT 0.3,        -- Priority Value weighting (shortest-job-first term)
  pv_gamma          FLOAT NOT NULL DEFAULT 0.2,        -- Priority Value weighting (compatibility term)
  hall_ids          VARCHAR(15)[] NOT NULL,
  started_at        TIMESTAMPTZ,
  ended_at          TIMESTAMPTZ,
  first_admission_at TIMESTAMPTZ,     -- set on first login, needed for makespan
  last_submission_at TIMESTAMPTZ,     -- updated on each submission
  is_active         BOOLEAN DEFAULT FALSE
);

-- Only one session may be active at a time
CREATE UNIQUE INDEX one_active_session ON exam_sessions (is_active) WHERE is_active = TRUE;

-- ── Students ─────────────────────────────────────────────────────────────
CREATE TABLE students (
  matric_number     VARCHAR(30) PRIMARY KEY,
  session_id        INT REFERENCES exam_sessions(session_id) ON DELETE CASCADE,
  course_code       VARCHAR(15) NOT NULL,
  status            VARCHAR(20) NOT NULL DEFAULT 'NOT_ADMITTED',
  current_station   VARCHAR(30) REFERENCES workstations(station_id),
  queued_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- WT(s,t) proxy: time roster record was loaded, NOT physical queue join time (no sensor exists, see Section 0)
  session_start     TIMESTAMPTZ,
  submission_time   TIMESTAMPTZ,
  disruption_count  INT DEFAULT 0,
  CONSTRAINT valid_student_status CHECK (status IN
    ('NOT_ADMITTED','IN_SESSION','COMPLETED','DISRUPTED','RESIT_REQUIRED'))
);
CREATE INDEX idx_students_session_status ON students(session_id, status);
CREATE INDEX idx_students_station ON students(current_station) WHERE current_station IS NOT NULL;

-- ── Batches (admission records) ──────────────────────────────────────────
CREATE TABLE batches (
  batch_id          SERIAL PRIMARY KEY,
  session_id        INT REFERENCES exam_sessions(session_id) ON DELETE CASCADE,
  batch_number      INT NOT NULL,
  announced_size    INT NOT NULL,          -- how many the admin was told to call in
  admitted_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  free_stations_at_admission INT NOT NULL,
  idle_gap_sec      INT DEFAULT 0,         -- Dead Air before this batch
  admitted_by       VARCHAR(60)
);

-- ── Heartbeat log (append-only) ──────────────────────────────────────────
CREATE TABLE heartbeat_log (
  id                BIGSERIAL PRIMARY KEY,
  station_id        VARCHAR(30) REFERENCES workstations(station_id),
  recorded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status_code       VARCHAR(20) NOT NULL,   -- OK | SLOW | TIMEOUT
  response_ms       INT
);
CREATE INDEX idx_heartbeat_recent ON heartbeat_log(station_id, recorded_at DESC);

-- ── Failure events ───────────────────────────────────────────────────────
CREATE TABLE failure_events (
  event_id          SERIAL PRIMARY KEY,
  session_id        INT REFERENCES exam_sessions(session_id) ON DELETE CASCADE,
  station_ids       VARCHAR(30)[] NOT NULL,
  failure_type      VARCHAR(20) NOT NULL,   -- SINGLE | CIRCUIT
  detected_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at       TIMESTAMPTZ,
  affected_matrics  VARCHAR(30)[] NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_failures_open ON failure_events(session_id) WHERE resolved_at IS NULL;

-- ── Recovery plans ───────────────────────────────────────────────────────
CREATE TABLE recovery_plans (
  plan_id           SERIAL PRIMARY KEY,
  event_id          INT UNIQUE REFERENCES failure_events(event_id) ON DELETE CASCADE,
  assignments       JSONB NOT NULL DEFAULT '[]',
  resit_flags       JSONB NOT NULL DEFAULT '[]',
  reserved_stations VARCHAR(30)[] NOT NULL DEFAULT '{}',  -- soft-locked seats
  confidence_score  FLOAT NOT NULL,
  estimated_disruption_min INT NOT NULL,
  generated_at      TIMESTAMPTZ DEFAULT NOW(),
  approved_at       TIMESTAMPTZ,
  approved_by       VARCHAR(60)
);

-- ── Metrics snapshots (time series for charts) ───────────────────────────
CREATE TABLE metrics_snapshots (
  id                BIGSERIAL PRIMARY KEY,
  session_id        INT REFERENCES exam_sessions(session_id) ON DELETE CASCADE,
  recorded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  throughput_rate   FLOAT,
  utilisation_ratio FLOAT,
  failure_impact_factor FLOAT,   -- NULL when no failures yet
  queue_depth       INT,
  students_in_session INT,
  students_completed  INT,
  stations_occupied   INT,
  stations_available  INT,
  stations_failed     INT
);
CREATE INDEX idx_metrics_series ON metrics_snapshots(session_id, recorded_at DESC);

-- ── Human-readable audit trail ───────────────────────────────────────────
CREATE TABLE session_events (
  id                BIGSERIAL PRIMARY KEY,
  session_id        INT REFERENCES exam_sessions(session_id) ON DELETE CASCADE,
  event_time        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  level             VARCHAR(10) NOT NULL DEFAULT 'INFO',
  message           TEXT NOT NULL
);
CREATE INDEX idx_events_recent ON session_events(session_id, event_time DESC);
