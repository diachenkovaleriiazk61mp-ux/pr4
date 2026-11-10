CREATE TABLE IF NOT EXISTS vaccinations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_code VARCHAR(200) NOT NULL CHECK (length(trim(patient_code)) > 0),
    vaccine VARCHAR(200) NOT NULL CHECK (length(trim(vaccine)) > 0),
    batch VARCHAR(200) NOT NULL CHECK (length(trim(batch)) > 0),
    dose_number SMALLINT NOT NULL CHECK (dose_number BETWEEN 1 AND 4),
    administration_date DATE NOT NULL CHECK (administration_date <= CURRENT_DATE),
    facility VARCHAR(200) NOT NULL CHECK (length(trim(facility)) > 0)
);
CREATE INDEX IF NOT EXISTS vaccinations_vaccine_id_idx ON vaccinations(vaccine, id);

CREATE TABLE IF NOT EXISTS tasks (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    record_id BIGINT NOT NULL REFERENCES vaccinations(id),
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','done','failed')),
    seed BIGINT NOT NULL CHECK (seed BETWEEN 0 AND 2147483647),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    token BIGINT NOT NULL DEFAULT 0,
    lease_until TIMESTAMPTZ,
    worker_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    run_after TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS tasks_status_created_idx ON tasks(status, created_at);
CREATE TABLE IF NOT EXISTS task_results (
    task_id BIGINT PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    token BIGINT NOT NULL,
    worker_id TEXT NOT NULL,
    result JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS task_events (
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    token BIGINT NOT NULL,
    worker_id TEXT,
    kind TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
