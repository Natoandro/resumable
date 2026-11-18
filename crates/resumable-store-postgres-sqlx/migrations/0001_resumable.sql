CREATE FUNCTION resumable_jsonb_object_length(value JSONB) RETURNS INTEGER
LANGUAGE SQL IMMUTABLE STRICT
AS $$
	SELECT count(*)::INTEGER
	FROM jsonb_object_keys(value)
$$;

CREATE FUNCTION resumable_now() RETURNS TIMESTAMPTZ
LANGUAGE SQL VOLATILE
AS $$
	SELECT clock_timestamp()
$$;

CREATE TABLE resumable_executions (
	id UUID PRIMARY KEY,
	operation_name TEXT NOT NULL CHECK (
		octet_length(operation_name) BETWEEN 1 AND 200
	),
	operation_version BIGINT NOT NULL CHECK (
		operation_version BETWEEN 0 AND 4294967295
	),
	input_codec TEXT,
	input_schema_version BIGINT,
	input_bytes BYTEA,
	result_codec TEXT,
	result_schema_version BIGINT,
	result_bytes BYTEA,
	metadata JSONB NOT NULL DEFAULT '{"labels": {}}'::jsonb,
	record JSONB NOT NULL,
	status TEXT NOT NULL CHECK (
		status IN ('pending', 'running', 'completed', 'failed', 'cancelled')
	),
	revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
	fencing_generation BIGINT NOT NULL DEFAULT 0 CHECK (fencing_generation >= 0),
	worker_id UUID,
	lease_token UUID,
	claim_id UUID,
	lease_duration_ms BIGINT,
	lease_expires_at TIMESTAMPTZ,
	failure_class TEXT CHECK (
		failure_class IN ('retryable', 'permanent', 'cancelled')
	),
	failure_diagnostic TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
	started_at TIMESTAMPTZ,
	completed_at TIMESTAMPTZ,
	cancellation_requested_at TIMESTAMPTZ,
	CHECK (
		(input_codec IS NULL AND input_schema_version IS NULL AND input_bytes IS NULL)
		OR (
			input_codec IS NOT NULL
			AND input_schema_version IS NOT NULL
			AND input_bytes IS NOT NULL
			AND octet_length(input_codec) BETWEEN 1 AND 64
			AND input_schema_version BETWEEN 0 AND 4294967295
			AND octet_length(input_bytes) <= 16777216
		)
	),
	CHECK (
		(result_codec IS NULL AND result_schema_version IS NULL AND result_bytes IS NULL)
		OR (
			result_codec IS NOT NULL
			AND result_schema_version IS NOT NULL
			AND result_bytes IS NOT NULL
			AND octet_length(result_codec) BETWEEN 1 AND 64
			AND result_schema_version BETWEEN 0 AND 4294967295
			AND octet_length(result_bytes) <= 16777216
		)
	),
	CHECK (octet_length(metadata::text) <= 16384),
	CHECK (
		(
			status = 'running'
			AND worker_id IS NOT NULL
			AND lease_token IS NOT NULL
			AND claim_id IS NOT NULL
			AND lease_duration_ms > 0
			AND lease_expires_at IS NOT NULL
		)
		OR (
			status <> 'running'
			AND worker_id IS NULL
			AND lease_token IS NULL
			AND claim_id IS NULL
			AND lease_duration_ms IS NULL
			AND lease_expires_at IS NULL
		)
	),
	CHECK (
		status NOT IN ('completed', 'failed', 'cancelled')
		OR completed_at IS NOT NULL
	),
	CHECK (record ->> 'id' = id::text),
	CHECK (record ->> 'status' = status),
	CHECK (record ->> 'revision' = revision::text),
	CHECK (record ->> 'fencing_generation' = fencing_generation::text),
	CHECK (failure_class IS NOT NULL OR failure_diagnostic IS NULL),
	CHECK (
		(status = 'failed' AND failure_class IS NOT NULL)
		OR (status = 'cancelled')
		OR (status NOT IN ('failed', 'cancelled') AND failure_class IS NULL)
	),
	CHECK (jsonb_typeof(metadata) = 'object'),
	CHECK (jsonb_typeof(metadata -> 'labels') = 'object'),
	CHECK (resumable_jsonb_object_length(metadata -> 'labels') <= 64)
);

CREATE INDEX resumable_executions_pending_idx
	ON resumable_executions (created_at, id)
	WHERE status = 'pending';

CREATE INDEX resumable_executions_expired_lease_idx
	ON resumable_executions (lease_expires_at, id)
	WHERE status = 'running';

CREATE TABLE resumable_steps (
	id UUID PRIMARY KEY,
	execution_id UUID NOT NULL REFERENCES resumable_executions(id) ON DELETE CASCADE,
	name TEXT COLLATE "C" NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 200),
	status TEXT NOT NULL CHECK (
		status IN ('pending', 'running', 'completed', 'failed', 'cancelled')
	),
	attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (
		attempt_count BETWEEN 0 AND 4294967295
	),
	active_attempt_generation BIGINT,
	result_codec TEXT,
	result_schema_version BIGINT,
	result_bytes BYTEA,
	retry_policy JSONB,
	record JSONB NOT NULL,
	failure_class TEXT CHECK (
		failure_class IN ('retryable', 'permanent', 'cancelled')
	),
	failure_diagnostic TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
	started_at TIMESTAMPTZ,
	completed_at TIMESTAMPTZ,
	next_attempt_at TIMESTAMPTZ,
	UNIQUE (execution_id, name),
	CHECK (
		(result_codec IS NULL AND result_schema_version IS NULL AND result_bytes IS NULL)
		OR (
			result_codec IS NOT NULL
			AND result_schema_version IS NOT NULL
			AND result_bytes IS NOT NULL
			AND octet_length(result_codec) BETWEEN 1 AND 64
			AND result_schema_version BETWEEN 0 AND 4294967295
			AND octet_length(result_bytes) <= 16777216
		)
	),
	CHECK (
		(status = 'running' AND active_attempt_generation IS NOT NULL)
		OR (status <> 'running' AND active_attempt_generation IS NULL)
	),
	CHECK ((status IN ('completed', 'failed', 'cancelled')) = (completed_at IS NOT NULL)),
	CHECK (record ->> 'id' = id::text),
	CHECK (record ->> 'execution_id' = execution_id::text),
	CHECK (record ->> 'name' = name),
	CHECK (record ->> 'status' = status),
	CHECK (record ->> 'attempt' = attempt_count::text),
	CHECK (failure_class IS NOT NULL OR failure_diagnostic IS NULL),
	CHECK (
		(status IN ('failed', 'cancelled') AND failure_class IS NOT NULL)
		OR (status NOT IN ('failed', 'cancelled') AND failure_class IS NULL)
	)
);

CREATE FUNCTION resumable_guard_attempt_count() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
	IF TG_OP = 'INSERT' AND NEW.attempt_count NOT IN (0, 1) THEN
		RAISE EXCEPTION 'new resumable steps may start with at most one attempt';
	END IF;
	IF TG_OP = 'UPDATE' AND (
		NEW.attempt_count < OLD.attempt_count
		OR NEW.attempt_count > OLD.attempt_count + 1
	) THEN
		RAISE EXCEPTION 'resumable attempt_count must stay the same or increment by one';
	END IF;
	RETURN NEW;
END;
$$;

CREATE TRIGGER resumable_steps_attempt_count_guard
BEFORE INSERT OR UPDATE ON resumable_steps
FOR EACH ROW EXECUTE FUNCTION resumable_guard_attempt_count();

CREATE INDEX resumable_steps_due_retry_idx
	ON resumable_steps (next_attempt_at, execution_id)
	WHERE status = 'failed' AND failure_class = 'retryable';

CREATE TABLE resumable_attempts (
	step_id UUID NOT NULL REFERENCES resumable_steps(id) ON DELETE CASCADE,
	attempt_number BIGINT NOT NULL CHECK (attempt_number BETWEEN 1 AND 4294967295),
	fencing_generation BIGINT NOT NULL CHECK (fencing_generation > 0),
	status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
	started_at TIMESTAMPTZ NOT NULL,
	finished_at TIMESTAMPTZ,
	failure_class TEXT CHECK (
		failure_class IN ('retryable', 'permanent', 'cancelled')
	),
	failure_diagnostic TEXT,
	PRIMARY KEY (step_id, attempt_number),
	CHECK ((status = 'running') = (finished_at IS NULL)),
	CHECK ((status = 'failed' OR status = 'cancelled') = (failure_class IS NOT NULL)),
	CHECK (failure_class IS NOT NULL OR failure_diagnostic IS NULL)
);

CREATE INDEX resumable_attempts_generation_idx
	ON resumable_attempts (step_id, fencing_generation, attempt_number DESC);

CREATE TABLE resumable_progress (
	step_id UUID PRIMARY KEY REFERENCES resumable_steps(id) ON DELETE CASCADE,
	current_value NUMERIC(20, 0) CHECK (current_value >= 0),
	total_value NUMERIC(20, 0) CHECK (total_value >= 0),
	message TEXT CHECK (octet_length(message) <= 1024),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE resumable_mutation_receipts (
	execution_id UUID NOT NULL REFERENCES resumable_executions(id) ON DELETE CASCADE,
	mutation_id UUID NOT NULL,
	mutation_kind TEXT NOT NULL CHECK (
		mutation_kind IN (
			'claim', 'begin_step', 'complete_step', 'fail_step',
			'complete_execution', 'fail_execution'
		)
	),
	request_fingerprint BYTEA NOT NULL CHECK (octet_length(request_fingerprint) = 32),
	outcome JSONB NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
	PRIMARY KEY (execution_id, mutation_id)
);
