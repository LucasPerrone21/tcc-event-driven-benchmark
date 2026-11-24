CREATE TABLE IF NOT EXISTS sensors (
  sensor_id VARCHAR(128) PRIMARY KEY,
  type VARCHAR(64) NOT NULL,
  unit VARCHAR(32) NOT NULL,
  last_value DOUBLE PRECISION,
  last_seen_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS actuators (
  actuator_id VARCHAR(128) PRIMARY KEY,
  type VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'IDLE',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alert_rules (
  id SERIAL PRIMARY KEY,
  sensor_type VARCHAR(64) NOT NULL,
  operator VARCHAR(8) NOT NULL,
  threshold DOUBLE PRECISION NOT NULL,
  actuator_id VARCHAR(128) NOT NULL,
  action VARCHAR(64) NOT NULL,
  cooldown_seconds INTEGER NOT NULL DEFAULT 30,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS command_logs (
  command_id UUID PRIMARY KEY,
  actuator_id VARCHAR(128) NOT NULL,
  action VARCHAR(64) NOT NULL,
  reason VARCHAR(255) NOT NULL,
  triggered_by_event_id UUID,
  status VARCHAR(32) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO alert_rules (sensor_type, operator, threshold, actuator_id, action, cooldown_seconds, enabled)
SELECT 'temperature', '>', 30.0, 'actuator-cooling-01', 'TURN_ON', 30, TRUE
WHERE NOT EXISTS (SELECT 1 FROM alert_rules);
