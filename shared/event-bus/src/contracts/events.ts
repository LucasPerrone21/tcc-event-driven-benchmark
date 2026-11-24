export const TOPICS = {
  TELEMETRY_RAW: 'telemetry.raw',
  COMMANDS_REQUESTED: 'commands.requested',
  COMMANDS_STATUS: 'commands.status',
} as const;

export interface TelemetryTimestamps {
  sensor_created_at: string;
  ingested_at: string;
  persisted_at?: string;
}

export interface TelemetryRawEvent {
  event_id: string;
  sensor_id: string;
  type: string;
  value: number;
  unit: string;
  timestamps: TelemetryTimestamps;
}

export interface CommandRequestedEvent {
  command_id: string;
  actuator_id: string;
  action: string;
  reason: string;
  triggered_by_event_id: string;
  created_at: string;
}
