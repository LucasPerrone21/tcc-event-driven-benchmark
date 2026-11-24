import { InfluxDB, Point, WriteApi } from '@influxdata/influxdb-client';
import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { TelemetryRawEvent } from '@tcc/event-bus';

export interface PersistedTelemetryItem {
  event: TelemetryRawEvent;
  persistedAt: string;
}

@Injectable()
export class InfluxRepository implements OnModuleDestroy {
  private readonly logger = new Logger(InfluxRepository.name);
  private readonly writeApi: WriteApi;

  constructor() {
    const url = process.env.INFLUXDB_URL ?? 'http://localhost:8086';
    const token = process.env.INFLUXDB_TOKEN ?? 'tcc-super-secret-token';
    const org = process.env.INFLUXDB_ORG ?? 'tcc-ufba';
    const bucket = process.env.INFLUXDB_BUCKET ?? 'telemetry';

    const client = new InfluxDB({ url, token });
    this.writeApi = client.getWriteApi(org, bucket, 'ns');
  }

  async writeBatch(items: PersistedTelemetryItem[]): Promise<void> {
    if (items.length === 0) {
      return;
    }

    for (const { event, persistedAt } of items) {
      const createdMs = Date.parse(event.timestamps.sensor_created_at);
      const persistedMs = Date.parse(persistedAt);
      const e2eLatencyMs =
        Number.isFinite(createdMs) && Number.isFinite(persistedMs)
          ? Math.max(0, persistedMs - createdMs)
          : 0;

      const point = new Point('telemetry')
        .tag('sensor_id', event.sensor_id)
        .tag('type', event.type)
        .tag('unit', event.unit)
        .floatField('value', event.value)
        .stringField('event_id', event.event_id)
        .stringField('sensor_created_at', event.timestamps.sensor_created_at)
        .stringField('ingested_at', event.timestamps.ingested_at)
        .stringField('persisted_at', persistedAt)
        .floatField('e2e_latency_ms', e2eLatencyMs)
        .timestamp(new Date(persistedMs));

      this.writeApi.writePoint(point);
    }

    await this.writeApi.flush();
  }

  async onModuleDestroy(): Promise<void> {
    try {
      await this.writeApi.close();
    } catch (err) {
      this.logger.warn(
        `Error closing InfluxDB WriteApi: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
}
