import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { TelemetryRawEvent } from '@tcc/event-bus';
import { Pool } from 'pg';

@Injectable()
export class PostgresRepository implements OnModuleDestroy {
  private readonly pool: Pool;

  constructor() {
    this.pool = new Pool({
      host: process.env.POSTGRES_HOST ?? 'localhost',
      port: Number(process.env.POSTGRES_PORT ?? 5432),
      user: process.env.POSTGRES_USER ?? 'tcc',
      password: process.env.POSTGRES_PASSWORD ?? 'tcc',
      database: process.env.POSTGRES_DB ?? 'tcc_monitoring',
    });
  }

  async upsertSensors(events: TelemetryRawEvent[]): Promise<void> {
    if (events.length === 0) {
      return;
    }

    // Deduplica por sensor_id dentro do batch para atualizar apenas o estado mais recente
    const latestBySensor = new Map<string, TelemetryRawEvent>();
    for (const ev of events) {
      latestBySensor.set(ev.sensor_id, ev);
    }

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      for (const ev of latestBySensor.values()) {
        await client.query(
          `INSERT INTO sensors (sensor_id, type, unit, last_value, last_seen_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, NOW())
           ON CONFLICT (sensor_id) DO UPDATE SET
             type = EXCLUDED.type,
             unit = EXCLUDED.unit,
             last_value = EXCLUDED.last_value,
             last_seen_at = EXCLUDED.last_seen_at,
             updated_at = NOW()`,
          [
            ev.sensor_id,
            ev.type,
            ev.unit,
            ev.value,
            ev.timestamps.sensor_created_at,
          ],
        );
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }
}
