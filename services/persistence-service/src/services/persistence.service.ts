import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import {
  EVENT_BUS_CONSUMER,
  EventBusConsumer,
  EventMessage,
  TOPICS,
  TelemetryRawEvent,
} from '@tcc/event-bus';
import { InfluxRepository, PersistedTelemetryItem } from '../repositories/influx.repository';
import { PostgresRepository } from '../repositories/postgres.repository';
import { utcNowMicroseconds } from '../utils/time.util';

@Injectable()
export class PersistenceService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PersistenceService.name);
  private readonly batchSize = Number(process.env.BATCH_SIZE ?? 50);
  private readonly flushIntervalMs = Number(
    process.env.BATCH_FLUSH_INTERVAL_MS ?? 200,
  );
  private buffer: Array<EventMessage<TelemetryRawEvent>> = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private flushing = false;

  constructor(
    @Inject(EVENT_BUS_CONSUMER)
    private readonly consumer: EventBusConsumer,
    private readonly influx: InfluxRepository,
    private readonly postgres: PostgresRepository,
  ) {}

  async onModuleInit(): Promise<void> {
    this.flushTimer = setInterval(() => {
      void this.flush();
    }, this.flushIntervalMs);

    await this.consumer.subscribe<TelemetryRawEvent>(
      TOPICS.TELEMETRY_RAW,
      async (message) => {
        await this.enqueue(message);
      },
      {
        groupId: 'persistence-service',
        prefetch: this.batchSize,
      },
    );

    this.logger.log(
      `persistence-service subscribed to "${TOPICS.TELEMETRY_RAW}" (batchSize=${this.batchSize}, flushIntervalMs=${this.flushIntervalMs})`,
    );
  }

  async onModuleDestroy(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flush();
  }

  async enqueue(message: EventMessage<TelemetryRawEvent>): Promise<void> {
    this.buffer.push(message);
    if (this.buffer.length >= this.batchSize) {
      await this.flush();
    }
  }

  async flush(): Promise<void> {
    if (this.flushing || this.buffer.length === 0) {
      return;
    }

    this.flushing = true;
    const batch = this.buffer;
    this.buffer = [];

    try {
      const persistedAt = utcNowMicroseconds();
      const items: PersistedTelemetryItem[] = batch.map((msg) => ({
        event: {
          ...msg.payload,
          timestamps: {
            ...msg.payload.timestamps,
            persisted_at: persistedAt,
          },
        },
        persistedAt,
      }));

      await Promise.all([
        this.influx.writeBatch(items),
        this.postgres.upsertSensors(items.map((i) => i.event)),
      ]);

      await Promise.all(batch.map((msg) => msg.ack()));
    } catch (err) {
      this.logger.error(
        `Failed to persist batch of ${batch.length} events: ${err instanceof Error ? err.message : String(err)}`,
      );
      await Promise.allSettled(batch.map((msg) => msg.nack(true)));
    } finally {
      this.flushing = false;
    }
  }
}
