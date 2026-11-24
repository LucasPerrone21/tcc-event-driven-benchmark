import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  EVENT_BUS_PUBLISHER,
  EventBusPublisher,
  TOPICS,
  TelemetryRawEvent,
} from '@tcc/event-bus';
import { plainToInstance } from 'class-transformer';
import { validateSync } from 'class-validator';
import { randomUUID } from 'crypto';
import { TelemetryPayloadDto } from '../dto/telemetry-payload.dto';
import { utcNowMicroseconds } from '../utils/time.util';
import { DeduplicationService } from './deduplication.service';

export interface IngestResult {
  accepted: boolean;
  reason?: 'invalid_json' | 'invalid_schema' | 'duplicate';
  event?: TelemetryRawEvent;
}

@Injectable()
export class IngestService {
  private readonly logger = new Logger(IngestService.name);

  constructor(
    @Inject(EVENT_BUS_PUBLISHER)
    private readonly publisher: EventBusPublisher,
    private readonly deduplication: DeduplicationService,
  ) {}

  async processRawMessage(rawPayload: Buffer | string): Promise<IngestResult> {
    let parsed: unknown;
    try {
      const text =
        typeof rawPayload === 'string'
          ? rawPayload
          : rawPayload.toString('utf-8');
      parsed = JSON.parse(text);
    } catch {
      this.logger.warn('Discarding malformed JSON telemetry message');
      return { accepted: false, reason: 'invalid_json' };
    }

    const dto = plainToInstance(TelemetryPayloadDto, parsed);
    const errors = validateSync(dto, {
      whitelist: true,
      forbidNonWhitelisted: false,
    });

    const sensorCreatedAt = dto.sensor_created_at ?? dto.timestamp;
    if (errors.length > 0 || !sensorCreatedAt) {
      this.logger.warn('Discarding telemetry message with invalid schema');
      return { accepted: false, reason: 'invalid_schema' };
    }

    const dedupKey = dto.message_id
      ? `msg:${dto.sensor_id}:${dto.message_id}`
      : `ts:${dto.sensor_id}:${sensorCreatedAt}`;

    if (this.deduplication.isDuplicate(dedupKey)) {
      this.logger.debug(`Duplicate telemetry ignored (${dedupKey})`);
      return { accepted: false, reason: 'duplicate' };
    }

    const event: TelemetryRawEvent = {
      event_id: randomUUID(),
      sensor_id: dto.sensor_id,
      type: dto.type,
      value: dto.value,
      unit: dto.unit,
      timestamps: {
        sensor_created_at: sensorCreatedAt,
        ingested_at: utcNowMicroseconds(),
      },
    };

    await this.publisher.publish(TOPICS.TELEMETRY_RAW, event, {
      key: event.sensor_id,
    });

    return { accepted: true, event };
  }
}
