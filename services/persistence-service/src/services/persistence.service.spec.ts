import { Test } from '@nestjs/testing';
import {
  EVENT_BUS_CONSUMER,
  EventBusConsumer,
  EventMessage,
  TOPICS,
  TelemetryRawEvent,
} from '@tcc/event-bus';
import { InfluxRepository } from '../repositories/influx.repository';
import { PostgresRepository } from '../repositories/postgres.repository';
import { PersistenceService } from './persistence.service';

describe('PersistenceService', () => {
  let service: PersistenceService;
  let writeBatchMock: jest.Mock;
  let upsertSensorsMock: jest.Mock;

  beforeEach(async () => {
    writeBatchMock = jest.fn().mockResolvedValue(undefined);
    upsertSensorsMock = jest.fn().mockResolvedValue(undefined);

    const consumer: EventBusConsumer = {
      subscribe: jest.fn().mockResolvedValue(undefined),
    };

    const moduleRef = await Test.createTestingModule({
      providers: [
        PersistenceService,
        { provide: EVENT_BUS_CONSUMER, useValue: consumer },
        { provide: InfluxRepository, useValue: { writeBatch: writeBatchMock } },
        { provide: PostgresRepository, useValue: { upsertSensors: upsertSensorsMock } },
      ],
    }).compile();

    service = moduleRef.get(PersistenceService);
  });

  it('stamps persisted_at, writes to InfluxDB and Postgres, and ACKs messages on flush', async () => {
    const ack = jest.fn().mockResolvedValue(undefined);
    const nack = jest.fn().mockResolvedValue(undefined);
    const msg: EventMessage<TelemetryRawEvent> = {
      topic: TOPICS.TELEMETRY_RAW,
      payload: {
        event_id: 'evt-1',
        sensor_id: 'sensor-01',
        type: 'temperature',
        value: 25.5,
        unit: 'C',
        timestamps: {
          sensor_created_at: '2026-11-24T15:00:00.120000Z',
          ingested_at: '2026-11-24T15:00:00.125000Z',
        },
      },
      ack,
      nack,
    };

    await service.enqueue(msg);
    await service.flush();

    expect(writeBatchMock).toHaveBeenCalledTimes(1);
    const writtenItems = writeBatchMock.mock.calls[0][0];
    expect(writtenItems[0].persistedAt).toMatch(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/,
    );
    expect(upsertSensorsMock).toHaveBeenCalledTimes(1);
    expect(ack).toHaveBeenCalledTimes(1);
    expect(nack).not.toHaveBeenCalled();
  });

  it('calls nack(true) and does not ACK when persistence fails', async () => {
    writeBatchMock.mockRejectedValueOnce(new Error('InfluxDB unavailable'));
    const ack = jest.fn().mockResolvedValue(undefined);
    const nack = jest.fn().mockResolvedValue(undefined);

    await service.enqueue({
      topic: TOPICS.TELEMETRY_RAW,
      payload: {
        event_id: 'evt-2',
        sensor_id: 'sensor-02',
        type: 'humidity',
        value: 60,
        unit: '%',
        timestamps: {
          sensor_created_at: '2026-11-24T15:00:00.120000Z',
          ingested_at: '2026-11-24T15:00:00.125000Z',
        },
      },
      ack,
      nack,
    });
    await service.flush();

    expect(ack).not.toHaveBeenCalled();
    expect(nack).toHaveBeenCalledWith(true);
  });
});
