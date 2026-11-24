import { EVENT_BUS_PUBLISHER, EventBusPublisher, TOPICS } from '@tcc/event-bus';
import { Test } from '@nestjs/testing';
import { DeduplicationService } from './deduplication.service';
import { IngestService } from './ingest.service';

describe('IngestService', () => {
  let ingestService: IngestService;
  let publishMock: jest.Mock;

  beforeEach(async () => {
    publishMock = jest.fn().mockResolvedValue(undefined);
    const publisher: EventBusPublisher = { publish: publishMock };

    const moduleRef = await Test.createTestingModule({
      providers: [
        IngestService,
        {
          provide: DeduplicationService,
          useValue: new DeduplicationService(),
        },
        {
          provide: EVENT_BUS_PUBLISHER,
          useValue: publisher,
        },
      ],
    }).compile();

    ingestService = moduleRef.get(IngestService);
  });

  it('publishes valid telemetry with microsecond UTC ingested_at', async () => {
    const payload = JSON.stringify({
      sensor_id: 'sensor-01',
      type: 'temperature',
      value: 26.4,
      unit: 'C',
      sensor_created_at: '2026-11-24T15:00:00.120Z',
    });

    const result = await ingestService.processRawMessage(payload);

    expect(result.accepted).toBe(true);
    expect(result.event?.sensor_id).toBe('sensor-01');
    expect(result.event?.timestamps.ingested_at).toMatch(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/,
    );
    expect(publishMock).toHaveBeenCalledTimes(1);
    expect(publishMock).toHaveBeenCalledWith(
      TOPICS.TELEMETRY_RAW,
      expect.objectContaining({
        sensor_id: 'sensor-01',
        type: 'temperature',
        value: 26.4,
        unit: 'C',
      }),
      { key: 'sensor-01' },
    );
  });

  it('rejects duplicate messages based on sensor_id + timestamp or message_id', async () => {
    const payload = JSON.stringify({
      message_id: 'msg-123',
      sensor_id: 'sensor-01',
      type: 'temperature',
      value: 26.4,
      unit: 'C',
      sensor_created_at: '2026-11-24T15:00:00.120Z',
    });

    const first = await ingestService.processRawMessage(payload);
    const second = await ingestService.processRawMessage(payload);

    expect(first.accepted).toBe(true);
    expect(second).toEqual({ accepted: false, reason: 'duplicate' });
    expect(publishMock).toHaveBeenCalledTimes(1);
  });

  it('rejects malformed JSON and invalid schemas without publishing', async () => {
    const badJson = await ingestService.processRawMessage('{invalid');
    const badSchema = await ingestService.processRawMessage(
      JSON.stringify({ sensor_id: 'sensor-01', value: 'not-a-number' }),
    );

    expect(badJson).toEqual({ accepted: false, reason: 'invalid_json' });
    expect(badSchema).toEqual({ accepted: false, reason: 'invalid_schema' });
    expect(publishMock).not.toHaveBeenCalled();
  });
});
