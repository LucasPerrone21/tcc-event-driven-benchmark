import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import mqtt, { MqttClient } from 'mqtt';
import { IngestService } from '../services/ingest.service';

@Injectable()
export class MqttIngestConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MqttIngestConsumer.name);
  private client: MqttClient | null = null;

  constructor(private readonly ingestService: IngestService) {}

  onModuleInit(): void {
    const mqttUrl = process.env.MQTT_URL ?? 'mqtt://localhost:1883';
    const topic = process.env.MQTT_TELEMETRY_TOPIC ?? 'sensors/+/telemetry';

    this.client = mqtt.connect(mqttUrl, {
      clientId: `ingest-service-${process.pid}`,
      clean: true,
      reconnectPeriod: 2000,
    });

    this.client.on('connect', () => {
      this.logger.log(`Connected to MQTT broker at ${mqttUrl}`);
      this.client?.subscribe(topic, { qos: 1 }, (err) => {
        if (err) {
          this.logger.error(`Failed to subscribe to "${topic}": ${err.message}`);
          return;
        }
        this.logger.log(`Subscribed to MQTT topic "${topic}"`);
      });
    });

    this.client.on('message', (_topic, payload) => {
      void this.ingestService.processRawMessage(payload).catch((err) => {
        this.logger.error(
          `Failed to ingest message: ${err instanceof Error ? err.message : String(err)}`,
        );
      });
    });

    this.client.on('error', (err) => {
      this.logger.error(`MQTT error: ${err.message}`);
    });
  }

  async onModuleDestroy(): Promise<void> {
    if (!this.client) {
      return;
    }
    await new Promise<void>((resolve) => {
      this.client?.end(false, {}, () => resolve());
    });
  }
}
