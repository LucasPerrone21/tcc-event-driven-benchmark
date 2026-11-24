import { Module } from '@nestjs/common';
import { EventBusModule } from '@tcc/event-bus';
import { HealthController } from './health.controller';
import { MqttIngestConsumer } from './mqtt/mqtt-ingest.consumer';
import { DeduplicationService } from './services/deduplication.service';
import { IngestService } from './services/ingest.service';

@Module({
  imports: [
    EventBusModule.forRoot({
      serviceName: 'ingest-service',
    }),
  ],
  controllers: [HealthController],
  providers: [DeduplicationService, IngestService, MqttIngestConsumer],
})
export class AppModule {}
