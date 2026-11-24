import { Module } from '@nestjs/common';
import { EventBusModule } from '@tcc/event-bus';
import { HealthController } from './health.controller';
import { InfluxRepository } from './repositories/influx.repository';
import { PostgresRepository } from './repositories/postgres.repository';
import { PersistenceService } from './services/persistence.service';

@Module({
  imports: [
    EventBusModule.forRoot({
      serviceName: 'persistence-service',
    }),
  ],
  controllers: [HealthController],
  providers: [InfluxRepository, PostgresRepository, PersistenceService],
})
export class AppModule {}
