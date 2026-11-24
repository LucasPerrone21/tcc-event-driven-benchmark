import { DynamicModule, Global, Module, Provider } from '@nestjs/common';
import { KafkaEventBusAdapter } from './adapters/kafka.adapter';
import { RabbitMqEventBusAdapter } from './adapters/rabbitmq.adapter';
import { RedisStreamEventBusAdapter } from './adapters/redis-stream.adapter';
import {
  EVENT_BUS_CONSUMER,
  EVENT_BUS_PUBLISHER,
  EventBusDriver,
} from './interfaces/event-bus.interface';

export interface EventBusModuleOptions {
  driver?: EventBusDriver;
  serviceName?: string;
}

const EVENT_BUS_ADAPTER = 'EVENT_BUS_ADAPTER';

@Global()
@Module({})
export class EventBusModule {
  static forRoot(options: EventBusModuleOptions = {}): DynamicModule {
    const driver = (options.driver ??
      process.env.EVENT_BUS_DRIVER ??
      'redis') as EventBusDriver;
    const serviceName =
      options.serviceName ?? process.env.SERVICE_NAME ?? 'tcc-service';

    const adapterProvider: Provider = {
      provide: EVENT_BUS_ADAPTER,
      useFactory: () => {
        switch (driver) {
          case 'redis':
            return new RedisStreamEventBusAdapter({
              host: process.env.REDIS_HOST ?? 'localhost',
              port: Number(process.env.REDIS_PORT ?? 6379),
            });
          case 'rabbitmq':
            return new RabbitMqEventBusAdapter({
              url:
                process.env.RABBITMQ_URL ??
                'amqp://tcc:tcc@localhost:5672',
            });
          case 'kafka':
            return new KafkaEventBusAdapter({
              clientId: serviceName,
              brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092')
                .split(',')
                .map((b) => b.trim()),
            });
          default:
            throw new Error(
              `Unsupported EVENT_BUS_DRIVER "${String(driver)}". Expected: redis | rabbitmq | kafka`,
            );
        }
      },
    };

    return {
      module: EventBusModule,
      providers: [
        adapterProvider,
        {
          provide: EVENT_BUS_PUBLISHER,
          useExisting: EVENT_BUS_ADAPTER,
        },
        {
          provide: EVENT_BUS_CONSUMER,
          useExisting: EVENT_BUS_ADAPTER,
        },
      ],
      exports: [EVENT_BUS_PUBLISHER, EVENT_BUS_CONSUMER],
    };
  }
}
