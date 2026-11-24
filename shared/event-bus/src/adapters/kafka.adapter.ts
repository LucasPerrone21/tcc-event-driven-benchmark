import { Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Consumer, Kafka, Producer } from 'kafkajs';
import {
  EventBusConsumer,
  EventBusPublisher,
  EventHandler,
  PublishOptions,
  SubscribeOptions,
} from '../interfaces/event-bus.interface';

export interface KafkaConfig {
  clientId: string;
  brokers: string[];
}

export class KafkaEventBusAdapter
  implements EventBusPublisher, EventBusConsumer, OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(KafkaEventBusAdapter.name);
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly consumers: Consumer[] = [];
  private producerConnected = false;

  constructor(private readonly config: KafkaConfig) {
    this.kafka = new Kafka({
      clientId: this.config.clientId,
      brokers: this.config.brokers,
    });
    this.producer = this.kafka.producer();
  }

  async onModuleInit(): Promise<void> {
    await this.ensureProducerConnected();
    this.logger.log(
      `Connected to Kafka brokers: ${this.config.brokers.join(', ')}`,
    );
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.allSettled([
      this.producerConnected ? this.producer.disconnect() : Promise.resolve(),
      ...this.consumers.map((c) => c.disconnect()),
    ]);
  }

  private async ensureProducerConnected(): Promise<void> {
    if (!this.producerConnected) {
      await this.producer.connect();
      this.producerConnected = true;
    }
  }

  async publish<T>(
    topic: string,
    payload: T,
    options?: PublishOptions,
  ): Promise<void> {
    await this.ensureProducerConnected();
    await this.producer.send({
      topic,
      acks: -1,
      messages: [
        {
          key: options?.key,
          value: JSON.stringify(payload),
          headers: options?.headers,
        },
      ],
    });
  }

  async subscribe<T>(
    topic: string,
    handler: EventHandler<T>,
    options: SubscribeOptions,
  ): Promise<void> {
    const consumer = this.kafka.consumer({ groupId: options.groupId });
    this.consumers.push(consumer);

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        if (!message.value) {
          return;
        }
        const payload = JSON.parse(message.value.toString('utf-8')) as T;
        await handler({
          topic,
          payload,
          raw: { partition, message },
          ack: async () => {
            await consumer.commitOffsets([
              {
                topic,
                partition,
                offset: (BigInt(message.offset) + 1n).toString(),
              },
            ]);
          },
          nack: async () => {
            // Sem commitOffsets, a partição reprocessará a partir deste offset no próximo rebalance/restart
          },
        });
      },
    });
  }
}
