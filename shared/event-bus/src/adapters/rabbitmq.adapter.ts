import { Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import amqplib, { Channel, ConsumeMessage } from 'amqplib';
import {
  EventBusConsumer,
  EventBusPublisher,
  EventHandler,
  PublishOptions,
  SubscribeOptions,
} from '../interfaces/event-bus.interface';

export interface RabbitMqConfig {
  url: string;
}

type AmqpConnection = Awaited<ReturnType<typeof amqplib.connect>>;

export class RabbitMqEventBusAdapter
  implements EventBusPublisher, EventBusConsumer, OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(RabbitMqEventBusAdapter.name);
  private connection: AmqpConnection | null = null;
  private publishChannel: Channel | null = null;
  private readonly consumerChannels: Channel[] = [];
  private readonly assertedExchanges = new Set<string>();

  constructor(private readonly config: RabbitMqConfig) {}

  async onModuleInit(): Promise<void> {
    await this.ensureConnected();
    this.logger.log('Connected to RabbitMQ');
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.allSettled([
      ...this.consumerChannels.map((ch) => ch.close()),
      this.publishChannel?.close(),
      this.connection?.close(),
    ]);
  }

  private async ensureConnected(): Promise<Channel> {
    if (this.publishChannel) {
      return this.publishChannel;
    }
    this.connection = await amqplib.connect(this.config.url);
    this.publishChannel = await this.connection.createChannel();
    return this.publishChannel;
  }

  private async ensureExchange(channel: Channel, topic: string): Promise<void> {
    if (this.assertedExchanges.has(topic)) {
      return;
    }
    await channel.assertExchange(topic, 'fanout', { durable: true });
    this.assertedExchanges.add(topic);
  }

  async publish<T>(
    topic: string,
    payload: T,
    options?: PublishOptions,
  ): Promise<void> {
    const channel = await this.ensureConnected();
    await this.ensureExchange(channel, topic);

    const buffer = Buffer.from(JSON.stringify(payload), 'utf-8');
    channel.publish(topic, options?.key ?? '', buffer, {
      persistent: true,
      contentType: 'application/json',
      headers: options?.headers,
    });
  }

  async subscribe<T>(
    topic: string,
    handler: EventHandler<T>,
    options: SubscribeOptions,
  ): Promise<void> {
    await this.ensureConnected();
    if (!this.connection) {
      throw new Error('RabbitMQ connection not initialized');
    }

    const channel = await this.connection.createChannel();
    this.consumerChannels.push(channel);

    await this.ensureExchange(channel, topic);
    const queueName = `${topic}.${options.groupId}`;
    await channel.assertQueue(queueName, { durable: true });
    await channel.bindQueue(queueName, topic, '');
    await channel.prefetch(options.prefetch ?? 10);

    await channel.consume(
      queueName,
      async (msg: ConsumeMessage | null) => {
        if (!msg) {
          return;
        }
        try {
          const payload = JSON.parse(msg.content.toString('utf-8')) as T;
          await handler({
            topic,
            payload,
            raw: msg,
            ack: async () => {
              channel.ack(msg);
            },
            nack: async (requeue = true) => {
              channel.nack(msg, false, requeue);
            },
          });
        } catch (err) {
          this.logger.error(
            `Error processing RabbitMQ message from "${queueName}": ${err instanceof Error ? err.message : String(err)}`,
          );
          channel.nack(msg, false, false);
        }
      },
      { noAck: false },
    );
  }
}
