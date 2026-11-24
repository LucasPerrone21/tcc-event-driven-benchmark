import { Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import {
  EventBusConsumer,
  EventBusPublisher,
  EventHandler,
  PublishOptions,
  SubscribeOptions,
} from '../interfaces/event-bus.interface';

export interface RedisStreamConfig {
  host: string;
  port: number;
}

export class RedisStreamEventBusAdapter
  implements EventBusPublisher, EventBusConsumer, OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(RedisStreamEventBusAdapter.name);
  private readonly client: Redis;
  private readonly blockingClients: Redis[] = [];
  private isShuttingDown = false;

  constructor(private readonly config: RedisStreamConfig) {
    this.client = new Redis({
      host: this.config.host,
      port: this.config.port,
      maxRetriesPerRequest: null,
    });
  }

  async onModuleInit(): Promise<void> {
    await this.client.ping();
    this.logger.log(
      `Connected to Redis Streams at ${this.config.host}:${this.config.port}`,
    );
  }

  async onModuleDestroy(): Promise<void> {
    this.isShuttingDown = true;
    await Promise.allSettled([
      this.client.quit(),
      ...this.blockingClients.map((c) => c.quit()),
    ]);
  }

  async publish<T>(
    topic: string,
    payload: T,
    options?: PublishOptions,
  ): Promise<void> {
    const serialized = JSON.stringify(payload);
    const args: string[] = ['payload', serialized];
    if (options?.key) {
      args.push('key', options.key);
    }
    await this.client.xadd(topic, '*', ...args);
  }

  async subscribe<T>(
    topic: string,
    handler: EventHandler<T>,
    options: SubscribeOptions,
  ): Promise<void> {
    const consumerName =
      options.consumerName ?? `${options.groupId}-${process.pid}`;
    const count = options.prefetch ?? 10;

    try {
      await this.client.xgroup('CREATE', topic, options.groupId, '0', 'MKSTREAM');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('BUSYGROUP')) {
        throw err;
      }
    }

    const reader = this.client.duplicate();
    this.blockingClients.push(reader);

    void this.pollLoop(reader, topic, options.groupId, consumerName, count, handler);
  }

  private async pollLoop<T>(
    reader: Redis,
    topic: string,
    groupId: string,
    consumerName: string,
    count: number,
    handler: EventHandler<T>,
  ): Promise<void> {
    while (!this.isShuttingDown) {
      try {
        const response = (await reader.xreadgroup(
          'GROUP',
          groupId,
          consumerName,
          'COUNT',
          count,
          'BLOCK',
          2000,
          'STREAMS',
          topic,
          '>',
        )) as Array<[string, Array<[string, string[]]>]> | null;

        if (!response) {
          continue;
        }

        for (const [, entries] of response) {
          for (const [id, fields] of entries) {
            const payloadIndex = fields.indexOf('payload');
            if (payloadIndex === -1 || payloadIndex + 1 >= fields.length) {
              await this.client.xack(topic, groupId, id);
              continue;
            }

            const payload = JSON.parse(fields[payloadIndex + 1]) as T;
            await handler({
              topic,
              payload,
              raw: { id, fields },
              ack: async () => {
                await this.client.xack(topic, groupId, id);
              },
              nack: async () => {
                // Em Redis Streams, não enviar XACK mantém a mensagem na PEL para reprocessamento
              },
            });
          }
        }
      } catch (err) {
        if (!this.isShuttingDown) {
          this.logger.error(
            `Error reading Redis Stream "${topic}": ${err instanceof Error ? err.message : String(err)}`,
          );
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    }
  }
}
