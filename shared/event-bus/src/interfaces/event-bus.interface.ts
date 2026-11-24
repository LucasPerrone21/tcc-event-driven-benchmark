export const EVENT_BUS_PUBLISHER = 'EVENT_BUS_PUBLISHER';
export const EVENT_BUS_CONSUMER = 'EVENT_BUS_CONSUMER';

export type EventBusDriver = 'redis' | 'rabbitmq' | 'kafka';

export interface PublishOptions {
  key?: string;
  headers?: Record<string, string>;
}

export interface EventMessage<T = unknown> {
  topic: string;
  payload: T;
  ack: () => Promise<void>;
  nack: (requeue?: boolean) => Promise<void>;
  raw?: unknown;
}

export type EventHandler<T = unknown> = (message: EventMessage<T>) => Promise<void>;

export interface SubscribeOptions {
  groupId: string;
  consumerName?: string;
  prefetch?: number;
}

export interface EventBusPublisher {
  publish<T>(topic: string, payload: T, options?: PublishOptions): Promise<void>;
}

export interface EventBusConsumer {
  subscribe<T>(
    topic: string,
    handler: EventHandler<T>,
    options: SubscribeOptions,
  ): Promise<void>;
}
