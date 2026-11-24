import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();

  const port = Number(process.env.PORT ?? 3002);
  await app.listen(port);

  const logger = new Logger('Bootstrap');
  logger.log(
    `persistence-service listening on port ${port} (EVENT_BUS_DRIVER=${process.env.EVENT_BUS_DRIVER ?? 'redis'})`,
  );
}

void bootstrap();
