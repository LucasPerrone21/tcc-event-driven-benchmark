FROM node:22-alpine AS builder

WORKDIR /app

# Build shared event-bus package
COPY shared/event-bus/package*.json ./shared/event-bus/
RUN cd shared/event-bus && npm install
COPY shared/event-bus/ ./shared/event-bus/
RUN cd shared/event-bus && npm run build

# Build ingest-service
COPY services/ingest-service/package*.json ./services/ingest-service/
RUN cd services/ingest-service && npm install
COPY services/ingest-service/ ./services/ingest-service/
RUN cd services/ingest-service && npm run build

FROM node:22-alpine AS runner

WORKDIR /app/services/ingest-service
ENV NODE_ENV=production

COPY --from=builder /app/shared/event-bus/package*.json /app/shared/event-bus/
COPY --from=builder /app/shared/event-bus/dist /app/shared/event-bus/dist
COPY --from=builder /app/shared/event-bus/node_modules /app/shared/event-bus/node_modules

COPY --from=builder /app/services/ingest-service/package*.json ./
COPY --from=builder /app/services/ingest-service/dist ./dist
COPY --from=builder /app/services/ingest-service/node_modules ./node_modules

EXPOSE 3001
CMD ["node", "dist/main.js"]
