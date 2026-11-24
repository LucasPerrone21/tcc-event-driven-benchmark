FROM node:22-alpine AS builder

WORKDIR /app

# Build shared event-bus package
COPY shared/event-bus/package*.json ./shared/event-bus/
RUN cd shared/event-bus && npm install
COPY shared/event-bus/ ./shared/event-bus/
RUN cd shared/event-bus && npm run build

# Build persistence-service
COPY services/persistence-service/package*.json ./services/persistence-service/
RUN cd services/persistence-service && npm install
COPY services/persistence-service/ ./services/persistence-service/
RUN cd services/persistence-service && npm run build

FROM node:22-alpine AS runner

WORKDIR /app/services/persistence-service
ENV NODE_ENV=production

COPY --from=builder /app/shared/event-bus/package*.json /app/shared/event-bus/
COPY --from=builder /app/shared/event-bus/dist /app/shared/event-bus/dist
COPY --from=builder /app/shared/event-bus/node_modules /app/shared/event-bus/node_modules

COPY --from=builder /app/services/persistence-service/package*.json ./
COPY --from=builder /app/services/persistence-service/dist ./dist
COPY --from=builder /app/services/persistence-service/node_modules ./node_modules

EXPOSE 3002
CMD ["node", "dist/main.js"]
