# Sublite Distributed

Тот же домен, что [sublite-core](https://github.com/david-arutyunyan/sublite-core)
(подписки, биллинг, retention/cancellation flow, лояльность), разрезанный на
сервисы через Kafka. Цель проекта — закрыть пробел с брокерами сообщений и
Kubernetes: Transactional Outbox, идемпотентные консьюмеры, Saga (хореография)
с компенсацией, Resilience4j (circuit breaker, retry, DLQ), Kafka в KRaft,
Kubernetes-манифесты под kind, Prometheus/Grafana, OpenTelemetry/Jaeger.

Обоснование границ сервисов и полная схема событий — [docs/architecture.md](docs/architecture.md).

## Сервисы

| Сервис | Стек | Статус |
|---|---|---|
| subscription-service | Java 21, Spring Boot 4, Postgres | покупка подписки + Transactional Outbox |
| billing-service | Java 21, Spring Boot 4, Postgres | не начат |
| notification-service | Java 21, Spring Boot 4, MongoDB | не начат |
| analytics-service | Go, Postgres | не начат (опционально) |

## Быстрый старт

```bash
docker compose up -d --build
```

Поднимает Kafka (KRaft, один брокер) + создаёт топики из
`docs/architecture.md` + [Kafka UI](http://localhost:8090) для просмотра
топиков/партиций/сообщений + subscription-service (`localhost:8081`) со
своим Postgres.

```bash
docker compose ps
docker compose logs kafka-init          # список созданных топиков
docker compose logs subscription-service -f
```

Купить подписку (два демо-плана уже засеяны — `docs/architecture.md`
не покрывает каталог, коды и id смотри в
`subscription-service/src/main/resources/db/migration/V4__seed_plans.sql`):

```bash
curl -X POST http://localhost:8081/subscriptions \
  -H "Content-Type: application/json" \
  -d '{"customerId":"11111111-2222-3333-4444-555555555555","planPriceId":"33333333-3333-3333-3333-333333333333"}'
```

Ответ — `status: PENDING_PAYMENT` (не `ACTIVE` — списание асинхронное,
через billing-service, которого пока нет, см. `SubscriptionStatus.java`).
Событие `SubscriptionCreated` уходит в топик `subscription.events`
(ключ — `subscriptionId`) в течение секунды — смотреть либо в Kafka UI,
либо:

```bash
docker exec sublite-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic subscription.events \
  --property print.key=true --from-beginning
```

## План

1. ~~Границы сервисов и схема событий~~ — [docs/architecture.md](docs/architecture.md)
2. ~~Kafka в Compose, топики~~
3. ~~Transactional Outbox (поллер) в subscription-service~~ — этот шаг
4. Вынос billing-service, консьюмер SubscriptionCreated
5-6. notification-service, идемпотентные консьюмеры, DLQ
7. Saga с компенсацией на сценарий отмены подписки
8. Resilience4j: circuit breaker, retry, таймауты
9-10. Prometheus, Grafana, OpenTelemetry, Jaeger
11. Kubernetes-манифесты под kind
12. README и диаграммы
