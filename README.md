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
| subscription-service | Java 21, Spring Boot 4, Postgres | не начат |
| billing-service | Java 21, Spring Boot 4, Postgres | не начат |
| notification-service | Java 21, Spring Boot 4, MongoDB | не начат |
| analytics-service | Go, Postgres | не начат (опционально) |

## Быстрый старт (пока только инфраструктура)

```bash
docker compose up -d
```

Поднимает Kafka (KRaft, один брокер) + создаёт топики из
`docs/architecture.md` + [Kafka UI](http://localhost:8090) для просмотра
топиков/партиций/сообщений.

```bash
docker compose ps
docker compose logs kafka-init   # список созданных топиков
```

## План

1. ~~Границы сервисов и схема событий~~ — [docs/architecture.md](docs/architecture.md)
2. Kafka в Compose, топики — этот шаг
3-4. Transactional Outbox (поллер, Debezium — если успеем) + вынос billing-service
5-6. notification-service, идемпотентные консьюмеры, DLQ
7. Saga с компенсацией на сценарий отмены подписки
8. Resilience4j: circuit breaker, retry, таймауты
9-10. Prometheus, Grafana, OpenTelemetry, Jaeger
11. Kubernetes-манифесты под kind
12. README и диаграммы
