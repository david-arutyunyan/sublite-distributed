# Архитектура: границы сервисов и схема событий

Разрез монолита [sublite-core](https://github.com/david-arutyunyan/sublite-core)
на события через Kafka. Домен тот же (подписки, биллинг, retention/cancellation
flow, лояльность) — здесь фиксируется, как он делится на сервисы и какие
события между ними ходят. `billing` в монолите специально держит
`invoice.subscription_id` без FK (см. миграцию V11) — уже тогда закладывался
будущий вынос в отдельный сервис.

## Границы сервисов

| Сервис | Владеет | Почему отдельно |
|---|---|---|
| **subscription-service** (Java 21, Spring Boot 4, Postgres) | Plan/PlanPrice, Subscription + state machine, Retention/Cancellation flow, Loyalty | Ядро — жизненный цикл подписки и всё, что напрямую от него зависит |
| **billing-service** (Java 21, Spring Boot 4, Postgres) | Invoice, PaymentAttempt, платёжный шлюз | Другой профиль надёжности (внешний провайдер — медленно, нестабильно, нужен circuit breaker) и другой профиль чувствительности данных. Уже анонсировано в монолите как кандидат на вынос |
| **notification-service** (Java 21, Spring Boot 4, MongoDB) | История уведомлений | Неудачная отправка НИКОГДА не должна откатывать бизнес-транзакцию — классический eventually-consistent consumer. MongoDB естественно ложится на слабоструктурированный контент уведомлений |
| **analytics-service** (Go, Postgres, опционально) | Агрегаты воронки отмены | Другой язык — доказывает, что событийный контракт языко-агностичен, а не завязан на Java-сериализацию |

**Loyalty не выделен в отдельный сервис** — маленький контекст, тесно
завязанный на события подписки, без своего независимого жизненного цикла.
Отдельный сервис ради него — лишний сетевой хоп без архитектурной выгоды.

**Retention — отдельный топик, не отдельный сервис.** Логически часть
subscription-service, но события в своём топике `retention.events`, а не в
`subscription.events` — у них разные потребители (analytics-service хочет
только воронку отмены, `SubscriptionCreated` ему не нужен и будет шумом).

```mermaid
graph LR
    subgraph SUB["subscription-service (Java, Postgres)"]
        S1[Subscription lifecycle]
        S2[Retention / Cancellation]
        S3[Loyalty]
    end
    subgraph BILL["billing-service (Java, Postgres)"]
        B1[Invoices & Payments]
    end
    subgraph NOTIF["notification-service (Java, MongoDB)"]
        N1[Notification history]
    end
    subgraph AN["analytics-service (Go, Postgres)"]
        A1[Cancellation funnel]
    end

    S1 -.publishes.-> T1[(subscription.events)]
    S2 -.publishes.-> T2[(retention.events)]
    S3 -.publishes.-> T3[(loyalty.events)]
    B1 -.publishes.-> T4[(billing.events)]

    T1 -.consumes.-> BILL
    T1 -.consumes.-> NOTIF
    T4 -.consumes.-> SUB
    T4 -.consumes.-> NOTIF
    T2 -.consumes.-> S3
    T2 -.consumes.-> AN
    T2 -.consumes.-> NOTIF
```

## Правило именования топиков

Топик называется по домену **издателя**, не потребителя: `subscription.events`
— факты, которые публикует subscription-service о себе. Отсюда:
subscription-service знает, когда подписка "созрела" для списания (у него
`currentPeriodEnd`), и публикует `SubscriptionRenewalDue` в **свой** топик —
это факт о подписке, а не команда billing-сервису. billing-service слушает
`subscription.events`, ведёт свою локальную проекцию "какие подписки я
биллю" и решает списывать сам. Осознанная альтернатива синхронному вызову
из scheduler'а, который вернул бы тесную связанность.

## Каталог событий

| Топик | Событие | Ключ | Публикует | Слушает |
|---|---|---|---|---|
| `subscription.events` | SubscriptionCreated | subscriptionId | subscription-service | billing-service, notification-service |
| | SubscriptionRenewalDue | subscriptionId | subscription-service | billing-service |
| | SubscriptionRenewed | subscriptionId | subscription-service | notification-service |
| | SubscriptionCancelled | subscriptionId | subscription-service | billing-service, notification-service, analytics-service |
| | SubscriptionPaused | subscriptionId | subscription-service | billing-service |
| `retention.events` | CancellationStarted | subscriptionId | subscription-service | analytics-service |
| | RetentionOfferShown | subscriptionId | subscription-service | analytics-service |
| | RetentionOfferAccepted | subscriptionId | subscription-service | loyalty-модуль, analytics-service |
| | RetentionOfferDeclined | subscriptionId | subscription-service | analytics-service |
| | SubscriptionRetained | subscriptionId | subscription-service | notification-service, analytics-service |
| `billing.events` | PaymentSucceeded | subscriptionId | billing-service | subscription-service, notification-service |
| | PaymentFailed | subscriptionId | billing-service | subscription-service, notification-service |
| | RefundIssued / RefundFailed | subscriptionId | billing-service | subscription-service, notification-service |
| `loyalty.events` | LoyaltyPointsAwarded / AwardFailed | **customerId** | subscription-service (loyalty-модуль) | notification-service |
| `notification.events` | NotificationSent / NotificationFailed | customerId | notification-service | (саге/DLQ-наблюдаемость) |

Каждый топик из этой таблицы имеет DLQ-двойник (`<topic>.DLQ`) — созданы
заранее в `kafka/create-topics.sh`, retry+DLQ на реальных consumer'ах —
шаг 6 (ещё не сделан).

На сегодня в коде реализовано и реально публикуется только три события:
`SubscriptionCreated`, `PaymentSucceeded`, `PaymentFailed` — остальные
строки таблицы фиксируют целевую форму системы (нужны для saga на шаге 7),
но самих publisher'ов для них пока нет.

## Ключ сообщения

Kafka гарантирует порядок только внутри одной партиции (`hash(key) %
numPartitions`), никогда — внутри топика целиком. Без ключа — round-robin,
порядка нет вообще.

- **subscriptionId** для subscription/retention/billing-событий: consumer,
  обрабатывающий жизненный цикл одной подписки, обязан видеть
  `SubscriptionCreated` раньше `SubscriptionCancelled` для неё же.
- **customerId** для loyalty/notification: баланс баллов и история
  уведомлений — ресурс уровня клиента, не подписки.

Consistent-ключевание НЕ гарантирует межтопиковый порядок — это два
независимых потока, даже если ключ один и тот же. Если порядок между
`subscription.events` и `billing.events` важен — это забота consumer-логики
(проверка состояния перед применением события), а не брокера.

## Consumer groups

Каждый сервис, независимо потребляющий топик, — свой `group.id`. Общий
`group.id` у двух сервисов означает, что Kafka поделит партиции между ними
(каждый увидит только часть потока) — почти никогда не то, что нужно для
независимых сервисов. Когда один сервис масштабируется на несколько
инстансов (HPA, шаг 11) — все его поды используют один и тот же `group.id`,
и вот тогда партиции делятся между ними осознанно.

## Конверт события

Без Schema Registry/Avro на старте — осознанное упрощение, эволюция схем
отдельная тема, если понадобится позже.

```json
{
  "eventId": "0f3a...-uuid",
  "eventType": "SubscriptionCancelled",
  "aggregateId": "subscriptionId-uuid",
  "occurredAt": "2026-09-05T10:00:00Z",
  "correlationId": "request-scope-uuid",
  "payload": { "...": "событие-специфичные поля" }
}
```

`eventId` — ключ дедупликации на стороне consumer'а (`processed_messages`
таблица с уникальным индексом, проверяется перед обработкой). `correlationId`
— продолжение `CorrelationIdFilter` из sublite-core: связывает цепочку
событий через несколько сервисов в рамках одной саги.

## Партиции и гарантии доставки

3 партиции на топик — достаточно, чтобы увидеть параллелизм и ребалансировку
локально, без переинженеринга под нагрузку, которой в pet-проекте не будет.

At-least-once + идемпотентный consumer, не exactly-once. Kafka Transactions
дают EOS только строго внутри Kafka или Kafka→одна БД через outbox — не
через границу независимых сервисов с раздельными БД. Промышленный паттерн —
не бороться за EOS, а сделать side-effect идемпотентным через дедуп-таблицу.

## Идемпотентные consumer'ы (шаг 5, сделано)

`subscription-service` и `billing-service` — каждый со своей таблицей
`processed_messages` (`event_id UUID PRIMARY KEY`). Проверка "уже
обработано?" и запись в эту таблицу происходят в ОДНОЙ локальной
`@Transactional`-транзакции с самим side-effect'ом (списание, смена
статуса подписки) — падение между "сделали" и "записали, что сделали"
невозможно: либо оба произошли, либо ни один, повторная доставка в любом
случае безопасна.

`notification-service` (MongoDB) — та же гарантия, другой механизм:
у документа `Notification` `_id` — это сам `eventId`, и `insert()` (не
`save()` — тот делает upsert и молча перезапишет документ) кидает
`DuplicateKeyException` на повторе. Естественная уникальность `_id` вместо
отдельной дедуп-таблицы — там, где единственный side-effect и есть сама
запись, а не что-то, что нужно защищать отдельно.

Проверено не вызовом метода дважды, а настоящей передоставкой: захвачено
реальное сообщение `SubscriptionCreated` с боевого топика и опубликовано
повторно байт-в-байт (тот же `eventId`) — `billing-service` залогировал
skip и не создал второй Invoice.

`notification-service` также держит `subscription_projections` — локальную
проекцию `subscriptionId -> customerId`, построенную из `SubscriptionCreated`
(не синхронный запрос в subscription-service — database-per-service такое
не позволяет, да и не должен). `billing.events` не несёт customerId
(billing-service тоже не владеет данными клиента), поэтому уведомление по
оплате достаёт customerId из этой проекции. Живая проверка вскрыла реальный
гоч: у свежего consumer group'а с `auto.offset.reset=earliest` при первом
старте (или после большого бэклога) `billing.events` и `subscription.events`
догоняются НЕЗАВИСИМО — нет гарантии, что проекция из одного топика успеет
собраться раньше, чем событие из другого топика её потребует. Раз
случилось на реальном стеке: уведомление об оплате пришло раньше, чем
`SubscriptionCreated` для той же подписки успел построить проекцию.
Обработано явным fallback (customerId = "unknown" + warning в лог), а не
падением или потерей уведомления.

## Грабли

1. Число партиций топика можно только увеличивать, никогда не уменьшать —
   и увеличение задним числом меняет `hash(key) % N` для новых сообщений.
2. `enable.auto.commit=true` (дефолт) коммитит offset по таймеру независимо
   от успеха обработки — крах между auto-commit и завершением обработки
   теряет сообщение. Нужен manual commit после успешной обработки.
3. "Poison pill": одно неперевариваемое сообщение блокирует всю партицию за
   собой — Kafka не даёт закоммитить offset "через" непрочитанное сообщение.
4. Idempotent producer (`enable.idempotence=true`) убирает дубли только от
   ретраев самого продюсера — не имеет отношения к дублированию на стороне
   consumer'а при redelivery.
5. Rebalancing storm: consumer, не укладывающийся в `max.poll.interval.ms`
   или часто падающий, останавливает ребалансировкой всю группу (со старым
   eager-ассайнером). `CooperativeStickyAssignor` смягчает это.
6. KRaft, не ZooKeeper — большинство туториалов в интернете всё ещё про
   ZK-based setup, другой набор переменных окружения.
