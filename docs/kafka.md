# Конфигурация Apache Kafka

## Kafka-кластер

Учебное окружение использует кластер из трёх Kafka brokers:

```text
Kafka Broker 1
Kafka Broker 2
Kafka Broker 3
```

Каждый broker имеет отдельный ID:

```text
Broker 1 -> ID 1
Broker 2 -> ID 2
Broker 3 -> ID 3
```

Kafka использует ZooKeeper.

---

# Kafka Topics

Приложение работает с:

```text
messages
filtered_messages
blocked_users
banned_words
```

Текущая конфигурация:

```text
Partitions:          1
Replication Factor:  2
```

для каждого пользовательского топика.

---

## messages

Основной входной поток:

```text
Producer
    |
    v
messages
    |
    v
process_messages
```

Конфигурация:

```text
Partitions:          1
Replication Factor:  2
```

---

## filtered_messages

Содержит сообщения, прошедшие обработку.

```text
process_messages
       |
       v
filtered_messages
       |
       v
Consumer
```

Конфигурация:

```text
Partitions:          1
Replication Factor:  2
```

---

## blocked_users

Содержит события:

```text
block
unblock
```

Конфигурация:

```text
Partitions:          1
Replication Factor:  2
```

---

## banned_words

Содержит события:

```text
add
remove
```

Конфигурация:

```text
Partitions:          1
Replication Factor:  2
```

---

# Replication

Для пользовательских топиков:

```text
Replication Factor = 2
```

Это означает, что каждая partition назначается двум replicas.

В кластере используются три brokers, поэтому leader и replica могут находиться на разных brokers.

---

# min.insync.replicas

Kafka brokers настроены:

```text
min.insync.replicas = 2
```

В Docker Compose:

```yaml
KAFKA_MIN_INSYNC_REPLICAS: 2
```

Поэтому для записи, требующей подтверждения от всех необходимых ISR, необходимо наличие достаточного количества синхронизированных replicas.

Рабочая схема:

```text
Replication Factor = 2
        |
        v
Replica 1 + Replica 2
        |
        v
ISR = 2
        |
        v
min.insync.replicas = 2
        |
        v
write OK
```

---

# Faust Changelog Topics

Faust Tables используют Kafka changelog.

Для проекта создаются:

```text
message-moderation-blocked-users-table-changelog
message-moderation-banned-words-table-changelog
```

Для автоматически создаваемых Faust topics приложение использует:

```python
TOPIC_REPLICATION_FACTOR = 2
```

и:

```python
app = faust.App(
    APP_NAME,
    broker=KAFKA_BROKER,
    store="rocksdb://",
    topic_partitions=TOPIC_PARTITIONS,
    topic_replication_factor=TOPIC_REPLICATION_FACTOR,
)
```

---

## blocked-users changelog

Схема:

```text
blocked_users
P=1
     |
     v
blocked_users_table
P=1
     |
     v
message-moderation-blocked-users-table-changelog
P=1
RF=2
```

---

## banned-words changelog

Схема:

```text
banned_words
P=1
     |
     v
banned_words_table
P=1
     |
     v
message-moderation-banned-words-table-changelog
P=1
RF=2
```

---

# Changelog Cleanup Policy

Changelog-топики предназначены для хранения актуального состояния ключей.

Для них используется:

```text
cleanup.policy=compact
```

Например, последовательность:

```text
spam -> True
spam -> False
spam -> True
```

представляет изменения состояния одного ключа `spam`.

---

# Kafka Broker Configuration

В Docker Compose каждый broker использует:

```text
INTERNAL listener
EXTERNAL listener
```

## INTERNAL

Используется контейнерами внутри Docker network.

Например:

```text
kafka-broker-1:29092
kafka-broker-2:29092
kafka-broker-3:29092
```

## EXTERNAL

Используется клиентами с хостовой машины.

Например:

```text
localhost:9092
localhost:9093
localhost:9094
```

---

# Kafka Bootstrap Servers

При локальном запуске Faust адреса передаются через:

```text
KAFKA_BROKER
```

Пример:

```powershell
$env:KAFKA_BROKER="kafka://kafka1:9092;kafka2:9093;kafka3:9094"
```

Адреса необходимо заменить на фактические DNS-имена или IP-адреса Kafka brokers.

Для Docker используется INTERNAL listener:

```text
kafka://kafka-broker-1:29092;kafka-broker-2:29092;kafka-broker-3:29092
```

---

# Количество Partitions

В текущем учебном варианте:

```python
TOPIC_PARTITIONS = 1
BANNED_WORDS_PARTITIONS = 1
```

Поэтому:

```text
messages           -> P0
filtered_messages  -> P0
blocked_users      -> P0
banned_words       -> P0
```

Использование одной partition упрощает демонстрацию stateful processing.

---

# Проверка Kafka Topics

Список топиков:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

Описание конкретного топика:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic banned_words
```

Для changelog:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic message-moderation-banned-words-table-changelog
```

При проверке необходимо обратить внимание на:

```text
PartitionCount
ReplicationFactor
Leader
Replicas
Isr
```

Для текущей конфигурации ожидается:

```text
PartitionCount:    1
ReplicationFactor: 2
```

и при нормальной работе обе replicas должны присутствовать в ISR.

---

# Kafka UI

Docker Compose включает Kafka UI.

После запуска полного Docker-окружения интерфейс доступен на порту:

```text
8080
```

Kafka UI позволяет просматривать:

- Topics;
- Partitions;
- Replicas;
- ISR;
- Messages;
- Consumers;
- настройки топиков.

---

# Итоговая Kafka-схема

```text
                   ZooKeeper
                       |
             +---------+---------+
             |         |         |
             v         v         v
          Broker 1  Broker 2  Broker 3
             |         |         |
             +---------+---------+
                       |
          +------------+------------+
          |            |            |
          v            v            v
       messages   blocked_users  banned_words
          |            |            |
          |            v            v
          |         Tables       Tables
          |            |            |
          |            v            v
          |        changelog     changelog
          |
          v
   filtered_messages
```