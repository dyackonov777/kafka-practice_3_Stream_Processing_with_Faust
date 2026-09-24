# Stateful Processing

## Stateful и Stateless Processing

Приложение совмещает два подхода:

```text
Stateless Processing
Stateful Processing
```

---

# Stateless Processing

Stateless processing не требует сохранённого состояния между отдельными событиями.

В проекте примером является функция:

```python
censor_text()
```

Функция получает:

```text
text
banned_words
```

и возвращает обработанный текст.

Сама функция не хранит состояние между вызовами.

---

# Stateful Processing

Stateful processing зависит от ранее полученных событий.

В проекте используются две Faust Tables:

```text
blocked_users_table
banned_words_table
```

Например, событие:

```json
{
  "user_id": "user_2",
  "blocked_user_id": "user_1",
  "action": "block"
}
```

изменяет состояние:

```text
user_2:user_1 -> True
```

Позднее приходит сообщение:

```json
{
  "sender_id": "user_1",
  "recipient_id": "user_2",
  "text": "Hello"
}
```

Для обработки приложение проверяет ранее сохранённое состояние:

```text
user_2:user_1
```

Таким образом, результат обработки текущего события зависит от предыдущего события.

---

# blocked_users_table

Table:

```python
blocked_users_table = app.Table(
    BLOCKED_USERS_TABLE,
    default=bool,
    partitions=TOPIC_PARTITIONS,
    options={
        "driver": "rocksdict",
        "max_open_files": 1024,
    },
)
```

Ключ имеет формат:

```text
user_id:blocked_user_id
```

Пример:

```text
user_2:user_1
```

Значения:

```text
True  -> блокировка активна
False -> блокировка не активна
```

---

# banned_words_table

Table:

```python
banned_words_table = app.Table(
    BANNED_WORDS_TABLE,
    default=bool,
    partitions=BANNED_WORDS_PARTITIONS,
    options={
        "driver": "rocksdict",
        "max_open_files": 1024,
    },
)
```

Пример:

```text
spam -> True
test -> True
word -> False
```

Активные слова получаются:

```python
banned_words = [
    word
    for word, enabled in banned_words_table.items()
    if enabled
]
```

Поэтому значения со статусом `False` исключаются из цензуры.

---

# RocksDB / RocksDict

Для persistent-хранения Tables приложение использует:

```python
store="rocksdb://"
```

Для Table задаётся RocksDict driver:

```python
options={
    "driver": "rocksdict",
    "max_open_files": 1024,
}
```

Общая схема:

```text
Faust Table
     |
     v
RocksDB / RocksDict
     |
     v
локальное состояние worker
```

---

# Kafka Changelog

Локальное состояние дополняется Kafka changelog.

При изменении Table:

```text
Agent
  |
  v
Table
  |
  +------> RocksDB
  |
  +------> Kafka changelog
```

Для `blocked_users_table`:

```text
message-moderation-blocked-users-table-changelog
```

Для `banned_words_table`:

```text
message-moderation-banned-words-table-changelog
```

Faust использует changelog для сохранения изменений состояния Table и его последующего восстановления. Faust 0.15.3 documentation описывает Tables как stateful key/value stores, а изменения состояния публикуются в changelog. 【2-5c33b6】

---

# Recovery

При запуске Faust выполняется восстановление состояния.

Схема:

```text
Worker start
     |
     v
открытие RocksDB
     |
     v
подключение changelog
     |
     v
Recovery
     |
     v
Restore complete
     |
     v
Worker ready
```

Пример успешного запуска:

```text
[INFO] [^---Recovery]: Recovery complete
[INFO] [^---Recovery]: Restore complete!
[INFO] [^---Recovery]: Seek stream partitions to committed offsets.
[INFO] [^---Recovery]: Worker ready
[INFO] [^Worker]: Ready
```

После `Worker: Ready` приложение готово к обработке новых событий.

---

# Co-partitioning

Для корректной работы stateful processing source topic и changelog соответствующей Table должны иметь согласованное количество partitions.

Для текущего проекта:

```text
blocked_users
P=1
     |
     v
blocked_users_table
P=1
     |
     v
blocked-users-table-changelog
P=1
```

Также:

```text
banned_words
P=1
     |
     v
banned_words_table
P=1
     |
     v
banned-words-table-changelog
P=1
```

Faust требует, чтобы changelog Table имел такое же количество partitions, как source topic, из которого Table изменяется. 【1-4e18e2】

---

# Почему используется одна Partition

В текущем учебном варианте:

```text
TOPIC_PARTITIONS = 1
```

Использование одной partition упрощает:

- демонстрацию состояния;
- обработку событий;
- согласование source topics и changelog;
- проверку Tables;
- анализ логов.

При увеличении количества partitions необходимо отдельно учитывать распределение состояния.

---

# Ограничения

Текущая реализация ориентирована на учебную демонстрацию.

При горизонтальном масштабировании нужно учитывать:

- увеличение количества Kafka partitions;
- выбор Kafka keys;
- co-partitioning;
- распределение Table;
- количество Faust workers;
- восстановление отдельных shards состояния;
- отказоустойчивость Kafka.

Faust использует sharding Tables и streams для распределённой stateful-обработки. 【3-ba0375】【1-4e18e2】

---

# Итоговая схема состояния

```text
Kafka Event
     |
     v
Faust Agent
     |
     v
Faust Table
     |
     +------------+
     |            |
     v            v
  RocksDB      Changelog
     |            |
     v            v
 local state   Kafka state
     |            |
     +------+-----+
            |
            v
         Recovery
```