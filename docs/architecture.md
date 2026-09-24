# Архитектура приложения

## Назначение

Приложение реализует потоковую обработку пользовательских сообщений с использованием Apache Kafka и Faust Streaming.

Основные функции:

- получение сообщений из Kafka;
- проверка блокировок между пользователями;
- динамическое управление запрещёнными словами;
- цензура сообщений;
- отправка обработанных сообщений в отдельный Kafka-топик;
- хранение состояния в Faust Tables;
- логирование операций.

---

## Общая архитектура

```text
                           Kafka
                             |
             +---------------+---------------+
             |               |               |
             v               v               v
         messages      blocked_users     banned_words
             |               |               |
             |               v               v
             |        blocked_users     banned_words
             |            table             table
             |               |               |
             +---------------+---------------+
                             |
                             v
                      process_messages
                             |
                    +--------+--------+
                    |                 |
                 BLOCKED           ALLOWED
                    |                 |
                    v                 v
                   DROP          censor_text()
                                      |
                                      v
                              filtered_messages
```

---

# Компоненты приложения

Исходный код приложения находится в:

```text
app/
├── __init__.py
├── censor.py
├── config.py
├── logger.py
├── main.py
└── models.py
```

## `main.py`

Основной модуль приложения.

Содержит:

- создание `faust.App`;
- описание Kafka Topics;
- создание Faust Tables;
- Agent обработки блокировок;
- Agent обработки запрещённых слов;
- Agent обработки сообщений.

---

## `config.py`

Содержит конфигурацию приложения:

```text
APP_NAME
KAFKA_BROKER
LOG_LEVEL
TOPIC_PARTITIONS
BANNED_WORDS_PARTITIONS
TOPIC_REPLICATION_FACTOR
```

Также содержит имена Kafka Topics и Faust Tables.

---

## `models.py`

Содержит модели Kafka-сообщений:

```text
Message
BlockUserEvent
BannedWordEvent
```

Модели реализованы через:

```python
faust.Record
```

и используют JSON-сериализацию.

---

## `censor.py`

Содержит алгоритм цензуры текста:

```python
censor_text()
```

Алгоритм получает:

```text
текст
+
список активных запрещённых слов
```

и возвращает обработанный текст.

---

## `logger.py`

Содержит общую настройку логирования приложения.

Формат:

```text
timestamp | level | module | message
```

---

# Модели данных

## Message

Представляет пользовательское сообщение:

```python
class Message(faust.Record, serializer="json"):
    sender_id: str
    recipient_id: str
    text: str
```

Пример:

```json
{
  "sender_id": "user_1",
  "recipient_id": "user_2",
  "text": "Hello world"
}
```

---

## BlockUserEvent

Представляет изменение блокировки:

```python
class BlockUserEvent(faust.Record, serializer="json"):
    user_id: str
    blocked_user_id: str
    action: str
```

Поддерживаются:

```text
block
unblock
```

Пример:

```json
{
  "user_id": "user_2",
  "blocked_user_id": "user_1",
  "action": "block"
}
```

---

## BannedWordEvent

Представляет изменение списка запрещённых слов:

```python
class BannedWordEvent(faust.Record, serializer="json"):
    word: str
    action: str
```

Поддерживаются:

```text
add
remove
```

Пример:

```json
{
  "word": "spam",
  "action": "add"
}
```

---

# Faust Application

Приложение создаётся через:

```python
app = faust.App(
    APP_NAME,
    broker=KAFKA_BROKER,
    store="rocksdb://",
    topic_partitions=TOPIC_PARTITIONS,
    topic_replication_factor=TOPIC_REPLICATION_FACTOR,
)
```

Для локального состояния используется RocksDB.

---

# Kafka Topics приложения

Приложение работает с четырьмя пользовательскими топиками:

```text
messages
filtered_messages
blocked_users
banned_words
```

Назначение:

```text
messages
    входящие сообщения

filtered_messages
    обработанные сообщения

blocked_users
    события block/unblock

banned_words
    события add/remove
```

Подробная Kafka-конфигурация описана в:

```text
docs/kafka.md
```

---

# Agent process_blocked_users

Agent получает события из:

```text
blocked_users
```

и изменяет:

```text
blocked_users_table
```

Алгоритм:

```text
BlockUserEvent
       |
       v
проверка user_id
и blocked_user_id
       |
       v
формирование ключа
       |
       v
user_id:blocked_user_id
       |
       +-------------+
       |             |
       v             v
     block         unblock
       |             |
       v             v
      True          False
       |             |
       +------+------+
              |
              v
    blocked_users_table
```

Например:

```json
{
  "user_id": "user_2",
  "blocked_user_id": "user_1",
  "action": "block"
}
```

создаёт состояние:

```text
user_2:user_1 -> True
```

При `unblock`:

```text
user_2:user_1 -> False
```

---

# Agent process_banned_words

Agent получает события:

```text
banned_words
```

и изменяет:

```text
banned_words_table
```

Алгоритм:

```text
BannedWordEvent
       |
       v
нормализация word
       |
       v
проверка action
       |
   +---+----+
   |        |
   v        v
  add     remove
   |        |
   v        v
 True     False
   |        |
   +---+----+
       |
       v
banned_words_table
```

Пример:

```json
{
  "word": "spam",
  "action": "add"
}
```

приводит к:

```text
spam -> True
```

После:

```json
{
  "word": "spam",
  "action": "remove"
}
```

состояние:

```text
spam -> False
```

---

# Agent process_messages

Это основной обработчик пользовательских сообщений.

Алгоритм:

```text
Message
   |
   v
получение sender_id
и recipient_id
   |
   v
формирование ключа блокировки
recipient_id:sender_id
   |
   v
blocked_users_table
   |
   +---------- True ----------> DROP
   |
   |
  False
   |
   v
получение активных
запрещённых слов
   |
   v
censor_text()
   |
   v
создание нового Message
   |
   v
filtered_messages
```

Для сообщения:

```json
{
  "sender_id": "user_1",
  "recipient_id": "user_2",
  "text": "Hello"
}
```

формируется ключ:

```text
user_2:user_1
```

Если:

```text
user_2:user_1 -> True
```

сообщение блокируется.

Если блокировки нет, выполняется цензура.

---

# Цензура сообщений

Цензура вынесена в:

```text
app/censor.py
```

Функция:

```python
censor_text(
    text,
    banned_words,
)
```

ищет запрещённые слова без учёта регистра.

Например:

```text
spam -> ****
Spam -> ****
SPAM -> ****
```

Используется:

```python
re.escape(word)
```

что позволяет безопасно использовать содержимое слова в регулярном выражении.

Замена:

```python
lambda match: "*" * len(match.group())
```

сохраняет длину найденного слова.

Пример:

```text
This is spam message
```

результат:

```text
This is **** message
```

---

# Логирование

Основные события приложения записываются в лог:

```text
Запуск Faust-приложения
Получено сообщение
Добавлено запрещённое слово
Запрещённое слово удалено
Пользователь заблокирован
Пользователь разблокирован
Сообщение заблокировано
Обнаружено запрещённое слово
Сообщение успешно обработано
```

Также предусмотрены warning-сообщения для некорректных входных событий.

---

# Основной Data Flow

Итоговая последовательность обработки:

```text
Producer
   |
   v
messages
   |
   v
Faust
   |
   v
проверка блокировки
   |
   +---- BLOCKED ----> DROP
   |
   v
получение banned_words
   |
   v
censor_text()
   |
   v
Message
   |
   v
filtered_messages
   |
   v
Consumer
```