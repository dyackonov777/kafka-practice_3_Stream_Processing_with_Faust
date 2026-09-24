# Kafka Stream Processing with Faust

Учебный проект потоковой обработки сообщений Apache Kafka с использованием Python и Faust Streaming.

Приложение получает пользовательские сообщения из Kafka, проверяет блокировки между пользователями, фильтрует запрещённые слова и отправляет результат в отдельный Kafka-топик.

## Возможности

- обработка Kafka-сообщений через Faust Agents;
- блокировка сообщений между пользователями;
- динамическое управление запрещёнными словами;
- цензура сообщений;
- Stateful Processing;
- Faust Tables;
- RocksDB / RocksDict;
- Kafka changelog;
- восстановление состояния после перезапуска;
- Kafka replication;
- Docker Compose;
- автоматическое тестирование.

## Архитектура

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

## Kafka Topics

Приложение использует:

```text
messages
filtered_messages
blocked_users
banned_words
```

Текущая учебная конфигурация:

```text
Partitions:          1
Replication Factor:  2
min.insync.replicas: 2
Kafka Brokers:       3
```

Для хранения состояния Faust создаёт changelog-топики:

```text
message-moderation-blocked-users-table-changelog
message-moderation-banned-words-table-changelog
```

## Структура проекта

```text
kafka-faust-moderation/
│
├── app/
│   ├── __init__.py
│   ├── censor.py
│   ├── config.py
│   ├── logger.py
│   ├── main.py
│   └── models.py
│
├── scripts/
│   └── test_data.py
│
├── docs/
│   ├── architecture.md
│   ├── kafka.md
│   ├── stateful-processing.md
│   └── testing.md
│   
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Установка

Создать виртуальное окружение:

```powershell
py -3.13 -m venv .venv
```

Активировать:

```powershell
.\.venv\Scripts\Activate.ps1
```

Установить зависимости:

```powershell
pip install -r requirements.txt
```

Основная зависимость проекта:

```text
faust-streaming[rocksdict]==0.15.3
```

## Настройка Kafka

Kafka brokers задаются переменной окружения:

```powershell
$env:KAFKA_BROKER="kafka://kafka1:9092;kafka2:9093;kafka3:9094"
```

Адреса необходимо заменить на фактические DNS-имена или IP-адреса Kafka brokers.

Проверка:

```powershell
$env:KAFKA_BROKER
```

## Запуск Faust

```powershell
python -m app.main worker -l info
```

Приложение готово после появления:

```text
[^Recovery]: Worker ready
[^Worker]: Ready
```

## Автоматическое тестирование

В отдельном PowerShell:

```powershell
python .\scripts\test_data.py
```

Тестовый сценарий проверяет:

1. добавление запрещённого слова;
2. цензуру сообщения;
3. блокировку пользователя;
4. блокирование сообщения;
5. прохождение сообщения от другого пользователя;
6. разблокировку пользователя;
7. прохождение сообщения после разблокировки;
8. удаление запрещённого слова;
9. сообщение после удаления запрещённого слова.

## Docker Compose

Полное окружение можно запустить:

```powershell
docker compose up -d --build
```

Проверить:

```powershell
docker compose ps
```

Логи Faust:

```powershell
docker compose logs -f faust
```

Остановить:

```powershell
docker compose down
```

# Логи работы приложения

Ниже приведены фактические логи запуска Faust-приложения и выполнения тестового сценария.

## Запуск Faust Worker

```text
2026-09-24 14:51:00,406 | INFO | __main__ | Запуск Faust-приложения: message-moderation
2026-09-24 14:51:00,406 | INFO | __main__ | Kafka broker: kafka://kafka1:9092;kafka2:9093;kafka3:9094
2026-09-24 14:51:00,407 | INFO | __main__ | Количество партиций пользовательских топиков: 1

[2026-09-24 14:51:00,495] [38596] [INFO] [^Worker]: Starting...
[2026-09-24 14:51:00,502] [38596] [INFO] [^-App]: Starting...
[2026-09-24 14:51:00,502] [38596] [INFO] [^--Monitor]: Starting...
[2026-09-24 14:51:00,502] [38596] [INFO] [^--Producer]: Starting...
[2026-09-24 14:51:00,519] [38596] [INFO] [^--Consumer]: Starting...
```

## Запуск Agents

```text
[2026-09-24 14:51:00,566] [38596] [INFO] [^---Agent: __main__.process_blocked_users]: Starting...
[2026-09-24 14:51:00,568] [38596] [INFO] [^---Agent: __main__.process_banned_words]: Starting...
[2026-09-24 14:51:00,569] [38596] [INFO] [^---Agent: __main__.process_messages]: Starting...
```

Запущены три потоковых обработчика:

```text
process_blocked_users
process_banned_words
process_messages
```

## Faust Tables и RocksDB

```text
[2026-09-24 14:51:01,573] [38596] [INFO] [^---Table: blocked-users-table]: Starting...
[2026-09-24 14:51:01,591] [38596] [INFO] [^----Store: rocksdb:blocked-users-table]: Starting...

[2026-09-24 14:51:01,622] [38596] [INFO] [^---Table: banned-words-table]: Starting...
[2026-09-24 14:51:01,624] [38596] [INFO] [^----Store: rocksdb:banned-words-table]: Starting...
```

Используются две stateful Tables:

```text
blocked-users-table
banned-words-table
```

## Kafka Changelog Topics

При запуске приложения были созданы changelog-топики для Faust Tables:

```text
[2026-09-24 14:51:01,591] [38596] [INFO] [^--Producer]: Creating topic 'message-moderation-blocked-users-table-changelog'
[2026-09-24 14:51:01,613] [38596] [INFO] [^--Producer]: Topic 'message-moderation-blocked-users-table-changelog' created.

[2026-09-24 14:51:01,624] [38596] [INFO] [^--Producer]: Creating topic 'message-moderation-banned-words-table-changelog'
[2026-09-24 14:51:01,639] [38596] [INFO] [^--Producer]: Topic 'message-moderation-banned-words-table-changelog' created.
```

## Назначение Kafka Partitions

Faust получил partition `0` используемых топиков:

```text
┌Topic Partition Set───────────────────────────────┬────────────┐
│ topic                                            │ partitions │
├──────────────────────────────────────────────────┼────────────┤
│ banned_words                                     │ {0}        │
│ blocked_users                                    │ {0}        │
│ message-moderation-__assignor-__leader           │ {0}        │
│ message-moderation-banned-words-table-changelog  │ {0}        │
│ message-moderation-blocked-users-table-changelog │ {0}        │
│ messages                                         │ {0}        │
└──────────────────────────────────────────────────┴────────────┘
```

Это соответствует текущей конфигурации:

```text
Partitions = 1
```

## Recovery Faust Tables

После подключения выполняется восстановление состояния Faust Tables:

```text
[2026-09-24 14:51:04,957] [38596] [INFO] [^---Recovery]: Resuming flow...
[2026-09-24 14:51:04,957] [38596] [INFO] [^---Recovery]: Recovery complete
[2026-09-24 14:51:04,957] [38596] [INFO] [^---Recovery]: Restore complete!
[2026-09-24 14:51:04,958] [38596] [INFO] [^---Recovery]: Seek stream partitions to committed offsets.
[2026-09-24 14:51:04,977] [38596] [INFO] [^---Fetcher]: Starting...
[2026-09-24 14:51:04,977] [38596] [INFO] [^---Recovery]: Worker ready
[2026-09-24 14:51:04,977] [38596] [INFO] [^Worker]: Ready
```

После:

```text
[^Worker]: Ready
```

приложение готово к потоковой обработке событий.

---

## Проверка цензуры

В список запрещённых слов было добавлено:

```text
spam
```

После этого приложение получило сообщение:

```text
2026-09-24 14:51:06,715 | INFO | __main__ | Получено сообщение | Отправитель=user_1 Получатель=user_2 Текст=This is spam message
2026-09-24 14:51:06,716 | INFO | app.censor | Обнаружено и замаскировано запрещённое слово: spam
2026-09-24 14:51:06,716 | INFO | __main__ | В сообщении обнаружены и замаскированы запрещённые слова | Отправитель=user_1 Получатель=user_2
2026-09-24 14:51:06,716 | INFO | __main__ | Сообщение успешно обработано и отправлено в топик | Топик=filtered_messages Отправитель=user_1 Получатель=user_2
```

Результат:

```text
This is spam message
        |
        v
This is **** message
```

Сообщение после цензуры отправлено в:

```text
filtered_messages
```

---

## Проверка блокировки пользователя

Пользователь `user_2` блокирует `user_1`:

```text
2026-09-24 14:51:08,732 | INFO | __main__ | Пользователь user_2 заблокировал пользователя user_1
```

В `blocked_users_table` формируется состояние:

```text
user_2:user_1 -> True
```

После этого приходит сообщение:

```text
2026-09-24 14:51:10,755 | INFO | __main__ | Получено сообщение | Отправитель=user_1 Получатель=user_2 Текст=This message must be blocked
2026-09-24 14:51:10,755 | WARNING | __main__ | Сообщение заблокировано | Отправитель=user_1 Получатель=user_2
```

Результат:

```text
user_1 -> user_2
      |
      v
user_2:user_1 = True
      |
      v
    BLOCKED
      |
      v
     DROP
```

Сообщение не отправляется в `filtered_messages`.

---

## Проверка сообщения другого пользователя

После блокировки `user_1` отправляется сообщение от `user_3`:

```text
2026-09-24 14:51:12,766 | INFO | __main__ | Получено сообщение | Отправитель=user_3 Получатель=user_2 Текст=Hello from user_3
2026-09-24 14:51:12,767 | INFO | __main__ | Сообщение успешно обработано и отправлено в топик | Топик=filtered_messages Отправитель=user_3 Получатель=user_2
```

Блокировка:

```text
user_2:user_1
```

не распространяется на:

```text
user_2:user_3
```

Поэтому сообщение успешно проходит обработку.

---

## Проверка разблокировки

Пользователь `user_2` разблокирует `user_1`:

```text
2026-09-24 14:51:14,777 | INFO | __main__ | Пользователь user_2 разблокировал пользователя user_1
```

Состояние изменяется:

```text
user_2:user_1 -> False
```

После разблокировки снова отправляется сообщение от `user_1`:

```text
2026-09-24 14:51:16,790 | INFO | __main__ | Получено сообщение | Отправитель=user_1 Получатель=user_2 Текст=Hello after unblock
2026-09-24 14:51:16,791 | INFO | __main__ | Сообщение успешно обработано и отправлено в топик | Топик=filtered_messages Отправитель=user_1 Получатель=user_2
```

Результат:

```text
user_1 -> user_2
      |
      v
user_2:user_1 = False
      |
      v
    ALLOWED
      |
      v
filtered_messages
```

---

## Проверка удаления запрещённого слова

Запрещённое слово `spam` удаляется из списка активных слов:

```text
2026-09-24 14:51:18,807 | INFO | __main__ | Запрещённое слово удалено из списка: spam
```

Состояние:

```text
spam -> False
```

Затем отправляется:

```text
2026-09-24 14:51:20,831 | INFO | __main__ | Получено сообщение | Отправитель=user_3 Получатель=user_2 Текст=This is spam message
2026-09-24 14:51:20,832 | INFO | __main__ | Сообщение успешно обработано и отправлено в топик | Топик=filtered_messages Отправитель=user_3 Получатель=user_2
```

В логе больше нет сообщения:

```text
Обнаружено и замаскировано запрещённое слово: spam
```

Результат остаётся без изменений:

```text
This is spam message
```

Это подтверждает динамическое отключение запрещённого слова без перезапуска Faust-приложения.

---

## Итоговый бизнес-лог

Ниже приведена последовательность основных событий тестового запуска без служебных startup-сообщений Faust:

```text
2026-09-24 14:51:06,715 | INFO | __main__ | Получено сообщение | Отправитель=user_1 Получатель=user_2 Текст=This is spam message
2026-09-24 14:51:06,716 | INFO | app.censor | Обнаружено и замаскировано запрещённое слово: spam
2026-09-24 14:51:06,716 | INFO | __main__ | В сообщении обнаружены и замаскированы запрещённые слова | Отправитель=user_1 Получатель=user_2
2026-09-24 14:51:06,716 | INFO | __main__ | Сообщение успешно обработано и отправлено в топик | Топик=filtered_messages Отправитель=user_1 Получатель=user_2

2026-09-24 14:51:08,732 | INFO | __main__ | Пользователь user_2 заблокировал пользователя user_1

2026-09-24 14:51:10,755 | INFO | __main__ | Получено сообщение | Отправитель=user_1 Получатель=user_2 Текст=This message must be blocked
2026-09-24 14:51:10,755 | WARNING | __main__ | Сообщение заблокировано | Отправитель=user_1 Получатель=user_2

2026-09-24 14:51:12,766 | INFO | __main__ | Получено сообщение | Отправитель=user_3 Получатель=user_2 Текст=Hello from user_3
2026-09-24 14:51:12,767 | INFO | __main__ | Сообщение успешно обработано и отправлено в топик | Топик=filtered_messages Отправитель=user_3 Получатель=user_2

2026-09-24 14:51:14,777 | INFO | __main__ | Пользователь user_2 разблокировал пользователя user_1

2026-09-24 14:51:16,790 | INFO | __main__ | Получено сообщение | Отправитель=user_1 Получатель=user_2 Текст=Hello after unblock
2026-09-24 14:51:16,791 | INFO | __main__ | Сообщение успешно обработано и отправлено в топик | Топик=filtered_messages Отправитель=user_1 Получатель=user_2

2026-09-24 14:51:18,807 | INFO | __main__ | Запрещённое слово удалено из списка: spam

2026-09-24 14:51:20,831 | INFO | __main__ | Получено сообщение | Отправитель=user_3 Получатель=user_2 Текст=This is spam message
2026-09-24 14:51:20,832 | INFO | __main__ | Сообщение успешно обработано и отправлено в топик | Топик=filtered_messages Отправитель=user_3 Получатель=user_2
```

## Итог проверки

Фактический тестовый запуск подтверждает:

```text
Добавление spam
      |
      v
spam -> True
      |
      v
Цензура работает
      |
      v
Block user_1
      |
      v
Сообщение user_1 блокируется
      |
      v
Сообщение user_3 проходит
      |
      v
Unblock user_1
      |
      v
Сообщение user_1 снова проходит
      |
      v
Remove spam
      |
      v
spam -> False
      |
      v
spam больше не цензурируется
```

## Документация

Подробная документация проекта разделена по разделам:

- docs/architecture.md
- docs/kafka.md
- docs/stateful-processing.md
- docs/testing.md

## Результат

Проект демонстрирует полный цикл Stateful Stream Processing:

```text
Kafka Topics
     |
     v
Faust Agents
     |
     +------> Faust Tables
  
