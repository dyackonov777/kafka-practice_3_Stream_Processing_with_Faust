# Тестирование приложения

## Автоматическое тестирование

Для создания тестовых событий используется:

```text
scripts/test_data.py
```

Тестовый скрипт подключается непосредственно к Kafka и последовательно отправляет события, необходимые для проверки бизнес-логики приложения.

Kafka CLI для запуска тестов не требуется.

---

# Подготовка

Перед запуском тестов необходимо запустить Faust worker.

В PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Указать Kafka brokers:

```powershell
$env:KAFKA_BROKER="kafka://kafka1:9092;kafka2:9093;kafka3:9094"
```

Запустить приложение:

```powershell
python -m app.main worker -l info
```

Необходимо дождаться:

```text
[^Recovery]: Worker ready
[^Worker]: Ready
```

---

# Запуск тестов

В отдельном PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Запуск:

```powershell
python .\scripts\test_data.py
```

---

# Тест 1. Добавление запрещённого слова

В:

```text
banned_words
```

отправляется:

```json
{
  "word": "spam",
  "action": "add"
}
```

Ожидаемое состояние:

```text
spam -> True
```

В логе:

```text
Добавлено запрещённое слово: spam
```

---

# Тест 2. Цензура сообщения

В `messages`:

```json
{
  "sender_id": "user_1",
  "recipient_id": "user_2",
  "text": "This is spam message"
}
```

Поскольку:

```text
spam -> True
```

ожидается цензура:

```text
This is **** message
```

В логе:

```text
Получено сообщение |
Отправитель=user_1
Получатель=user_2
Текст=This is spam message

Обнаружено и замаскировано запрещённое слово: spam

В сообщении обнаружены и замаскированы запрещённые слова
```

В `filtered_messages` должно появиться:

```json
{
  "sender_id": "user_1",
  "recipient_id": "user_2",
  "text": "This is **** message"
}
```

---

# Тест 3. Блокировка пользователя

В `blocked_users`:

```json
{
  "user_id": "user_2",
  "blocked_user_id": "user_1",
  "action": "block"
}
```

Ожидается:

```text
user_2:user_1 -> True
```

Лог:

```text
Пользователь user_2 заблокировал пользователя user_1
```

---

# Тест 4. Сообщение заблокированного пользователя

Отправляется:

```json
{
  "sender_id": "user_1",
  "recipient_id": "user_2",
  "text": "This message must be blocked"
}
```

При обработке формируется:

```text
recipient_id:sender_id
```

то есть:

```text
user_2:user_1
```

В Table:

```text
user_2:user_1 -> True
```

Результат:

```text
Message
   |
   v
BLOCKED
   |
   v
DROP
```

В логе:

```text
Сообщение заблокировано |
Отправитель=user_1
Получатель=user_2
```

Сообщение не должно попасть в:

```text
filtered_messages
```

---

# Тест 5. Другой пользователь

Отправляется:

```json
{
  "sender_id": "user_3",
  "recipient_id": "user_2",
  "text": "Hello from user_3"
}
```

Активна блокировка:

```text
user_2:user_1
```

но не:

```text
user_2:user_3
```

Поэтому сообщение должно попасть в:

```text
filtered_messages
```

Ожидаемый лог:

```text
Получено сообщение |
Отправитель=user_3
Получатель=user_2
Текст=Hello from user_3

Сообщение успешно обработано и отправлено в топик |
Топик=filtered_messages
```

---

# Тест 6. Разблокировка

Отправляется:

```json
{
  "user_id": "user_2",
  "blocked_user_id": "user_1",
  "action": "unblock"
}
```

Ожидается:

```text
user_2:user_1 -> False
```

В логе:

```text
Пользователь user_2 разблокировал пользователя user_1
```

---

# Тест 7. Сообщение после разблокировки

Отправляется:

```json
{
  "sender_id": "user_1",
  "recipient_id": "user_2",
  "text": "Hello after unblock"
}
```

Поскольку:

```text
user_2:user_1 -> False
```

сообщение должно пройти обработку.

Ожидаемый лог:

```text
Получено сообщение |
Отправитель=user_1
Получатель=user_2
Текст=Hello after unblock

Сообщение успешно обработано и отправлено в топик |
Топик=filtered_messages
```

---

# Тест 8. Удаление запрещённого слова

В `banned_words`:

```json
{
  "word": "spam",
  "action": "remove"
}
```

Ожидаемое состояние:

```text
spam -> False
```

Лог:

```text
Запрещённое слово удалено из списка: spam
```

---

# Тест 9. Проверка удаления запрещённого слова

Отправляется:

```json
{
  "sender_id": "user_3",
  "recipient_id": "user_2",
  "text": "This is spam message"
}
```

Поскольку:

```text
spam -> False
```

слово больше не участвует в цензуре.

Ожидаемый результат:

```text
This is spam message
```

В `filtered_messages`:

```json
{
  "sender_id": "user_3",
  "recipient_id": "user_2",
  "text": "This is spam message"
}
```

---

# Чистый сценарий тестирования

```text
START
  |
  v
add spam
  |
  v
spam -> True
  |
  v
send "This is spam message"
  |
  v
"This is **** message"
  |
  v
block user_1
  |
  v
send user_1 -> user_2
  |
  v
DROP
  |
  v
send user_3 -> user_2
  |
  v
ALLOWED
  |
  v
unblock user_1
  |
  v
send user_1 -> user_2
  |
  v
ALLOWED
  |
  v
remove spam
  |
  v
spam -> False
  |
  v
send "This is spam message"
  |
  v
"This is spam message"
  |
  v
END
```

---

# Проверка filtered_messages

Результирующие сообщения необходимо проверить в:

```text
filtered_messages
```

При использовании Kafka UI:

```text
Topics
    |
    v
filtered_messages
    |
    v
Messages
```

После полного теста в `filtered_messages` должны присутствовать:

```text
цензурированное сообщение
сообщение user_3
сообщение после unblock
сообщение после удаления spam
```

Сообщение, отправленное `user_1` во время активной блокировки, присутствовать не должно.

---

# Что считается успешным результатом

Тестирование считается успешным, если выполняются все условия:

```text
[OK] add banned word
[OK] censorship
[OK] block
[OK] blocked message dropped
[OK] another sender allowed
[OK] unblock
[OK] message allowed after unblock
[OK] remove banned word
[OK] removed word is no longer censored
```