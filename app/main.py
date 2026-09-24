import faust

from app.censor import censor_text
from app.config import (
    APP_NAME,
    BANNED_WORDS_PARTITIONS,
    BANNED_WORDS_TABLE,
    BANNED_WORDS_TOPIC,
    BLOCKED_USERS_TABLE,
    BLOCKED_USERS_TOPIC,
    FILTERED_MESSAGES_TOPIC,
    KAFKA_BROKER,
    MESSAGES_TOPIC,
    TOPIC_PARTITIONS,
)
from app.logger import get_logger
from app.models import (
    BannedWordEvent,
    BlockUserEvent,
    Message,
)


logger = get_logger(__name__)


# Faust app
app = faust.App(
    APP_NAME,
    broker=KAFKA_BROKER,
    store="rocksdb://",
    topic_partitions=TOPIC_PARTITIONS,
    topic_replication_factor=2,
)

# Kafka topics
messages_topic = app.topic(
    MESSAGES_TOPIC,
    key_type=str,
    value_type=Message,
    partitions=TOPIC_PARTITIONS,
)

filtered_messages_topic = app.topic(
    FILTERED_MESSAGES_TOPIC,
    key_type=str,
    value_type=Message,
    partitions=TOPIC_PARTITIONS,
)

blocked_users_topic = app.topic(
    BLOCKED_USERS_TOPIC,
    key_type=str,
    value_type=BlockUserEvent,
    partitions=TOPIC_PARTITIONS,
)

banned_words_topic = app.topic(
    BANNED_WORDS_TOPIC,
    key_type=str,
    value_type=BannedWordEvent,
    partitions=BANNED_WORDS_PARTITIONS,
)

# Faust table
# Хранит информацию о блокировках
blocked_users_table = app.Table(
    BLOCKED_USERS_TABLE,
    default=bool,
    partitions=TOPIC_PARTITIONS,
    options={
        "driver": "rocksdict",
        "max_open_files": 1024,
    },
)

# Запрещенные слова
# Spam -> True
# Test -> False
# False означает, что слово было удалено из списка
banned_words_table = app.Table(
    BANNED_WORDS_TABLE,
    default=bool,
    partitions=BANNED_WORDS_PARTITIONS,
    options={
        "driver": "rocksdict",
        "max_open_files": 1024,
    },
)

# Блокировки
@app.agent(blocked_users_topic)
async def process_blocked_users(events):
    """
    Обработка потока событий блокировки
    block - заблокировать юзера
    unblock - разблокировать юзера
    """

    async for event in events:

        user_id = event.user_id.strip()
        blocked_user_id = event.blocked_user_id.strip()
        action = event.action.lower().strip()

        if not user_id or not blocked_user_id:
            logger.warning(
                "Получено некорректное событие блокировки: пустой ID пользователя"
            )
            continue

        # Формирование укального ключа для Table
        table_key = f"{user_id}:{blocked_user_id}"

        if action == "block":

            blocked_users_table[table_key] = True

            logger.info(
                "Пользователь %s заблокировал пользователя %s",
                user_id,
                blocked_user_id,
            )

        elif action == "unblock":

            blocked_users_table[table_key] = False

            logger.info(
                "Пользователь %s разблокировал пользователя %s",
                user_id,
                blocked_user_id,
            )

        else:
            logger.warning(
                "Получено неизвестное действие для списка блокировок: %s",
                event.action,
            )

# Запрещённые слова

@app.agent(banned_words_topic)
async def process_banned_words(events):
    """
    Динамически обновляет список запрещённых слов
    add - добавить слово
    remove - удалить слово
    """

    async for event in events:

        word = event.word.lower().strip()

        action = event.action.lower().strip()

        if not word:

            logger.warning(
                "Получено пустое запрещённое слово"
            )
            continue

        if action == "add":

            banned_words_table[word] = True

            logger.info(
                "Добавлено запрещённое слово: %s",
                word,
            )

        elif action == "remove":

            banned_words_table[word] = False

            logger.info(
                "Запрещённое слово удалено из списка: %s",
                word,
            )

        else:

            logger.warning(
                "Получено неизвестное действие для запрещённого слова: %s",
                event.action,
            )

# Основная обработка сообщений
@app.agent(messages_topic)
async def process_messages(messages):
    """
    Обрабатывает входящие сообщения
        Последовательность:
            1. Получение сообщения
            2. Проверка блокировки
            3. Получение запрещенных слов
            4. Цензура сообщения
            5. Отправка результата в filtered_messages
    """
    async for message in messages:

        logger.info(
            "Получено сообщение | Отправитель=%s Получатель=%s Текст=%s",
            message.sender_id,
            message.recipient_id,
            message.text,
        )

        # Проверка блокировки
        block_key = (
            f"{message.recipient_id}:"
            f"{message.sender_id}"
        )

        is_blocked = blocked_users_table[block_key]

        if is_blocked:

            logger.warning(
                "Сообщение заблокировано | Отправитель=%s Получатель=%s",
                message.sender_id,
                message.recipient_id,
            )

            continue

        # Получаем активные запрещенные слова
        banned_words = [
            word
            for word, enabled in banned_words_table.items()
            if enabled
        ]

        # Цензура
        filtered_text = censor_text(
            message.text,
            banned_words,
        )

        if filtered_text != message.text:

            logger.info(
                "В сообщении обнаружены и замаскированы запрещённые слова | Отправитель=%s Получатель=%s",
                message.sender_id,
                message.recipient_id,
            )

        # Создаем обработанное сообщение
        filtered_message = Message(
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            text=filtered_text,
        )

        # Отправляем результат
        # Сообщения одного получателя будут иметь одинаковый ключ партиционирования
        await filtered_messages_topic.send(
            key=message.recipient_id,
            value=filtered_message,
        )

        logger.info(
            "Сообщение успешно обработано и отправлено в топик | Топик=%s Отправитель=%s Получатель=%s",
            FILTERED_MESSAGES_TOPIC,
            message.sender_id,
            message.recipient_id,
        )

# Запуск приложения
if __name__ == "__main__":

    logger.info(
        "Запуск Faust-приложения: %s",
        APP_NAME,
    )

    logger.info(
        "Kafka broker: %s",
        KAFKA_BROKER,
    )
    logger.info(
        "Количество партиций пользовательских топиков: %s",
        TOPIC_PARTITIONS,
    )
    app.main()