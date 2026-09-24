import asyncio
import json
import os
from typing import Any, Optional

from aiokafka import AIOKafkaProducer


# Kafka topics
BANNED_WORDS_TOPIC = "banned_words"
BLOCKED_USERS_TOPIC = "blocked_users"
MESSAGES_TOPIC = "messages"

# Пауза между связанными тестами.
# Нужна, чтобы Faust успел обработать событие
# и обновить Table перед следующим тестом.
TEST_DELAY_SECONDS = float(
    os.getenv("TEST_DELAY_SECONDS", "2")
)


def get_bootstrap_servers() -> list[str]:
    """
    Получает адреса Kafka-брокеров из переменной окружения KAFKA_BROKER.

    Формат Faust:
    kafka://host1:9093;host3:9094
    """

    value = os.getenv(
        "KAFKA_BROKER",
        "kafka://kafka1:9092;kafka2:9093;kafka3:9094",
    )

    # Удаляем транспорт Faust
    value = value.removeprefix("kafka://")
    value = value.removeprefix("aiokafka://")

    # Преобразуем строку Faust в список адресов для aiokafka
    servers = [
        server.strip()
        for server in value.split(";")
        if server.strip()
    ]

    if not servers:
        raise ValueError(
            "KAFKA_BROKER не содержит адресов Kafka-брокеров"
        )

    return servers


def encode_json(value: dict[str, Any]) -> bytes:
    """
    Сериализация JSON в UTF-8.
    """

    return json.dumps(
        value,
        ensure_ascii=False,
    ).encode("utf-8")


async def send_event(
    producer: AIOKafkaProducer,
    topic: str,
    value: dict[str, Any],
    key: Optional[str] = None,
) -> None:
    """
    Отправляет событие в Kafka и выводит
    информацию о partition и offset.
    """

    metadata = await producer.send_and_wait(
        topic,
        value=encode_json(value),
        key=key.encode("utf-8") if key is not None else None,
    )

    print(
        f"OK: topic={metadata.topic}, "
        f"partition={metadata.partition}, "
        f"offset={metadata.offset}, "
        f"key={key!r}"
    )

    print(
        f"    value="
        f"{json.dumps(value, ensure_ascii=False)}"
    )


async def pause() -> None:
    """
    Пауза между зависимыми тестами.
    """

    await asyncio.sleep(TEST_DELAY_SECONDS)


async def main() -> None:

    bootstrap_servers = get_bootstrap_servers()

    print("=" * 70)
    print("Тестирование Kafka / Faust")
    print("=" * 70)

    print(
        "Kafka brokers:",
        ", ".join(bootstrap_servers),
    )

    print(
        "Перед запуском тестов Faust worker "
        "должен быть запущен."
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,

        # Ждём подтверждения записи
        # от всех необходимых Kafka-реплик.
        acks="all",

        client_id="message-moderation-test-data",
    )

    await producer.start()

    try:

        # =====================================================
        # ТЕСТ 1
        # Добавление запрещённого слова
        # =====================================================

        print()
        print("=" * 70)
        print("ТЕСТ 1 - Добавление запрещённого слова")
        print("=" * 70)

        await send_event(
            producer,
            BANNED_WORDS_TOPIC,
            {
                "word": "spam",
                "action": "add",
            },
        )

        await pause()


        # =====================================================
        # ТЕСТ 2
        # Проверка цензуры
        # =====================================================

        print()
        print("=" * 70)
        print("ТЕСТ 2 - Проверка цензуры сообщения")
        print("=" * 70)

        print(
            "ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:"
        )

        print(
            "В filtered_messages должен появиться текст:"
        )

        print(
            "This is **** message"
        )

        await send_event(
            producer,
            MESSAGES_TOPIC,
            {
                "sender_id": "user_1",
                "recipient_id": "user_2",
                "text": "This is spam message",
            },

            # Kafka key = получатель сообщения
            key="user_2",
        )

        await pause()


        # =====================================================
        # ТЕСТ 3
        # Блокировка пользователя
        # =====================================================

        print()
        print("=" * 70)
        print("ТЕСТ 3 - Блокировка пользователя")
        print("=" * 70)

        print(
            "user_2 блокирует user_1"
        )

        await send_event(
            producer,
            BLOCKED_USERS_TOPIC,
            {
                "user_id": "user_2",
                "blocked_user_id": "user_1",
                "action": "block",
            },

            # Используем тот же логический ключ,
            # что и для сообщений получателя.
            key="user_2",
        )

        await pause()


        # =====================================================
        # ТЕСТ 4
        # Сообщение заблокированного пользователя
        # =====================================================

        print()
        print("=" * 70)
        print(
            "ТЕСТ 4 - Сообщение "
            "заблокированного пользователя"
        )
        print("=" * 70)

        print(
            "ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:"
        )

        print(
            "Сообщение НЕ должно попасть "
            "в filtered_messages"
        )

        await send_event(
            producer,
            MESSAGES_TOPIC,
            {
                "sender_id": "user_1",
                "recipient_id": "user_2",
                "text": "This message must be blocked",
            },
            key="user_2",
        )

        await pause()


        # =====================================================
        # ТЕСТ 5
        # Другой пользователь не заблокирован
        # =====================================================

        print()
        print("=" * 70)
        print(
            "ТЕСТ 5 - Сообщение "
            "от другого пользователя"
        )
        print("=" * 70)

        print(
            "user_3 пишет user_2"
        )

        print(
            "ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:"
        )

        print(
            "Сообщение должно попасть "
            "в filtered_messages"
        )

        await send_event(
            producer,
            MESSAGES_TOPIC,
            {
                "sender_id": "user_3",
                "recipient_id": "user_2",
                "text": "Hello from user_3",
            },
            key="user_2",
        )

        await pause()


        # =====================================================
        # ТЕСТ 6
        # Разблокировка пользователя
        # =====================================================

        print()
        print("=" * 70)
        print("ТЕСТ 6 - Разблокировка пользователя")
        print("=" * 70)

        print(
            "user_2 разблокирует user_1"
        )

        await send_event(
            producer,
            BLOCKED_USERS_TOPIC,
            {
                "user_id": "user_2",
                "blocked_user_id": "user_1",
                "action": "unblock",
            },
            key="user_2",
        )

        await pause()


        # =====================================================
        # ТЕСТ 7
        # Сообщение после разблокировки
        # =====================================================

        print()
        print("=" * 70)
        print(
            "ТЕСТ 7 - Сообщение "
            "после разблокировки"
        )
        print("=" * 70)

        print(
            "ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:"
        )

        print(
            "Сообщение должно попасть "
            "в filtered_messages"
        )

        await send_event(
            producer,
            MESSAGES_TOPIC,
            {
                "sender_id": "user_1",
                "recipient_id": "user_2",
                "text": "Hello after unblock",
            },
            key="user_2",
        )

        await pause()


        # =====================================================
        # ТЕСТ 8
        # Удаление запрещённого слова
        # =====================================================

        print()
        print("=" * 70)
        print(
            "ТЕСТ 8 - Удаление "
            "запрещённого слова"
        )
        print("=" * 70)

        await send_event(
            producer,
            BANNED_WORDS_TOPIC,
            {
                "word": "spam",
                "action": "remove",
            },
        )

        await pause()


        # =====================================================
        # ТЕСТ 9
        # Проверка динамического удаления слова
        # =====================================================

        print()
        print("=" * 70)
        print(
            "ТЕСТ 9 - Проверка удаления "
            "запрещённого слова"
        )
        print("=" * 70)

        print(
            "ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:"
        )

        print(
            "spam больше не должен "
            "заменяться на ****"
        )

        await send_event(
            producer,
            MESSAGES_TOPIC,
            {
                "sender_id": "user_3",
                "recipient_id": "user_2",
                "text": "This is spam message",
            },
            key="user_2",
        )

        await pause()


        # =====================================================
        # ТЕСТ 10
        # Проверка Kafka keys и partitions
        # =====================================================

        print()
        print("=" * 70)
        print(
            "ТЕСТ 10 - Проверка "
            "Kafka key / partitions"
        )
        print("=" * 70)

        print(
            "Отправляем сообщения "
            "разным получателям."
        )

        print(
            "В выводе будет показана partition, "
            "в которую Kafka записала сообщение."
        )

        recipients = (
            "user_2",
            "user_5",
            "user_10",
        )

        for recipient_id in recipients:

            await send_event(
                producer,
                MESSAGES_TOPIC,
                {
                    "sender_id": "user_test",
                    "recipient_id": recipient_id,
                    "text": (
                        f"Partition test "
                        f"for {recipient_id}"
                    ),
                },

                # recipient_id используется
                # как Kafka key
                key=recipient_id,
            )


        print()
        print("=" * 70)
        print(
            "Все тестовые события "
            "успешно отправлены"
        )
        print("=" * 70)

        print()
        print(
            "Проверь:"
        )

        print(
            "1. Логи Faust"
        )

        print(
            "2. Топик filtered_messages"
        )

        print(
            "3. Kafka partitions и keys"
        )


    finally:

        # Корректно закрываем Kafka producer
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())