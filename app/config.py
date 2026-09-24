import os


APP_NAME = os.getenv(
    "APP_NAME",
    "message-moderation",
)

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "kafka://kafka1:9092;kafka2:9093;kafka3:9094"
)

LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "INFO",
)

# Kafka topics
MESSAGES_TOPIC = "messages"
FILTERED_MESSAGES_TOPIC = "filtered_messages"
BLOCKED_USERS_TOPIC = "blocked_users"
BANNED_WORDS_TOPIC = "banned_words"

# Faust tables
BLOCKED_USERS_TABLE = "blocked-users-table"
BANNED_WORDS_TABLE = "banned-words-table"

# Количество партиций
TOPIC_PARTITIONS = 1
BANNED_WORDS_PARTITIONS = 1