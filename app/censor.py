import re
from typing import Iterable

from app.logger import get_logger


logger = get_logger(__name__)


def censor_text(
    text: str,
    banned_words: Iterable[str],
) -> str:
    """
    Маскирует запрещённые слова символами '*'
    Поиск выполняется без учёта регистра.

    Примеры:
        Spam -> ****
        spam -> ****
        SPAM -> ****
    """

    result = text

    for word in banned_words:
        word = word.strip()

        if not word:
            continue

        pattern = re.compile(
            rf"\b{re.escape(word)}\b",
            re.IGNORECASE,
        )

        if pattern.search(result):
            logger.info(
                "Обнаружено и замаскировано запрещённое слово: %s",
                word,
            )

        result = pattern.sub(
            lambda match: "*" * len(match.group()),
            result,
        )

    return result