import faust


class Message(faust.Record, serializer="json"):
    """
    Сообщение пользователя.
    """
    sender_id: str
    recipient_id: str
    text: str


class BlockUserEvent(faust.Record, serializer="json"):
    """
    Событие блокировки или разблокировки пользователя

    action:
        block
        unblock
    """
    user_id: str
    blocked_user_id: str
    action: str


class BannedWordEvent(faust.Record, serializer="json"):
    """
    Событие изменения списка запрещённых слов

    action:
        add
        remove
    """
    word: str
    action: str