from enum import Enum, auto


class KeyType(Enum):
    HASH = auto()
    RANGE = auto()


class Key:
    def __init__(self, name: str, type: str, key_type: KeyType):
        self.name = name
        self.type = type
        self.key_type = key_type
