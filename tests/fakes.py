from __future__ import annotations

from typing import Any


class FakeMongoCollection:
    def __init__(self) -> None:
        self.docs: list[dict[str, Any]] = []
        self.indexes: list[list[tuple[str, int]]] = []

    def count_documents(self, query: dict[str, Any]) -> int:
        if not query:
            return len(self.docs)
        return sum(1 for d in self.docs if all(d.get(k) == v for k, v in query.items()))

    def insert_one(self, doc: dict[str, Any]) -> None:
        self.docs.append(dict(doc))

    def insert_many(self, docs: list[dict[str, Any]]) -> None:
        self.docs.extend(dict(d) for d in docs)

    def create_index(self, keys: list[tuple[str, int]]) -> None:
        self.indexes.append(keys)


class FakeMqttClient:
    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    def publish(self, topic: str, payload: str, qos: int = 0) -> None:
        self.published.append((topic, payload))


class FakeKafkaProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, dict[str, Any]]] = []

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.messages.append((topic, key, value))
