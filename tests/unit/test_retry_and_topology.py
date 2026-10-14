from typing import Any

import pytest

from app.messaging.retry import ATTEMPT_HEADER, RetryPolicy, get_attempt
from app.messaging.topology import DELIVERY_LIMIT, build_topology
from tests.settings import make_settings


def policy(max_attempts: int = 3) -> RetryPolicy:
    return RetryPolicy(max_attempts=max_attempts, base_delay=2.0, multiplier=2.0)


def test_retry_policy_exponential_delays() -> None:
    assert [policy(4).delay_for(n) for n in (1, 2, 3)] == [2.0, 4.0, 8.0]
    assert policy(3).delays_ms() == [2000, 4000]


def test_retry_policy_attempt_budget() -> None:
    assert policy().should_retry(1)
    assert policy().should_retry(2)
    assert not policy().should_retry(3)


def test_single_attempt_policy_keeps_one_delay_for_deferrals() -> None:
    assert policy(1).delays_ms() == [2000]
    assert not policy(1).should_retry(1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0, "base_delay": 1.0, "multiplier": 2.0},
        {"max_attempts": 3, "base_delay": 0.0, "multiplier": 2.0},
        {"max_attempts": 3, "base_delay": 1.0, "multiplier": 0.5},
    ],
)
def test_retry_policy_validation(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="must be"):
        RetryPolicy(**kwargs)


@pytest.mark.parametrize(
    ("headers", "expected"),
    [(None, 1), ({}, 1), ({ATTEMPT_HEADER: 2}, 2), ({ATTEMPT_HEADER: "3"}, 3), ({ATTEMPT_HEADER: "junk"}, 1)],
)
def test_get_attempt(headers: dict[str, object] | None, expected: int) -> None:
    assert get_attempt(headers) == expected


def test_topology_layout() -> None:
    settings = make_settings(rabbitmq_exchange="payments", rabbitmq_queue="payments.new")
    topology = build_topology(settings, policy())

    assert topology.queue.name == topology.routing_key == "payments.new"
    assert topology.queue.arguments == {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.new.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": DELIVERY_LIMIT,
    }
    assert topology.dead_letter_exchange.name == "payments.dlx"
    assert topology.dead_letter_queue.name == topology.dead_letter_routing_key == "payments.new.dlq"
    assert topology.retry_exchange.name == "payments.retry"
    assert [q.name for q in topology.retry_queues] == ["payments.new.retry.2000ms", "payments.new.retry.4000ms"]
    assert topology.retry_queue_for(1).arguments == {
        "x-queue-type": "quorum",
        "x-message-ttl": 2000,
        "x-dead-letter-exchange": "payments",
        "x-dead-letter-routing-key": "payments.new",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }
    assert topology.retry_queue_for(2).name == "payments.new.retry.4000ms"
