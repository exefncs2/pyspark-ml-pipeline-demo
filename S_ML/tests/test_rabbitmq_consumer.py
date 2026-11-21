"""RabbitMQ 消費端引擎單元測試。"""

import pytest

from base.rabbitmq_consumer import RabbitMQConsumerEngine


@pytest.fixture
def mock_rabbitmq(mocker):
    mock_connection = mocker.patch("pika.BlockingConnection")
    mock_channel = mocker.Mock()
    mock_connection.return_value.channel.return_value = mock_channel
    return mock_channel


def test_setup_declares_exchange_and_queue(mock_rabbitmq) -> None:
    consumer = RabbitMQConsumerEngine(
        exchange="test_exchange",
        exchange_type="direct",
        queue="test_queue",
        routing_key="test_key",
        callback=lambda _body: None,
    )
    mock_rabbitmq.exchange_declare.assert_called_once()
    mock_rabbitmq.queue_declare.assert_called_once_with(queue="test_queue", durable=True)
    mock_rabbitmq.queue_bind.assert_called_once_with(
        queue="test_queue",
        exchange="test_exchange",
        routing_key="test_key",
    )
    consumer.close()
