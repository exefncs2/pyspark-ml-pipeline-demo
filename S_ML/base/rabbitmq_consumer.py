"""RabbitMQ 消費端與連線管理模組。"""

from __future__ import annotations

import functools
import logging
import threading
from typing import Callable, Optional

import pika

try:
    from S_ML.config import RabbitMQConfig
except ModuleNotFoundError:
    from config import RabbitMQConfig

logger = logging.getLogger(__name__)


class RabbitMQConsumerEngine:
    """管理 RabbitMQ Exchange／Queue 宣告、訊息接收與多執行緒工作派發。"""

    def __init__(
        self,
        exchange: str,
        exchange_type: str,
        queue: str,
        routing_key: str,
        callback: Optional[Callable[[bytes], None]] = None,
        heartbeat: int = 10,
        prefetch_count: int = 1,
        durable: bool = True,
        auto_delete: bool = False,
        config: Optional[RabbitMQConfig] = None,
    ) -> None:
        resolved = config or RabbitMQConfig.from_env()
        credentials = pika.PlainCredentials(resolved.user, resolved.password)
        parameters = pika.ConnectionParameters(
            host=resolved.host,
            port=resolved.port,
            credentials=credentials,
            heartbeat=heartbeat,
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.routing_key = routing_key
        self.callback = callback
        self.prefetch_count = prefetch_count
        self.durable = durable
        self.auto_delete = auto_delete
        self.threads: list[threading.Thread] = []

        self.setup()

    def __enter__(self) -> "RabbitMQConsumerEngine":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def setup(self) -> None:
        """宣告 Exchange、Queue 與綁定規則並設定 QoS。"""
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=self.exchange_type,
            passive=False,
            durable=self.durable,
            auto_delete=self.auto_delete,
        )
        self.channel.queue_declare(queue=self.queue, durable=self.durable)
        self.channel.queue_bind(
            queue=self.queue,
            exchange=self.exchange,
            routing_key=self.routing_key,
        )
        self.channel.basic_qos(prefetch_count=self.prefetch_count)

    def ack_message(self, channel, delivery_tag: int) -> None:
        """於原連線執行緒中安全送出訊息確認（ACK）。"""
        if channel.is_open:
            channel.basic_ack(delivery_tag)

    def _run_worker(self, channel, delivery_tag: int, body: bytes) -> None:
        """於背景執行緒處理單筆訊息，完成後排程回主執行緒 ACK。"""
        if self.callback is None:
            raise RuntimeError("未設定訊息處理 callback 函式")
        logger.info("開始處理訊息（thread=%d, tag=%d）", threading.get_ident(), delivery_tag)
        self.callback(body)
        ack_cb = functools.partial(self.ack_message, channel, delivery_tag)
        channel.connection.add_callback_threadsafe(ack_cb)

    def on_message(self, channel, method_frame, _header_frame, body: bytes) -> None:
        """RabbitMQ 訊息抵達事件回呼，派發至獨立執行緒避免阻塞心跳。"""
        worker = threading.Thread(
            target=self._run_worker,
            args=(channel, method_frame.delivery_tag, body),
            daemon=True,
        )
        worker.start()
        self.threads.append(worker)

    def start_consuming(self) -> None:
        """開始阻塞式監聽佇列，直到收到 KeyboardInterrupt 為止。"""
        self.channel.basic_consume(on_message_callback=self.on_message, queue=self.queue)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("收到中斷訊號，停止 RabbitMQ 消費循環")
            self.channel.stop_consuming()
        finally:
            for worker in self.threads:
                worker.join(timeout=5)
            self.close()

    def close(self) -> None:
        """安全關閉 RabbitMQ 連線。"""
        if self.connection and self.connection.is_open:
            self.connection.close()
