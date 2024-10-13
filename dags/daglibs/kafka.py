from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from kafka import KafkaConsumer
import asyncio
from airflow.utils.context import Context

class KafkaMessageTrigger(BaseTrigger):
    def __init__(self, topic, bootstrap_servers, group_id, poll_interval=5, max_messages=None, timeout=None):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.poll_interval = poll_interval
        self.max_messages = max_messages
        self.timeout = timeout

    def serialize(self):
        return (
            "daglibs.kafka.KafkaMessageTrigger",
            {
                "topic": self.topic,
                "bootstrap_servers": self.bootstrap_servers,
                "group_id": self.group_id,
                "poll_interval": self.poll_interval,
                "max_messages": self.max_messages,
                "timeout": self.timeout,
            },
        )

    async def run(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=self.group_id
        )
        messages_processed = 0
        start_time = asyncio.get_event_loop().time()

        while True:
            for message in consumer:
                messages_processed += 1
                yield TriggerEvent({"message": message.value.decode("utf-8")})
                if self.max_messages and messages_processed >= self.max_messages:
                    return
                if self.timeout and (asyncio.get_event_loop().time() - start_time) > self.timeout:
                    return
            await asyncio.sleep(self.poll_interval)
            if self.timeout and (asyncio.get_event_loop().time() - start_time) > self.timeout:
                return

class DeferrableKafkaSensor(BaseSensorOperator):
    def __init__(self, topic, bootstrap_servers, group_id, poll_interval=5, max_messages=None, timeout=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.poll_interval = poll_interval
        self.max_messages = max_messages
        self.timeout = timeout
        self.messages_processed = 0

    def execute(self, context: Context):
        self.defer(
            trigger=KafkaMessageTrigger(
                topic=self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                poll_interval=self.poll_interval,
                max_messages=self.max_messages,
                timeout=self.timeout,
            ),
            method_name="process_message",
        )

    def process_message(self, context: Context, event):
        message = event["message"]
        self.messages_processed += 1
        print(f"Processing Kafka message: {message}")
        if self.max_messages and self.messages_processed >= self.max_messages:
            return
        else:
            self.defer(
                trigger=KafkaMessageTrigger(
                    topic=self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    poll_interval=self.poll_interval,
                    max_messages=self.max_messages - self.messages_processed if self.max_messages else None,
                    timeout=self.timeout,
                ),
                method_name="process_message",
            )
