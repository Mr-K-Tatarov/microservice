import os
import json
import time
from collections import deque
from datetime import datetime
from kafka import KafkaConsumer
from clickhouse_driver import Client as ClickHouseClient
from dotenv import load_dotenv

BATCH_SIZE = 3000
FLUSH_INTERVAL = 5.0  # секунд

load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")

# ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "events")


def main():
    # Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='clickhouse-consumer-group',
        value_deserializer=lambda x: x.decode('utf-8'),
        fetch_max_wait_ms=500,
        max_poll_records=1000,
        fetch_max_bytes=52428800,
        max_partition_fetch_bytes=1048576,
    )

    # ClickHouse client
    ch_client = ClickHouseClient(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE
    )

    buffer = deque()
    last_flush = time.time()

    ch_client.execute(
        f"""CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE}
            (
                event_time DATETIME comment 'Время события',
                id               String comment 'ИД события',
                event_type       String comment 'Тип события',
                message String comment 'Содержание сообщения',
                payload DATETIME comment 'Время выгрузки'
            )
            ENGINE = MergeTree()
            ORDER BY (event_time)
            SETTINGS index_granularity = 8192;
        """
    )

    print("Starting ClickHouse consumer...")

    while True:
        batch = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
        if not batch:
            continue

        for tp, messages in batch.items():
            for message in messages:
                try:
                    event = json.loads(message.value)
                    # Преобразуем timestamp из ISO строки в datetime
                    ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+05:00'))

                    buffer.append({
                        'id': event['id'],
                        'event_time': ts,
                        'event_type': event['event_type'],
                        'message': event['message'],
                        'payload': datetime.now()
                    })
                except Exception as e:
                    print(f"Error: {e}")
                    continue

        if len(buffer) >= BATCH_SIZE:
            flush_buffer(ch_client, buffer, CLICKHOUSE_TABLE)
            buffer.clear()
            last_flush = time.time()

    # try:
    #     for message in consumer:
    #         try:
    #             event = json.loads(message.value)
    #             # Преобразуем timestamp из ISO строки в datetime
    #             ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
    #
    #             buffer.append({
    #                 'id': event['id'],
    #                 'event_time': ts,
    #                 'event_type': event['event_type'],
    #                 'message': event['message'],
    #                 'payload': datetime.now()
    #             })
    #
    #             now = time.time()
    #             if len(buffer) >= BATCH_SIZE or (now - last_flush) > FLUSH_INTERVAL:
    #                 flush_buffer(ch_client, buffer, CLICKHOUSE_TABLE)
    #                 buffer.clear()
    #                 last_flush = now
    #
    #         except Exception as e:
    #             print(f"Error processing message: {e}")
    #             continue
    #
    #     # Финальный сброс
    #     if buffer:
    #         flush_buffer(ch_client, buffer, CLICKHOUSE_TABLE)
    #
    # except KeyboardInterrupt:
    #     if buffer:
    #         flush_buffer(ch_client, buffer, CLICKHOUSE_TABLE)
    #     print("\nStopping consumer...")
    #
    # finally:
    #     consumer.close()
    #     ch_client.disconnect()

                # ch_client.execute(
                # 	f'INSERT INTO {CLICKHOUSE_TABLE} (event_time, id, event_type, message, payload) VALUES',
                # 	[{
                # 		'id': event['id'],
                # 		'event_time': ts,
                # 		'event_type': event['event_type'],
                # 		'message': event['message'],
                # 		'payload': datetime.now()
                # 	}]
                # )

    # 			print(f"Inserted event {event['id']}")
    # 		except Exception as e:
    # 			print(f"Error processing message: {e}")
    # 			continue
    # except KeyboardInterrupt:
    # 	print("\nStopping consumer...")
    # finally:
    # 	consumer.close()
    # 	ch_client.disconnect()

def flush_buffer(client, buffer, table):
    try:
        client.execute(
            f'INSERT INTO {table} (event_time, id, event_type, message, payload) VALUES',
            list(buffer)
        )
        print(f"✅ Flushed {len(buffer)} events to ClickHouse")
    except Exception as e:
        print(f"❌ Failed to flush batch: {e}")
        # Опционально: повторить или записать в dead-letter queue


if __name__ == "__main__":
    main()
