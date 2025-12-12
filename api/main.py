import os
import uuid
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
from clickhouse_driver import Client as ClickHouseClient
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Event Ingestion API")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")

# ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))  # clickhouse-driver использует native порт 9000
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "events")

# Инициализация Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v.encode('utf-8')
)

class Event(BaseModel):
    event_type: str
    message: str

@app.post("/events")
async def ingest_event(event: Event):
    try:
        event_data = f'{{"id": "{uuid.uuid4()}", "timestamp": "{datetime.utcnow().isoformat()}", "event_type": "{event.event_type}", "message": "{event.message}"}}'
        producer.send(KAFKA_TOPIC, value=event_data)
        producer.flush()
        return {"status": "accepted", "event_type": event.event_type}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send to Kafka: {str(e)}")

@app.get("/events")
async def get_events(limit: int = 100):
    try:
        client = ClickHouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DATABASE
        )
        query = f"""
            SELECT id, timestamp, event_type, message, payload
            FROM {CLICKHOUSE_TABLE}
            ORDER BY timestamp DESC
            LIMIT {limit}
        """
        rows = client.execute(query)
        client.disconnect()

        events = [
            {
                "id": str(row[0]),
                "timestamp": row[1].isoformat() if hasattr(row[1], 'isoformat') else str(row[1]),
                "event_type": row[2],
                "message": row[3],
                "payload": row[4]
            }
            for row in rows
        ]
        return {"events": events, "count": len(events)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ClickHouse query failed: {str(e)}")

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", 8000))
    )