"""Run inside the Compose network with alias payments-webhook-test.

Exercises the real API, outbox, broker, consumer and HTTP delivery.
"""

import asyncio
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import aio_pika
import httpx

notifications = []
notification_lock = threading.Lock()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with notification_lock:
            notifications.append((self.path, body))
        self.send_response(503 if self.path == "/fail" else 204)
        self.end_headers()

    def log_message(self, *args):
        pass


async def main():
    receiver = ThreadingHTTPServer(("0.0.0.0", 8080), Receiver)
    threading.Thread(target=receiver.serve_forever, daemon=True).start()
    try:
        async with httpx.AsyncClient(base_url="http://api:8000", timeout=5) as client:
            for path in ("/ok", "/fail"):
                key = str(uuid4())
                headers = {"X-API-Key": os.environ["API_KEY"], "Idempotency-Key": key}
                body = {
                    "amount": "123.45",
                    "currency": "RUB",
                    "description": "End-to-end check",
                    "webhook_url": f"http://payments-webhook-test:8080{path}",
                }
                response = await client.post("/api/v1/payments", json=body, headers=headers)
                assert response.status_code == 202, response.text
                payment_id = response.json()["payment_id"]
                replay = await client.post("/api/v1/payments", json=body, headers=headers)
                assert replay.json()["payment_id"] == payment_id
                deadline = time.monotonic() + 45
                expected = 1 if path == "/ok" else 3
                while time.monotonic() < deadline:
                    with notification_lock:
                        delivered = [data for route, data in notifications if route == path]
                    if len(delivered) >= expected:
                        break
                    await asyncio.sleep(0.2)
                assert len(delivered) == expected, (path, delivered)
                assert all(data["payment_id"] == payment_id for data in delivered)
                state = await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
                assert state.status_code == 200, state.text
                assert state.json()["idempotency_key"] == key
                assert state.json()["status"] in ("succeeded", "failed")
                if path == "/fail":
                    async with await aio_pika.connect_robust(os.environ["RABBITMQ_URL"]) as conn:
                        channel = await conn.channel()
                        queue = await channel.declare_queue("payments.dlq", durable=True)
                        async with asyncio.timeout(10):
                            while True:
                                message = await queue.get(fail=False)
                                if message is not None:
                                    break
                                await asyncio.sleep(0.1)
                        assert json.loads(message.body)["payment_id"] == payment_id
                        assert message.headers["attempts"] == 3
                        await message.ack()
                print(f"PASS {path}: payment result received, notifications={expected}")
    finally:
        receiver.shutdown()
        receiver.server_close()


if __name__ == "__main__":
    asyncio.run(main())
