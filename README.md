# Асинхронный процессинг платежей

Сервис принимает платёж, эмулирует обработку через шлюз (2–5 секунд, 90% успеха)
и отправляет результат на webhook. Платёж и событие outbox сохраняются одной
транзакцией; consumer публикует события в RabbitMQ и обрабатывает их.

Стек: Python 3.12, FastAPI, Pydantic 2, SQLAlchemy 2 (async), PostgreSQL,
FastStream/RabbitMQ, Alembic, uv.

## Запуск

Нужен Docker с Compose. Скопируйте `.env.example` в `.env` и задайте свой `API_KEY`.
Остальные значения подходят для локального запуска с новой базой.

```shell
docker compose up -d --build
docker compose ps
docker compose logs -f api consumer
```

API: http://localhost:8000. RabbitMQ Management: http://localhost:15672
(пользователь `payment_user`, пароль из `RABBITMQ_PASSWORD`).
Порты доступны только с локального компьютера.

Compose запускает PostgreSQL, RabbitMQ, API и consumer. Одноразовый сервис `migrate`
применяет миграции перед запуском API и consumer. Модели БД общие для обоих сервисов.

Пароли задаются в `.env`; используйте URL-safe символы, например буквы, цифры,
`-` и `_`. Compose формирует адреса БД и брокера внутри Docker самостоятельно.
`DATABASE_URL` и `RABBITMQ_URL` из `.env` нужны для запуска Python с компьютера.
Изменение паролей в `.env` не меняет уже созданные учётные записи в volumes.
Первая миграция рассчитана на пустую схему, старые legacy-таблицы нужно перенести отдельно.

Остановка: `docker compose down`. Данные сохраняются в volumes.

## API

Эндпоинты платежей требуют `X-API-Key`. Swagger UI доступен по `/docs`,
OpenAPI — по `/openapi.json`, ReDoc — по `/redoc`. Документация открывается без ключа.
Для запросов в Swagger нажмите Authorize и введите значение `API_KEY`.

| Метод | Адрес | Результат |
| --- | --- | --- |
| POST | `/api/v1/payments` | 202: `payment_id`, `status`, `created_at`; обязателен `Idempotency-Key` |
| GET | `/api/v1/payments/{payment_id}` | Детали платежа; 404, если не найден |

Пример в PowerShell:

```powershell
$headers = @{ 'X-API-Key' = 'ваш-ключ'; 'Idempotency-Key' = 'order-42' }
$body = @{
    amount = '100.50'
    currency = 'RUB'
    description = 'Order 42'
    metadata = @{ order_id = 42 }
    webhook_url = 'http://host.docker.internal:9000/webhook'
} | ConvertTo-Json
$result = Invoke-RestMethod -Method Post -Uri 'http://localhost:8000/api/v1/payments' -Headers $headers -ContentType 'application/json' -Body $body
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/payments/$($result.payment_id)" -Headers $headers
```

Получатель webhook запускается отдельно. В Docker Desktop `host.docker.internal`
указывает на компьютер; `localhost` внутри consumer указывает на сам контейнер.
Сумма положительная, до 18 цифр и двух десятичных разрядов; валюты RUB/USD/EUR.
Повтор с тем же ключом и данными возвращает существующий платёж, с другими данными — 409.

## Особенности

- Outbox повторяет публикацию до успеха с задержками от 2 до 300 секунд.
  Доставка допускает дубли; получатель webhook должен учитывать
  `Idempotency-Key: payment-result-{payment_id}`.
- Consumer делает три попытки обработки с задержками 2/4 секунды, затем переносит
  сообщение в `payments.dlq`. Бюджет из трёх отправок webhook хранится в БД.
  Авария может потратить попытку до отправки; задержки consumer после перезапуска
  начинаются заново. Повтор из DLQ требует отдельного решения о восстановлении бюджета.
- Результат платежа сохраняется до webhook: сбой уведомления не меняет его статус.
- Это тестовый сервис с эмуляцией шлюза. Для production нужны ограничения webhook
  и исходящей сети (SSRF), подпись уведомлений, TLS, управление секретами,
  мониторинг и резервное копирование. Масштабирование требует координации обработки.

## Проверки

```shell
uv sync --frozen --group dev
uv run --frozen ruff check .
uv run --frozen ruff format --check .
uv run --frozen pytest -q
```

Для интеграционных тестов задайте `TEST_DATABASE_URL`, `TEST_OUTBOX_DATABASE_URL`
и `TEST_RABBITMQ_URL`. Обе тестовые базы нужно предварительно мигрировать через
`alembic upgrade head`, подставив соответствующий URL в `DATABASE_URL`.
Без настроек тесты пропускаются. Используйте отдельные базы: тесты outbox/consumer
удаляют из своей базы платежи и события.
