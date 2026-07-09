# Agent API — подключение Telegram-бота (Claude API) к админ-панели

Панель отдаёт агенту готовые агрегаты через read-only JSON API. Агент **не читает
Google Sheets сам** — все правила (подсчёт фур 20GP=0.5, нормализация имён с
апострофами, Retention, 3 вкладки жизненного цикла сборного груза, категории
весов) уже посчитаны кодом панели. Панель — единственный источник истины.

## 1. Настройка на сервере (Railway)

Добавьте переменную окружения:

```
AGENT_API_TOKEN=<длинный случайный секрет, например: openssl rand -hex 32>
```

Без неё все `/api/agent/v1/*` отвечают `503`. Тот же токен положите в env
бота (например `PANEL_API_TOKEN`).

## 2. Эндпоинты

Все запросы — GET с заголовком `Authorization: Bearer <token>`
(альтернатива: `X-API-Key: <token>`).

| Эндпоинт | Что возвращает |
|---|---|
| `/api/agent/v1/overview` | Список sales-планов (id, период, цель, активный), какие директорские секции подключены, серверная дата. Точка входа. |
| `/api/agent/v1/sales-monitor?plan_id=<id>` | Прогресс плана: `plan`, `overall` (closed/remaining/progress_percent/total_bl), `monthly` (динамика по месяцам), `departments` (Savdo bo'limi: LTL m³ + FTL фуры; Logistika bo'limi: FTL фуры) с лидербордами. `plan_id` не указан → активный план. |
| `/api/agent/v1/director/seliy?from=&to=` | Целые фуры (FTL): KPI, топ продавцов Savdo bo'limi и Logistika bo'limi (фуры/BL), клиенты. |
| `/api/agent/v1/director/sborniy?from=&to=` | Сборный груз (LTL): jami m³/BL, топ продавцов, рейтинг агентов (Fura statuslari), весовые категории (10 тарифных корзин с продавцами), дневная динамика в `series`. |
| `/api/agent/v1/director/ombor?from=&to=` | Склады YIWU/ZHONGSHAN/HORGOS: заполненность (m³, % от capacity) — **снимок текущего состояния, не зависит от from/to**; период влияет только на «Davr harakati» (движение за период) и 4 суб-метрики (ortilgan, hajm, yo'ldagi, bojxonadagi). Плюс `weight_categories` — разбивка грузов, лежащих сейчас на складе, по 5 весовым категориям с списком cargos (bl/w/wh). |

Параметры:
- `from`, `to` — `YYYY-MM-DD`; пусто = за всё время.
- `detail=full` — добавляет диагностику и полные списки продаж по весовым
  категориям (по умолчанию ответ компактный, для экономии токенов).

Пример:

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://<railway-app>/api/agent/v1/director/sborniy?from=2026-07-01&to=2026-07-31"
```

## 3. Тулы для Claude API (скопируйте в бота)

```json
[
  {
    "name": "get_overview",
    "description": "Точка входа: список sales-планов (какой активен, периоды, цели) и какие секции директор-панели подключены. Вызывай первым, если не знаешь id плана или что доступно.",
    "input_schema": { "type": "object", "properties": {}, "required": [] }
  },
  {
    "name": "get_sales_monitor",
    "description": "Прогресс месячного sales-плана: закрыто/осталось/процент, всего BL, динамика по месяцам, лидерборды отделов. Savdo bo'limi = продажи (LTL m³ + FTL фуры, идёт в план), Logistika bo'limi = 3 логиста (FTL фуры, в план НЕ идёт).",
    "input_schema": {
      "type": "object",
      "properties": {
        "plan_id": { "type": "integer", "description": "ID плана из get_overview. Не указан — активный план." }
      },
      "required": []
    }
  },
  {
    "name": "get_director_section",
    "description": "Данные секции директор-панели за период. seliy = целые фуры (FTL, счёт в фурах); sborniy = сборный груз (LTL, счёт в m³ и BL, + рейтинг агентов и весовые категории); ombor = заполненность складов YIWU/ZHONGSHAN/HORGOS и грузы в пути/на таможне.",
    "input_schema": {
      "type": "object",
      "properties": {
        "section": { "type": "string", "enum": ["seliy", "sborniy", "ombor"] },
        "from": { "type": "string", "description": "Начало периода YYYY-MM-DD. Пусто = без нижней границы." },
        "to": { "type": "string", "description": "Конец периода YYYY-MM-DD. Пусто = без верхней границы." }
      },
      "required": ["section"]
    }
  }
]
```

## 4. Обработчик тулов на стороне бота (Python)

```python
import os, requests

PANEL_URL   = os.environ["PANEL_URL"]          # https://<railway-app>
PANEL_TOKEN = os.environ["PANEL_API_TOKEN"]

def _panel_get(path: str, params: dict | None = None) -> dict:
    r = requests.get(
        f"{PANEL_URL}{path}",
        params={k: v for k, v in (params or {}).items() if v},
        headers={"Authorization": f"Bearer {PANEL_TOKEN}"},
        timeout=60,
    )
    return r.json()

def handle_tool_call(name: str, args: dict) -> dict:
    if name == "get_overview":
        return _panel_get("/api/agent/v1/overview")
    if name == "get_sales_monitor":
        return _panel_get("/api/agent/v1/sales-monitor",
                          {"plan_id": args.get("plan_id")})
    if name == "get_director_section":
        return _panel_get(f"/api/agent/v1/director/{args['section']}",
                          {"from": args.get("from"), "to": args.get("to")})
    return {"error": f"unknown tool {name}"}
```

Результат тула кладите в `tool_result` как JSON-строку (`json.dumps(..., ensure_ascii=False)`).

## 5. Системный промпт для агента (словарь бизнес-терминов)

```
Ты — аналитический ассистент директора BURAQ Logistics. Отвечаешь на вопросы
о продажах и складах, вызывая тулы. Все цифры бери ТОЛЬКО из тулов — никогда
не выдумывай и не считай сам по памяти.

Словарь:
- Seliy = целые фуры (FTL). Измеряется в фурах: 20GP/20HQ контейнер = 0.5 фуры.
- Sborniy = сборный груз (LTL). Измеряется в m³ (кубометры, CBM) и BL.
- BL / BRAND NAME = код клиента/партии. "N ta BL" = число уникальных клиентов-брендов.
- Retention = строки без имени продавца; это отдельная строка рейтинга, не человек.
- Savdo bo'limi = отдел продаж (все продавцы, кроме трёх логистов).
- Logistika bo'limi = ровно 3 человека: SAYFULLAYEV ABDULLOH, O'KTAMOV
  MAQSUDXO'JA, ABDULLAYEV IBROHIM. Их FTL-фуры показываются отдельно и
  НЕ засчитываются в sales-план.
- Sales plan = месячная цель в m³. progress_percent — процент выполнения.
- Весовые категории — 10 тарифных корзин (кг): 0–100, 100–150, 150–200,
  200–250, 250–300, 300–400, 400–500, 500–700, 700–1000, 1000+.
- Склады: YIWU, ZHONGSHAN, HORGOS. fill_percent = занято от capacity.
- Yo'ldagi yuklar = в пути (выехал, не прибыл). Bojxonadagi = на таможне.

Правила:
- Если период не назван — уточни или возьми текущий месяц (from=1-е число,
  to=сегодня). Даты передавай как YYYY-MM-DD.
- Для "как идёт план?" вызывай get_sales_monitor.
- Отвечай на языке вопроса (узбекский/русский), кратко, с цифрами.
- Если в ответе тула configured=false или error — скажи, что раздел не
  настроен в админ-панели, и не придумывай данные.
```

## 6. Добавление новых данных агенту

Новая метрика делается сначала в панели (сервис + эндпоинт), затем — при
необходимости — расширяется этот API. Логику в бота не копировать.
