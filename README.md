# 🚛 LogiAdmin — Система управления логистикой

## Структура проекта

```
logistic_v2/
├── app.py          ← Flask-сервер (админ-панель + API)
├── bot.py          ← Telegram-бот
├── database.py     ← База данных SQLite
├── templates/
│   └── index.html  ← Интерфейс админ-панели
├── uploads/        ← Загруженные файлы (создаётся автоматически)
├── logistic.db     ← База данных (создаётся автоматически)
├── requirements.txt
└── .env            ← Настройки (создай из .env.example)
```

## Установка

```bash
cd logistic_v2

# Создай виртуальное окружение
python -m venv venv
source venv/bin/activate        # Mac / Linux
venv\Scripts\activate           # Windows

# Установи зависимости
pip install -r requirements.txt

# Настрой .env
cp .env.example .env
# Открой .env и вставь свои данные
```

## Запуск

### Локально

Достаточно одного процесса:

```bash
python app.py
```

Открой: http://127.0.0.1:5000

Если хочешь, старый `bot.py` можно оставить только для локальных экспериментов, но для сервера основной режим теперь через `webhook` внутри `app.py`.

### На сервере / Railway

Добавь в `.env` или в переменные Railway:

```env
BOT_TOKEN=your_bot_token_here
ADMIN_LOGIN=admin
ADMIN_PASSWORD=your_password
SECRET_KEY=your_secret
WEBHOOK_BASE_URL=https://your-domain.up.railway.app
WEBHOOK_SECRET=your_random_secret
PORT=8080
```

После запуска `app.py` сайт работает как админ-панель, а Telegram-бот принимает обновления через:

```text
/telegram/webhook
```

## Функции

### Adminh-панель
| Раздел | Функция |
|--------|---------|
| 📊 Дашборд | Статистика: партии, BL, отправки, ошибки |
| 📦 Партии | Создание партий, добавление BL кодов и клиентов |
| ✏️ Шаблон | Редактор шаблона + тексты по статусам |
| 📋 Логи | История всех отправок с результатами |

### В карточке BL кода
- Имя клиента
- Chat ID Telegram-группы
- Статус груза (Принят → Хоргос → Алматы → В пути → Ташкент → Доставлен)
- Загрузка документов (PDF, DOCX, XLSX, PNG, ZIP)
- Индивидуальная отправка или массовая по всей партии

### Telegram-бот
| Действие | Описание |
|---------|---------|
| /start | Показывает кнопку клиента |
| Кнопка "Статус моего груза" | Просит ввести BL-код |
| Введённый BL-код | Отправляет статус и прикреплённые файлы |
| /chatid | Служебно показывает ID чата/группы |

## Шаблон сообщения

Доступные переменные:
- `{batch_name}` — название партии
- `{bl_code}` — BL код
- `{client_name}` — имя клиента
- `{status}` — текущий статус
- `{status_detail}` — подробное описание статуса

## API эндпоинты

| Метод | URL | Описание |
|-------|-----|---------|
| GET | /api/stats | Статистика |
| GET/POST | /api/batches | Список / создание партий |
| DELETE | /api/batches/:id | Удаление партии |
| GET | /api/batches/:id/bl | BL коды партии |
| POST | /api/batches/:id/send | Отправить всю партию |
| POST | /api/bl | Добавить BL |
| PUT | /api/bl/:id | Обновить BL |
| DELETE | /api/bl/:id | Удалить BL |
| POST | /api/bl/:id/send | Отправить одному |
| GET/POST | /api/bl/:id/files | Файлы BL |
| DELETE | /api/files/:id | Удалить файл |
| GET | /api/logs | Логи отправок |
| GET/POST | /api/template | Шаблон сообщения |
