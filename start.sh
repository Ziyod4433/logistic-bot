#!/usr/bin/env bash
# Запуск с непрерывной репликацией SQLite (Litestream) — если настроена.
#
# Включение (переменные Railway):
#   LITESTREAM_BUCKET            имя бакета (R2/B2/S3)
#   LITESTREAM_ENDPOINT          endpoint S3-совместимого хранилища
#                                (R2: https://<ACCOUNT_ID>.r2.cloudflarestorage.com)
#   LITESTREAM_ACCESS_KEY_ID     ключ
#   LITESTREAM_SECRET_ACCESS_KEY секрет
#   LITESTREAM_PATH              путь в бакете (default: logistic)
#   LITESTREAM_REGION            (default: auto — так надо для R2)
#   LITESTREAM_VERSION           (default: 0.5.17)
#
# Без LITESTREAM_BUCKET приложение стартует как раньше — обвязка спит.
# Бинарь скачивается ОДИН раз на волюм (/app/data/bin) и переживает деплои.
set -u

APP_CMD="python app.py"

if [ -z "${LITESTREAM_BUCKET:-}" ]; then
    exec $APP_CMD
fi

DB="${DB_PATH:-/app/data/logistic.db}"
DATA_DIR="$(dirname "$DB")"
LS_VERSION="${LITESTREAM_VERSION:-0.5.17}"
LS_BIN="$DATA_DIR/bin/litestream-$LS_VERSION"

if [ ! -x "$LS_BIN" ]; then
    echo "[litestream] downloading v$LS_VERSION ..."
    mkdir -p "$DATA_DIR/bin"
    if curl -fsSL -o /tmp/litestream.tar.gz \
        "https://github.com/benbjohnson/litestream/releases/download/v$LS_VERSION/litestream-$LS_VERSION-linux-x86_64.tar.gz" \
        && tar -xzf /tmp/litestream.tar.gz -C /tmp litestream \
        && mv /tmp/litestream "$LS_BIN" \
        && chmod +x "$LS_BIN"; then
        echo "[litestream] installed to $LS_BIN"
    else
        # Репликация — страховка; её сбой не должен ронять сам сервис
        echo "[litestream] DOWNLOAD FAILED — starting WITHOUT replication"
        exec $APP_CMD
    fi
fi

CFG=/tmp/litestream.yml
cat > "$CFG" <<EOF
dbs:
  - path: $DB
    replicas:
      - type: s3
        bucket: ${LITESTREAM_BUCKET}
        path: ${LITESTREAM_PATH:-logistic}
        endpoint: ${LITESTREAM_ENDPOINT:-}
        region: ${LITESTREAM_REGION:-auto}
        access-key-id: ${LITESTREAM_ACCESS_KEY_ID:-}
        secret-access-key: ${LITESTREAM_SECRET_ACCESS_KEY:-}
EOF

# Диск пуст (новый волюм / катастрофа) — поднять базу из реплики
if [ ! -f "$DB" ]; then
    echo "[litestream] $DB missing — restoring from replica..."
    "$LS_BIN" restore -if-replica-exists -config "$CFG" "$DB" || true
fi

echo "[litestream] replication ON → bucket=$LITESTREAM_BUCKET path=${LITESTREAM_PATH:-logistic}"
exec "$LS_BIN" replicate -config "$CFG" -exec "$APP_CMD"
