#!/usr/bin/env bash
# Отправляет во входной топик тестовую акцию, действующую сейчас (+2 часа).
# Запуск в контейнере: source /workspace/kafka-env.sh && bash /workspace/send_test_message.sh [key]
set -euo pipefail
KEY="${1:-msg_$(date +%s)}"
NOW=$(date +%s)
END=$((NOW + 7200))
MSG="{\"restaurant_id\": \"123e4567-e89b-12d3-a456-426614174000\",\"adv_campaign_id\": \"123e4567-e89b-12d3-a456-426614174003\",\"adv_campaign_content\": \"first campaign\",\"adv_campaign_owner\": \"Ivanov Ivan Ivanovich\",\"adv_campaign_owner_contact\": \"iiivanov@restaurant.ru\",\"adv_campaign_datetime_start\": ${NOW},\"adv_campaign_datetime_end\": ${END},\"datetime_created\": ${NOW}}"
KCAT=$(command -v kcat || command -v kafkacat)
echo "${KEY}:${MSG}" | "$KCAT" -b "$KAFKA_BOOTSTRAP_SERVERS" \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="$KAFKA_USERNAME" \
  -X sasl.password="$KAFKA_PASSWORD" \
  -X ssl.ca.location="$KAFKA_CA_LOCATION" \
  -t "$KAFKA_INPUT_TOPIC" -K: -P
echo "sent ${KEY}: start=${NOW} end=${END}"
