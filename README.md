# Проект 8-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 8-го спринта.

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-8` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-8.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-8`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GitHub-аккаунте:
	* `git push origin main`

### Структура репозитория
Вложенные файлы в репозиторий будут использоваться для проверки и предоставления обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре — так будет проще соотнести задания с решениями.

Внутри `src` расположены две папки:
- `/src/dags`;
- `/src/sql`.

---

## Решение: RestaurantSubscribeStreamingService

| Файл | Назначение |
|---|---|
| `src/sql/subscribers.sql` | создание и заполнение входной таблицы `subscribers_restaurants` |
| `src/sql/feedback.sql` | создание выходной таблицы `subscribers_feedback` |
| `src/scripts/project.py` | Spark Structured Streaming приложение |
| `src/scripts/send_test_message.sh` | отправка тестовой акции (с текущим временем) через kcat |

### Логика
1. `readStream` из `KAFKA_INPUT_TOPIC` (опции подключения — `kafka_config.KAFKA_OPTIONS`).
2. `from_json(value)` → колонки; фильтр `start <= now <= end` (now = `unix_timestamp(current_timestamp())`, UTC).
3. Статичный `spark.read.jdbc` таблицы `subscribers_restaurants`.
4. `JOIN` по `restaurant_id` (по имени колонки — без дублей) + `trigger_datetime_created`.
5. `foreachBatch`: `persist()` → запись в Postgres (с `feedback = NULL`) → `to_json(struct(...))` в `value` и запись в `KAFKA_OUTPUT_TOPIC` (без `feedback`) → `unpersist()`.

### Запуск (в контейнере sprint-stream)
```bash
cd /workspace && source /workspace/kafka-env.sh
psql -h localhost -p 5432 -U jovyan -d de -W -v ON_ERROR_STOP=1 -f /workspace/subscribers.sql
psql -h localhost -p 5432 -U jovyan -d de -W -v ON_ERROR_STOP=1 -f /workspace/feedback.sql
python3 /workspace/project.py                    # терминал 1
bash /workspace/send_test_message.sh test_1      # терминал 2
```
Чтение результата:
```bash
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="$KAFKA_USERNAME" -X sasl.password="$KAFKA_PASSWORD" \
  -X ssl.ca.location="$KAFKA_CA_LOCATION" -t "$KAFKA_OUTPUT_TOPIC" -C -o beginning
```
Ожидается 9 сообщений/строк на одну акцию ресторана `...174000` (у него 9 подписчиков).
