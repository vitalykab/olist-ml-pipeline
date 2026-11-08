# Локальный ML пайплайн Olist

Проект обучает выданную CPU-модель опоздания доставки через PostgreSQL → Airflow → MLflow. DAG `olist_training` содержит четыре отдельные задачи: `extract → validate → prepare → train`.

Данные и алгоритм поставлены в стартовом наборе. `data/`, `src/olist_model.py` и `src/check_model.py` сохранены без изменений. Полные требования находятся в [ASSIGNMENT.md](ASSIGNMENT.md).

## Запуск из чистого клона

Нужны Git и работающий Docker с Compose, поддерживающим `docker compose`. На macOS подойдёт Docker Desktop или Colima. Рекомендуется выделить Docker не менее 4 ГБ RAM и 2 CPU; первая сборка скачивает образы и Python-зависимости.

```bash
git clone https://github.com/vitalykab/olist-ml-pipeline.git
cd olist-ml-pipeline
cp .env.example .env
docker compose up -d --build
docker compose ps
```

Для приватного репозитория нужен доступ GitHub. Дождитесь состояния `healthy` у трёх сервисов. При первом старте PostgreSQL сам загружает выданные данные; при перезапуске существующий volume используется повторно.

Интерфейсы доступны только на компьютере:

- Airflow: http://127.0.0.1:8080 — учебный standalone, без пароля.
- MLflow: http://127.0.0.1:5050 — эксперимент `olist_training`.

Если порты заняты, задайте `AIRFLOW_PORT` и `MLFLOW_PORT` в `.env`, затем повторите `docker compose up -d --build`.

Сначала включите и запустите `example_one_task` в Airflow. Затем включите `olist_training` и нажмите Trigger. Дождитесь успеха всех четырёх задач. Новый эксперимент появится в MLflow. Для повторной проверки нажмите Trigger ещё раз: создастся отдельный run, строки не будут накапливаться.

Альтернатива через CLI:

```bash
docker compose exec -T airflow airflow dags unpause olist_training
docker compose exec -T airflow airflow dags trigger olist_training
```

## Проверка данных и подключений

```bash
docker compose exec -T source psql -U olist -d olist -c 'SELECT count(*), count(DISTINCT order_id) FROM orders;'
docker compose exec -T airflow python -c "import urllib.request; print(urllib.request.urlopen('http://mlflow:5000/health', timeout=5).status)"
```

Ожидается 21 663 строки и столько же уникальных заказов; MLflow должен ответить HTTP 200.

Внутри Airflow используется `MLFLOW_TRACKING_URI=http://mlflow:5000`: `localhost` внутри контейнера не является компьютером или соседним контейнером. PostgreSQL доступен через `DATABASE_URL`, рабочие файлы — через `OLIST_WORKDIR=/opt/olist/work`.

## Этапы и восстановление

| Этап | Результат |
|---|---|
| `extract` | Упорядоченная выгрузка `public.orders` в обычный UTF-8 `snapshot.csv`; абсолютный путь строкой |
| `validate` | Проверка 13 столбцов, непустых данных, уникальных непустых ID, чисел без NaN/Inf, меток 0/1 и корректных дат; возвращает путь |
| `prepare` | Готовая `split_by_time`, файлы train/validation и короткий словарь путей, размеров и SHA256 |
| `train` | Готовая `fit_model`, параметры и метрики по эпохам в MLflow, полный артефакт `model`, возвращаемый MLflow `run_id` |

`validate(snapshot)` только читает файл: не создаёт каталоги, не записывает отчёты, не использует сеть или БД. Некорректный вход вызывает исключение и блокирует дальнейшие задачи.

Каждому Airflow run соответствует каталог `/opt/olist/work/<SHA256 Airflow run_id>/`. Запись CSV атомарна; повтор `extract` заменяет файл. Через XCom идут только пути и метаданные. Снимки и выборки остаются в volume `work`, поэтому повторный `train` может использовать уже подготовленные данные. Перед обучением проверяются хеши исходного снимка и обеих выборок.

Расписание: понедельник, 03:00 UTC (06:00 МСК); `catchup=False`, `max_active_runs=1`. У `extract` две повторные попытки с паузой 30 секунд. `validate` не повторяется. У `validate`, `prepare`, `train` — `all_success`. Таймауты: extract 5 минут, validate 2, prepare 5, train 15.

## Эксперимент и прогноз

Значения по умолчанию: 5 эпох, seed 42, learning rate 0.003, cutoff `2017-07-31`. Их можно задать через `.env` (`OLIST_EPOCHS`, `OLIST_SEED`, `OLIST_LR`, `OLIST_CUTOFF`) и пересоздать сервис через `docker compose up -d`.

Train содержит покупки до cutoff, validation — начиная с cutoff. При стандартной границе размеры — 17 928 и 3 735. Преобразования обучаются только на train внутри выданной модели. `validation_pr_auc` — average precision; sigmoid scores не являются откалиброванными вероятностями.

MLflow сохраняет `epochs`, `seed`, `lr`, `cutoff`, `train_rows`, `validation_rows`, `snapshot_sha256`, а также `snapshot_path`. Тег `dag_run_id` точно совпадает с Airflow run ID. Артефакт `dataset.json` содержит пути и хеши; хеш исходного снимка считается по байтам CSV из PostgreSQL, а не по `.csv.gz` или подготовленным выборкам.

В MLflow откройте успешный run и скопируйте его Run ID:

```bash
docker compose exec -T airflow python -m check_model --run-id RUN_ID
```

Вместо `RUN_ID` подставляется 32-символьный идентификатор MLflow, не Airflow. Команда скачивает `runs:/RUN_ID/model`, загружает `weights.pt` и `preprocessing.json` и выводит JSON с пятью прогнозами. В чистом окружении используйте ID созданного там эксперимента.

## Проверки ошибок и сохранности

Для проверки плохих данных временно измените `extract`, чтобы после выгрузки он создал отдельную копию CSV с повторным `order_id` и вернул путь к копии. Запустите отдельный DAG. Ожидается: extract — success, validate — failed, prepare/train — upstream_failed. Восстановите исходный код; поставленный `data/` не изменяйте.

Для проверки восстановления временно добавьте `raise RuntimeError('intentional training failure')` в начало `train` и запустите отдельный DAG. Дождитесь ошибки train, восстановите код, затем откройте именно эту задачу именно этого запуска и выполните Clear. Не выбирайте upstream, downstream, прошлые или будущие запуски. Успешные extract/validate/prepare должны сохранить прежние попытки и время выполнения; train завершится успешно.

Для проверки постоянного хранения запомните успешный MLflow run ID и его прогнозы, затем выполните:

```bash
docker compose restart
docker compose up -d --force-recreate --no-deps mlflow
docker compose ps
# После healthy у MLflow:
docker compose exec -T airflow python -m check_model --run-id RUN_ID
```

Должны сохраниться эксперимент, метрики, модель и прежние прогнозы. SQLite и артефакты MLflow хранятся вместе в volume `mlflow-data`. `docker compose down -v` удаляет volumes — не используйте его для этой проверки.

## Автоматические тесты и диагностика

Контрактные тесты запускаются в окружении Airflow, включая проверку реального чтения PostgreSQL:

```bash
docker compose cp tests airflow:/tmp/olist-tests
docker compose exec -T airflow python -m unittest discover -s /tmp/olist-tests -v
```

Для диагностики:

```bash
docker compose logs --tail=100 airflow
docker compose logs --tail=100 mlflow
docker compose exec -T airflow airflow dags list-import-errors
```

Правки `src/` и `dags/` доступны через mounts; для изменения зависимостей пересоберите образ. Для обычной остановки используйте `docker compose stop`, для запуска — `docker compose up -d`.

## Сдача и лицензия

Сдаются ссылка на репозиторий и SHA финального коммита (`git rev-parse HEAD`), README и два скриншота: успешный DAG и MLflow run с параметрами и метриками.

Выданные данные остаются в Git. `.env`, рабочие CSV, модели и логи исключены; результаты в Docker volumes не входят в репозиторий. Источник и условия использования данных — [data/README.md](data/README.md) и [data/LICENSE.md](data/LICENSE.md), CC BY-NC-SA 4.0.
