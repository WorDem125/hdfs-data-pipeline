# hdfs-data-pipeline

Учебный проект по курсу «Распределённые системы хранения данных», КТ 3.

## Цель

Построить pipeline обработки данных:

```
CSV (raw) → нормализация → Parquet → загрузка в HDFS
```

Используются два датасета с Kaggle:
- **Car Accident Dataset** — дорожные аварии в Великобритании
- **World Important Events Ancient to Modern** — важные события мировой истории

---

## Структура проекта

```
hdfs-data-pipeline/
├── docker-compose.yml
├── Dockerfile
├── hadoop.env
├── requirements.txt
├── .gitignore
├── README.md
├── data/
│   ├── raw/
│   │   ├── road_accident_data.csv
│   │   └── world_important_dates.csv
│   └── processed/
│       ├── road_accident_data.parquet
│       └── world_important_dates.parquet
├── notebooks/
│   └── data_preview.ipynb
└── src/
    ├── main.py
    ├── normalize.py
    ├── convert_to_parquet.py
    └── upload_to_hdfs.py
```

---

## Описание скриптов

**`src/normalize.py`** — читает сырые CSV, нормализует названия колонок (lowercase + underscore), удаляет дубликаты, заполняет пропуски (текст → "unknown", числа → медиана), приводит даты к datetime.

**`src/convert_to_parquet.py`** — сохраняет нормализированные DataFrame в формат Parquet с компрессией Snappy в папку `data/processed/`.

**`src/upload_to_hdfs.py`** — подключается к HDFS через WebHDFS (`hdfs.InsecureClient`), создаёт директорию `/veselov.dmitry`, загружает оба Parquet-файла, после загрузки выводит список файлов с размерами.

**`src/main.py`** — точка входа, запускает все три шага по очереди.

---

## Инфраструктура (Docker)

В `docker-compose.yml` поднимается Hadoop-кластер:

- **namenode** — хранит метаданные файловой системы (где какой файл, на каких нодах). Web UI доступен на `localhost:9870`.
- **datanode1**, **datanode2** — хранят блоки данных. Replication factor = 2, то есть каждый блок хранится на обоих DataNode одновременно.
- **app** — Python-контейнер, запускает pipeline. Стартует только после того, как namenode прошёл healthcheck.

Как это работает: когда файл загружается в HDFS, он разбивается на блоки (по умолчанию 128 МБ). NameNode записывает, где эти блоки лежат, а DataNode физически хранят данные. При replication=2 каждый блок есть на двух DataNode — если один упадёт, данные не потеряются.

---

## Запуск

**Поднять HDFS:**
```bash
docker-compose up -d namenode datanode1 datanode2
```

Подождать ~30 секунд пока namenode инициализируется.

**Запустить pipeline:**
```bash
docker-compose run --rm app
```

Контейнер сам дождётся готовности HDFS и выполнит все три шага.

**Проверить результат:**
```bash
# список контейнеров
docker ps

# файлы в HDFS
docker exec namenode hdfs dfs -ls /veselov.dmitry
```

Или открыть в браузере `http://localhost:9870` → Utilities → Browse the file system → `/veselov.dmitry`.

**Остановить:**
```bash
docker-compose down
```

---

## Локальный запуск (без Docker)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/main.py
```

Шаги 1 и 2 (нормализация + Parquet) выполнятся, шаг 3 упадёт с таймаутом если нет запущенного HDFS — это нормально.

---

## Что загружается на GitHub

- `data/raw/` — сырые CSV-файлы
- `data/processed/` — Parquet-файлы
- `src/` — Python-скрипты
- `notebooks/data_preview.ipynb` — просмотр структуры данных
- `requirements.txt`, `docker-compose.yml`, `Dockerfile`, `hadoop.env`
- `README.md`
