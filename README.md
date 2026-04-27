# hdfs-data-pipeline

Полноценный data engineering pipeline: загрузка сырых CSV, нормализация данных, конвертация в Parquet и загрузка в распределённое хранилище Hadoop HDFS — всё в Docker.

```
Raw CSV  →  Нормализация  →  Parquet (Snappy)  →  HDFS (репликация × 2)
```

---

## Датасеты

| Датасет | Файл | Строк | Колонок |
|---|---|---|---|
| Car Accident Dataset (UK) | `Road_Accident_Data.csv` | 307 973 | 21 |
| World Important Events | `World_Important_Dates.csv` | 1 096 | 12 |

---

## Структура проекта

```
hdfs-data-pipeline/
├── docker-compose.yml          # HDFS-кластер + app-контейнер
├── Dockerfile
├── hadoop.env
├── requirements.txt
├── data/
│   ├── raw/                    # исходные CSV-файлы
│   └── processed/              # нормализованные Parquet-файлы
├── notebooks/
│   └── data_preview.ipynb
└── src/
    ├── main.py                 # точка входа pipeline
    ├── normalize.py            # этап 1 — нормализация данных
    ├── convert_to_parquet.py   # этап 2 — конвертация в Parquet
    └── upload_to_hdfs.py       # этап 3 — загрузка в HDFS
```

---

## Инфраструктура

Кластер запускается через Docker Compose, 4 сервиса:

| Контейнер | Образ | Роль |
|---|---|---|
| `namenode` | `bde2020/hadoop-namenode:3.2.1` | Хранит метаданные файловой системы и расположение блоков |
| `datanode1` | `bde2020/hadoop-datanode:3.2.1` | Хранит физические блоки данных |
| `datanode2` | `bde2020/hadoop-datanode:3.2.1` | Хранит реплики блоков данных |
| `app` | Python 3.11 (собственный образ) | Запускает pipeline |

**Replication factor = 2** — каждый блок данных физически хранится на обоих DataNode одновременно. Если один узел упадёт, данные останутся доступными на втором.

---

## Запуск

### 1. Поднять HDFS-кластер

```bash
docker-compose up -d namenode datanode1 datanode2
```

Подождать ~30 секунд пока NameNode инициализируется, затем проверить:

```bash
docker ps
```

Все три контейнера должны показывать статус `(healthy)`:

![Контейнеры запущены и healthy](docs/screenshots/docker-containers.png)

---

### 2. Запустить pipeline

```bash
docker-compose run --rm app
```

App-контейнер стартует автоматически после прохождения healthcheck NameNode и выполняет все три этапа:

![Вывод выполнения pipeline](docs/screenshots/pipeline-run.png)

**Что происходит внутри:**

**Этап 1 — Нормализация** (`normalize.py`)
- Названия колонок → `snake_case` (например `Local_Authority_(District)` → `local_authority_district`)
- Strip пробелов, все текстовые поля → нижний регистр
- Пропуски в текстовых колонках → `"unknown"`, числовые не трогаются
- Удаление дубликатов (найден и удалён 1 дубликат в датасете аварий)
- Парсинг `accident_date` → `datetime64`

**Этап 2 — Конвертация в Parquet** (`convert_to_parquet.py`)
- Сохранение нормализованных DataFrame в формат Parquet с компрессией Snappy
- `road_accident_data.parquet` — 8.2 MB (вместо 66 MB исходного CSV)
- `world_important_dates.parquet` — 122.8 KB

**Этап 3 — Загрузка в HDFS** (`upload_to_hdfs.py`)
- Ожидание готовности WebHDFS API
- Создание директории `/veselov.dmitry` если не существует
- Загрузка обоих Parquet-файлов с `overwrite=True`
- Вывод листинга файлов с размерами после загрузки

Время выполнения всего pipeline: **~7–12 секунд**

---

## Проверка результата

### Через CLI

```bash
docker exec namenode hdfs dfs -ls /veselov.dmitry
```

![Листинг файлов в HDFS через CLI](docs/screenshots/hdfs-files.png)

Цифра `2` в третьей колонке — это replication factor.

### Через Web UI

Открыть [http://localhost:9870](http://localhost:9870), перейти в **Utilities → Browse the file system → `/veselov.dmitry`**:

![HDFS Web UI — файловый браузер](docs/screenshots/hdfs-ui-files.png)

Оба файла видны с **Replication = 2** и **Block Size = 128 MB**.

### Репликация подтверждена

При клике на файл открывается информация о блоках — блок хранится одновременно на `datanode1` и `datanode2`:

![Репликация блока на два DataNode](docs/screenshots/replication.png)

Это подтверждает реальное распределённое хранение: файл разбит на HDFS-блоки, каждый блок реплицирован на два DataNode.

---

## Остановка кластера

```bash
docker-compose down
```

Удалить также тома с данными HDFS:

```bash
docker-compose down -v
```

---

## Стек технологий

- **Python 3.11** — pandas, pyarrow, hdfs, requests
- **Apache Hadoop 3.2.1** — распределённое хранилище HDFS
- **Docker / Docker Compose** — контейнеризация инфраструктуры
- **Parquet + Snappy** — колоночный формат хранения данных

