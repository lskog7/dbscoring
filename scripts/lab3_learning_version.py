# Подключаем os, чтобы читать переменные окружения и при необходимости управлять окружением запуска.
import os
# Подключаем shutil для атомарной замены parquet-директорий через временную папку.
import shutil
# Подключаем subprocess, чтобы в учебном ноутбуке можно было доустановить PySpark, если он отсутствует.
import subprocess
# Подключаем sys, чтобы вызвать pip именно из текущего Python-интерпретатора ноутбука.
import sys
# Подключаем zipfile для автоматической распаковки source.zip с исходными parquet-файлами.
import zipfile
# Импортируем Path, чтобы работать с путями одинаково на macOS, Linux и Colab.
from pathlib import Path
# Импортируем datetime для фиксации времени создания строк и записей журнала загрузки.
from datetime import datetime

# Фиксируем список обязательных директорий источников; по нему find_data_dir понимает, что нашла правильную папку.
SOURCE_NAMES = [
    "client_cards_daily",
    "credit_cards_info",
    "deb_cards_info",
]


# Функция инкапсулирует создание локальной Spark-сессии, чтобы весь notebook использовал один способ запуска.
def init_spark(app_name="lab3_credit_scoring_warehouse"):
    """Создает локальную Spark-сессию для Google Colab или Jupyter Notebook."""
    # Пробуем выполнить основной путь без установки зависимостей, если PySpark уже есть в окружении.
    try:
        from pyspark.sql import SparkSession
    # Если PySpark не установлен, переходим в учебный fallback для интерактивного запуска ноутбука.
    except ImportError:
        # Устанавливаем pyspark в текущий интерпретатор; это удобно для Colab, но в проекте тесты запускаются через uv.
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark"])
        from pyspark.sql import SparkSession

    # При повторном запуске ячейки пересоздаем SparkSession, чтобы применились настройки памяти.
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        try:
            active_session.stop()
        except Exception:
            pass

    spark_session = (
        # Начинаем fluent-конфигурацию SparkSession: appName и master будут заданы следующими вызовами.
        SparkSession.builder
        # Передаем Spark человекочитаемое имя приложения, чтобы оно отображалось в логах и Spark UI.
        .appName(app_name)
        # Ограничиваем локальный параллелизм: local[*] создает слишком много одновременных задач для ноутбука.
        .master("local[2]")
        # Увеличиваем heap JVM, потому что parquet-запись запускает оконные сортировки Spark.
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        # Уменьшаем количество shuffle-партиций, чтобы локальный запуск не дробил работу на сотни задач.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        # Создаем новую SparkSession с заданными параметрами.
        .getOrCreate()
    )
    # Снижаем шум Spark-логов, чтобы вывод лабораторной был читаемым.
    spark_session.sparkContext.setLogLevel("ERROR")
    return spark_session


# Маленький предикат проверяет, что в директории одновременно лежат все обязательные источники.
def _contains_source_dirs(path):
    # Нормализуем вход в Path, чтобы функция принимала и строки, и уже готовые Path-объекты.
    path = Path(path)
    # Возвращаем True только если сама папка существует и внутри есть каждая директория из SOURCE_NAMES.
    return path.exists() and all((path / name).is_dir() for name in SOURCE_NAMES)


# Helper собирает базовые директории запуска: текущую папку, ее родителей, Colab и /mnt/data.
def _candidate_base_dirs():
    """Возвращает рабочую директорию, ее родителей и стандартные внешние корни."""
    # Начинаем с пустого списка, чтобы сохранить порядок и вручную убрать дубли.
    base_dirs = []
    # Родители Path.cwd() позволяют найти source/sources даже при запуске notebook из папки notebooks/.
    for root in [Path.cwd(), *Path.cwd().parents, Path("/content"), Path("/mnt/data")]:
        # Добавляем только существующие директории; это защищает локальный запуск от несуществующих Colab-путей.
        if root.exists() and root not in base_dirs:
            # Сохраняем корень для быстрых проверок и резервного рекурсивного поиска.
            base_dirs.append(root)
    return base_dirs


# Функция ищет данные в типичных местах запуска: локальный проект, Colab и /mnt/data.
def find_data_dir():
    """Находит директорию, внутри которой лежат три исходника с parquet-партициями."""
    # Составляем приоритетный список путей, которые проверяются до более дорогого рекурсивного поиска.
    candidates = [Path("/data")]
    # Для каждого возможного корня проверяем стандартные относительные расположения исходников.
    for base_dir in _candidate_base_dirs():
        candidates.extend(
            [
                base_dir / "data",
                base_dir / "source" / "sources",
                base_dir / "source",
            ]
        )

    # Проходим по заранее известным кандидатам от наиболее ожидаемых к менее специфичным.
    for candidate in candidates:
        # Проверяем, является ли текущий кандидат корнем с тремя нужными источниками.
        if _contains_source_dirs(candidate):
            # Возвращаем абсолютный путь, чтобы дальнейшие операции не зависели от смены рабочей директории.
            return candidate.resolve()

    # Если директории не найдены, дополнительно ищем архив source.zip в стандартных местах.
    zip_candidates = [
        Path("source.zip"),
        Path("/content/source.zip"),
        Path("/mnt/data/source.zip"),
    ]
    # Перебираем возможные расположения архива с исходниками.
    for zip_path in zip_candidates:
        # Распаковываем только первый найденный архив, чтобы не трогать лишние файлы.
        if zip_path.exists():
            # Выбираем папку рядом с архивом как место распаковки, чтобы структура путей была предсказуемой.
            extract_to = zip_path.parent
            print(f"Директории с parquet не найдены. Распаковываю {zip_path} в {extract_to}")
            # Открываем zip через context manager, чтобы дескриптор файла закрылся автоматически.
            with zipfile.ZipFile(zip_path, "r") as archive:
                # Распаковываем все содержимое архива; после этого ниже повторно ищем папку с источниками.
                archive.extractall(extract_to)
            # Останавливаем цикл после первой успешной распаковки, чтобы не распаковывать несколько архивов подряд.
            break

    # Запускаем рекурсивный поиск только после быстрых вариантов, потому что он дороже по файловой системе.
    for root in _candidate_base_dirs():
        # Перебираем все вложенные пути, чтобы найти директорию с нужной структурой источников.
        for path in root.rglob("*"):
            # Проверяем только директории: parquet-файлы и служебные файлы не могут быть корнем источников.
            if path.is_dir() and _contains_source_dirs(path):
                return path.resolve()

    # Если все варианты исчерпаны, явно сообщаем пользователю, какие директории ожидались.
    raise FileNotFoundError(
        "Не удалось найти DATA_DIR. Ожидалась директория с подпапками: "
        + ", ".join(SOURCE_NAMES)
    )


# Создаем Spark-сессию до объявления функций обработки, потому что дальнейшие функции используют глобальный spark.
spark = init_spark()

# Импортируем Spark SQL functions под коротким именем F для выражений col, lit, max, row_number и т.д.
from pyspark.sql import functions as F
# Импортируем Window, потому что dedup/merge выбирает последнюю строку внутри ключа через оконную функцию.
from pyspark.sql import Window
# Импортируем Spark-типы, чтобы явно задать физические схемы всех таблиц хранилища.
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)

# Находим корневую директорию с тремя parquet-источниками и сохраняем путь для всех чтений.
DATA_DIR = find_data_dir()
# Задаем директорию назначения для пяти parquet-таблиц учебного хранилища.
WAREHOUSE_DIR = "warehouse"

# Печатаем найденный источник данных, чтобы студент сразу видел, откуда читаются parquet-файлы.
print(f"DATA_DIR = {DATA_DIR}")
# Печатаем абсолютный путь хранилища, чтобы после выполнения было легко открыть созданные таблицы.
print(f"WAREHOUSE_DIR = {Path(WAREHOUSE_DIR).resolve()}")


# Описываем все исходные системы в одном словаре, чтобы загрузчики не держали параметры в разных местах.
SOURCE_CONFIG = {
    "deb_cards_info": {
        # source_id задает числовой первичный ключ источника для связей с dim_attributes, load_log и витринами.
        "source_id": 1,
        # source_name хранит техническое имя директории источника и человекочитаемое имя в справочнике.
        "source_name": "deb_cards_info",
        # source_description попадет в dim_sources и объяснит бизнес-смысл источника.
        "source_description": "Данные о транзакционной активности клиента по дебетовым картам за отчетный месяц.",
        # update_frequency фиксирует частоту обновления источника: месяц или день.
        "update_frequency": "1 месяц",
        # storage_type показывает, какая логика хранения применяется к источнику: SCD1 или SCD2.
        "storage_type": "SCD1",
        # partition_col указывает имя поля, по которому физически разложены parquet-партиции источника.
        "partition_col": "report_dt",
        # initial_partition задает первую партицию для демонстрации начальной загрузки.
        "initial_partition": "2023-02-28",
        # update_partition задает партицию, которая будет загружаться на шаге обновления.
        "update_partition": "2023-03-31",
        # target_table связывает источник с одной из двух EAV-витрин хранилища.
        "target_table": "client_monthly_attrs_scd1",
        # business_columns перечисляет исходные поля, которые участвуют в вертикализации; client_id остается ключом.
        "business_columns": [
            "client_id",
            "onl_bank_active_1m_nflag",
            "auto_pay_active_qty",
            "cl_income_1m_amt",
            "dep_acc_1st_open_dt",
            "wdr_cash_6m_amt",
            "cash_op_6m_amt",
            "cash_3m_qty",
            "lst_balance_amt",
            "card_active_1m_nflag",
        ],
    },
    "credit_cards_info": {
        # source_id задает числовой первичный ключ источника для связей с dim_attributes, load_log и витринами.
        "source_id": 2,
        # source_name хранит техническое имя директории источника и человекочитаемое имя в справочнике.
        "source_name": "credit_cards_info",
        # source_description попадет в dim_sources и объяснит бизнес-смысл источника.
        "source_description": "Данные по кредитным картам клиента за отчетный месяц.",
        # update_frequency фиксирует частоту обновления источника: месяц или день.
        "update_frequency": "1 месяц",
        # storage_type показывает, какая логика хранения применяется к источнику: SCD1 или SCD2.
        "storage_type": "SCD1",
        # partition_col указывает имя поля, по которому физически разложены parquet-партиции источника.
        "partition_col": "report_dt",
        # initial_partition задает первую партицию для демонстрации начальной загрузки.
        "initial_partition": "2023-02-28",
        # update_partition задает партицию, которая будет загружаться на шаге обновления.
        "update_partition": "2023-03-31",
        # target_table связывает источник с одной из двух EAV-витрин хранилища.
        "target_table": "client_monthly_attrs_scd1",
        # business_columns перечисляет исходные поля, которые участвуют в вертикализации; client_id остается ключом.
        "business_columns": [
            "client_id",
            "client_income_amt",
            "oi_total_amt",
            "act_pl_os_rub_amt",
            "payroll_client_nflag",
            "inf_payroll_rub_amt",
            "legal_entity_amt",
            "otf_loan_rub_amt",
            "otf_fee_rub_amt",
            "inf_transfer_rub_amt",
            "cc_ever_nflag",
        ],
    },
    "client_cards_daily": {
        # source_id задает числовой первичный ключ источника для связей с dim_attributes, load_log и витринами.
        "source_id": 3,
        # source_name хранит техническое имя директории источника и человекочитаемое имя в справочнике.
        "source_name": "client_cards_daily",
        # source_description попадет в dim_sources и объяснит бизнес-смысл источника.
        "source_description": "Данные по клиенту, обновляемые раз в день.",
        # update_frequency фиксирует частоту обновления источника: месяц или день.
        "update_frequency": "1 день",
        # storage_type показывает, какая логика хранения применяется к источнику: SCD1 или SCD2.
        "storage_type": "SCD2",
        # partition_col указывает имя поля, по которому физически разложены parquet-партиции источника.
        "partition_col": "row_actual_to",
        # initial_partition задает первую партицию для демонстрации начальной загрузки.
        "initial_partition": "2023-04-03",
        # update_partition задает партицию, которая будет загружаться на шаге обновления.
        "update_partition": "9999-12-31",
        # target_table связывает источник с одной из двух EAV-витрин хранилища.
        "target_table": "client_daily_attrs_scd2",
        # business_columns перечисляет исходные поля, которые участвуют в вертикализации; client_id остается ключом.
        "business_columns": [
            "client_id",
            "srv_mb_nflag",
            "cc_stoplist",
            "lne_tot_debt_int_ovrd_rub_amt",
            "lne_tot_debt_ovrd_rub_amt",
        ],
    },
}

# Технические поля не попадают в dim_attributes как бизнес-атрибуты.
# Перечисляем технические поля источников, которые нельзя превращать в бизнес-атрибуты dim_attributes.
TECHNICAL_COLUMNS = {
    "row_update_dtime",
    "loading_id",
    "row_hash_val",
    "report_dt",
    "row_actual_from",
    "row_actual_to",
}

# В реальных parquet из архива есть несколько расхождений с формулировкой задания:
# id вместо client_id и опечатка nfalg вместо nflag.
# Задаем точечные переименования реальных parquet-полей в имена из учебной схемы.
COLUMN_ALIASES = {
    "id": "client_id",
    "onl_bank_active_1m_nfalg": "onl_bank_active_1m_nflag",
}

# Храним человекочитаемые описания атрибутов, чтобы dim_attributes была не только техническим справочником.
ATTRIBUTE_DESCRIPTIONS = {
    "onl_bank_active_1m_nflag": "Флаг активности клиента в онлайн-банкинге за 1 месяц.",
    "auto_pay_active_qty": "Количество активных шаблонов Автоплатеж за месяц.",
    "cl_income_1m_amt": "Сумма дохода по клиенту за отчетный месяц по дебетовым картам.",
    "dep_acc_1st_open_dt": "Дата открытия первого депозитного договора клиента.",
    "wdr_cash_6m_amt": "Сумма снятия наличных в рублях за последние 6 месяцев.",
    "cash_op_6m_amt": "Объем операций в рублях за последние 6 месяцев.",
    "cash_3m_qty": "Количество операций за последние 3 месяца.",
    "lst_balance_amt": "Баланс последней действующей дебетовой карты.",
    "card_active_1m_nflag": "Флаг активности по дебетовым картам за 1 месяц.",
    "client_income_amt": "Доход клиента по данным от рисков.",
    "oi_total_amt": "Сумма общего дохода по клиенту за отчетный месяц по всем кредитам.",
    "act_pl_os_rub_amt": "Совокупная задолженность по кредитам наличными.",
    "payroll_client_nflag": "Флаг зарплатного клиента.",
    "inf_payroll_rub_amt": "Сумма зарплатных начислений по всем счетам клиента за месяц в рублях.",
    "legal_entity_amt": "Сумма поступлений от юридических лиц.",
    "otf_loan_rub_amt": "Общая сумма списания в погашение кредита по всем картам клиента за месяц в рублях.",
    "otf_fee_rub_amt": "Общая сумма списания комиссий по всем картам клиента за месяц в рублях.",
    "inf_transfer_rub_amt": "Общая сумма поступлений переводом с карты или счета за месяц в рублях.",
    "cc_ever_nflag": "Флаг наличия кредитной карты когда-либо.",
    "srv_mb_nflag": "Флаг, что у клиента подключен мобильный банк.",
    "cc_stoplist": "Флаг, что клиенту нельзя предлагать кредит.",
    "lne_tot_debt_int_ovrd_rub_amt": "Сумма просрочки по процентам по кредитам.",
    "lne_tot_debt_ovrd_rub_amt": "Сумма просрочки по основному долгу по кредитам.",
}

# Выделяем месячные источники в отдельный список, потому что они пишутся в SCD1-витрину.
MONTHLY_SOURCES = ["deb_cards_info", "credit_cards_info"]
# Отдельно фиксируем daily-источник, потому что он пишется в SCD2-витрину с периодом действия строк.
DAILY_SOURCE = "client_cards_daily"


# Начинаем явное описание физической схемы таблицы: порядок и имена полей должны совпадать со schema.png.
DIM_SOURCES_SCHEMA = StructType([
    StructField("source_id", IntegerType(), False),
    StructField("source_name", StringType(), False),
    StructField("source_description", StringType(), True),
    StructField("update_frequency", StringType(), True),
    StructField("row_create_dtime", TimestampType(), True),
    StructField("valid_to", StringType(), True),
    StructField("valid_from", StringType(), True),
    StructField("row_update_dtime", TimestampType(), True),
])

# Начинаем явное описание физической схемы таблицы: порядок и имена полей должны совпадать со schema.png.
DIM_ATTRIBUTES_SCHEMA = StructType([
    StructField("attribute_id", IntegerType(), False),
    StructField("attribute_name", StringType(), False),
    StructField("attribute_description", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("source_id", IntegerType(), False),
    StructField("update_frequency", StringType(), True),
    StructField("row_create_dtime", TimestampType(), True),
    StructField("row_update_dtime", TimestampType(), True),
])

# Начинаем явное описание физической схемы таблицы: порядок и имена полей должны совпадать со schema.png.
LOAD_LOG_SCHEMA = StructType([
    StructField("load_id", LongType(), False),
    StructField("source_id", IntegerType(), False),
    StructField("source_report_dt", StringType(), False),
    StructField("load_start_dtime", TimestampType(), True),
    StructField("load_end_dtime", TimestampType(), True),
    StructField("target_table", StringType(), False),
    StructField("load_status", StringType(), False),
    StructField("loading_id", LongType(), True),
    StructField("error_message", StringType(), True),
])

# Начинаем явное описание физической схемы таблицы: порядок и имена полей должны совпадать со schema.png.
CLIENT_MONTHLY_SCHEMA = StructType([
    StructField("client_id", StringType(), False),
    StructField("attribute_id", IntegerType(), False),
    StructField("report_dt", StringType(), False),
    StructField("attribute_value", StringType(), True),
    StructField("source_id", IntegerType(), False),
    StructField("row_update_dtime", TimestampType(), True),
    StructField("loading_id", LongType(), True),
    StructField("row_hash_val", StringType(), True),
])

# Начинаем явное описание физической схемы таблицы: порядок и имена полей должны совпадать со schema.png.
CLIENT_DAILY_SCHEMA = StructType([
    StructField("client_id", StringType(), False),
    StructField("attribute_id", IntegerType(), False),
    StructField("attribute_value", StringType(), True),
    StructField("row_actual_from", StringType(), False),
    StructField("row_actual_to", StringType(), False),
    StructField("source_id", IntegerType(), False),
    StructField("row_update_dtime", TimestampType(), True),
    StructField("loading_id", LongType(), True),
    StructField("row_hash_val", StringType(), True),
])

# Собираем схемы в реестр, чтобы чтение и создание пустых таблиц работали по имени таблицы.
TABLE_SCHEMAS = {
    "dim_sources": DIM_SOURCES_SCHEMA,
    "dim_attributes": DIM_ATTRIBUTES_SCHEMA,
    "load_log": LOAD_LOG_SCHEMA,
    "client_monthly_attrs_scd1": CLIENT_MONTHLY_SCHEMA,
    "client_daily_attrs_scd2": CLIENT_DAILY_SCHEMA,
}

# Получаем список таблиц из ключей реестра схем, чтобы не дублировать имена вручную.
TABLE_NAMES = list(TABLE_SCHEMAS.keys())


# Строим путь к конкретной parquet-таблице внутри WAREHOUSE_DIR по ее логическому имени.
def table_path(table_name):
    # Возвращаем путь без проверки существования: сама запись или чтение решают, что делать с директорией.
    return Path(WAREHOUSE_DIR) / table_name


# Чтение таблицы оборачиваем в функцию, чтобы отсутствующая таблица давала пустой DataFrame с правильной схемой.
def read_table_if_exists(table_name):
    """Читает таблицу хранилища или возвращает пустой DataFrame с нужной схемой."""
    # Вычисляем физическое расположение parquet-таблицы по ее логическому имени.
    path = table_path(table_name)
    # Если директория таблицы уже создана, читаем ее как parquet.
    if path.exists():
        # Spark лениво создает DataFrame поверх parquet-файлов; реальные вычисления начнутся на action.
        return spark.read.parquet(str(path))
    # Для отсутствующей таблицы возвращаем пустой DataFrame с тем же контрактом колонок и типов.
    return spark.createDataFrame([], TABLE_SCHEMAS[table_name])


# Запись таблицы сделана через временную папку, чтобы не оставить полуперезаписанную таблицу при сбое.
def write_table(df, table_name):
    """Перезаписывает одну parquet-таблицу через временную директорию."""
    # Запоминаем финальную директорию таблицы, которую нужно заменить после успешной временной записи.
    target = table_path(table_name)
    # Создаем имя временной директории внутри warehouse, чтобы move оставался локальной файловой операцией.
    temp = Path(WAREHOUSE_DIR) / f"__tmp_{table_name}"
    # Гарантируем наличие корневой директории warehouse перед любой записью таблиц.
    Path(WAREHOUSE_DIR).mkdir(parents=True, exist_ok=True)

    # Удаляем старую временную директорию от предыдущего неудачного запуска, чтобы overwrite писал в чистое место.
    if temp.exists():
        # Рекурсивно удаляем временную parquet-директорию вместе со служебными Spark-файлами.
        shutil.rmtree(temp)

    # Сначала полностью записываем новый DataFrame во временную parquet-директорию.
    df.write.mode("overwrite").parquet(str(temp))

    # Финальную директорию удаляем только после успешной записи temp, уменьшая риск потерять рабочую таблицу.
    if target.exists():
        # Удаляем предыдущую версию таблицы перед заменой на новую.
        shutil.rmtree(target)
    # Переименовываем временную директорию в финальную, завершая атомарную для локальной FS замену таблицы.
    shutil.move(str(temp), str(target))


# Нормализация приводит реальные parquet-колонки к именам и типам, ожидаемым физической моделью.
def normalize_source_columns(df):
    """Приводит реальные имена полей parquet к именам из задания."""
    # Применяем только заранее разрешенные переименования, чтобы случайно не изменить бизнес-смысл колонок.
    for old_name, new_name in COLUMN_ALIASES.items():
        # Переименовываем поле только если старое имя есть, а новое еще не создано, чтобы не получить конфликт колонок.
        if old_name in df.columns and new_name not in df.columns:
            # Spark возвращает новый DataFrame с другим именем колонки; исходный DataFrame не мутируется inplace.
            df = df.withColumnRenamed(old_name, new_name)

    # client_id приводим к string, потому что схема хранилища хранит ключ клиента как строку.
    if "client_id" in df.columns:
        # Создаем новую версию DataFrame, где client_id уже имеет требуемый тип string.
        df = df.withColumn("client_id", F.col("client_id").cast("string"))
    # loading_id приводим к long, чтобы технический идентификатор совпадал с типом в load_log и витринах.
    if "loading_id" in df.columns:
        # Создаем новую версию DataFrame, где loading_id имеет тип long.
        df = df.withColumn("loading_id", F.col("loading_id").cast("long"))
    # report_dt приводим к string, потому что партиционные даты в схеме представлены строками.
    if "report_dt" in df.columns:
        # Сохраняем report_dt как строку, чтобы ключ monthly-витрины был стабильным.
        df = df.withColumn("report_dt", F.col("report_dt").cast("string"))
    # row_actual_from приводим к string для совпадения с SCD2-схемой.
    if "row_actual_from" in df.columns:
        # Сохраняем начало периода действия SCD2-строки как строковое поле.
        df = df.withColumn("row_actual_from", F.col("row_actual_from").cast("string"))
    # row_actual_to приводим к string для совпадения с SCD2-схемой и partition value.
    if "row_actual_to" in df.columns:
        # Сохраняем конец периода действия SCD2-строки как строковое поле.
        df = df.withColumn("row_actual_to", F.col("row_actual_to").cast("string"))

    # Возвращаем последнюю версию DataFrame после всех ленивых Spark-преобразований.
    return df


# Эта функция читает одну физическую партицию источника и добавляет partition column, если Spark не восстановил ее из пути.
def read_source_partition(source_name, partition_col, partition_value):
    """Читает parquet из директории нужной партиции целиком."""
    # Строим путь до директории конкретного источника внутри найденного DATA_DIR.
    source_root = Path(DATA_DIR) / source_name
    # Составляем приоритетный список путей, которые проверяются до более дорогого рекурсивного поиска.
    candidates = [
        source_root / f"{partition_col}='{partition_value}'",
        source_root / f"{partition_col}={partition_value}",
    ]

    # Изначально считаем, что партиция не найдена; ниже заполним переменную первым существующим вариантом.
    partition_path = None
    # Проходим по заранее известным кандидатам от наиболее ожидаемых к менее специфичным.
    for candidate in candidates:
        # Выбираем только реально существующую директорию партиции.
        if candidate.exists():
            # Запоминаем найденный путь, чтобы после цикла прочитать parquet именно из этой директории.
            partition_path = candidate
            # Останавливаем цикл после первой успешной распаковки, чтобы не распаковывать несколько архивов подряд.
            break

    # Если ни один вариант директории не существует, прерываем загрузку понятной ошибкой.
    if partition_path is None:
        # Если все варианты исчерпаны, явно сообщаем пользователю, какие директории ожидались.
        raise FileNotFoundError(
            f"Не найдена партиция {source_name}/{partition_col}={partition_value}. "
            f"Проверенные варианты: {candidates}"
        )

    # Читаем parquet-файлы выбранной партиции в Spark DataFrame.
    df = spark.read.parquet(str(partition_path))
    # Сразу после чтения приводим имена и типы технических колонок к контракту хранилища.
    df = normalize_source_columns(df)

    # Spark может не восстановить partition column при чтении самой директории партиции, поэтому проверяем ее наличие.
    if partition_col not in df.columns:
        # Если partition column отсутствует, добавляем ее константой из имени директории.
        df = df.withColumn(partition_col, F.lit(partition_value))
    # Если daily-партиция еще не загружалась, выполняем чтение, вертикализацию, merge и запись в журнал.
    else:
        # Приводим partition column к string, чтобы она совпадала с ключами витрин и load_log.
        df = df.withColumn(partition_col, F.col(partition_col).cast("string"))

    # Возвращаем последнюю версию DataFrame после всех ленивых Spark-преобразований.
    return df


# Формируем справочник источников строго по SOURCE_CONFIG, чтобы он был воспроизводимым и маленьким.
def build_dim_sources():
    # Фиксируем один timestamp на построение справочника, чтобы create/update time были согласованы внутри batch.
    now = datetime.now()
    # Готовим обычный Python-список строк, потому что справочники маленькие и детерминированные.
    rows = []
    # Идем по источникам в фиксированном порядке, чтобы source_id и attribute_id были воспроизводимыми.
    for source_name in ["deb_cards_info", "credit_cards_info", "client_cards_daily"]:
        # Берем метаданные текущего источника из единого конфигурационного словаря.
        cfg = SOURCE_CONFIG[source_name]
        # Добавляем одну строку будущего Spark DataFrame как tuple в порядке полей схемы.
        rows.append((
            cfg["source_id"],
            cfg["source_name"],
            cfg["source_description"],
            cfg["update_frequency"],
            now,
            "9999-12-31",
            "1900-01-01",
            now,
        ))
    # Преобразуем маленький список Python tuple в Spark DataFrame с явно заданной схемой.
    return spark.createDataFrame(rows, DIM_SOURCES_SCHEMA)


# Формируем справочник бизнес-атрибутов, исключая client_id и технические поля.
def build_dim_attributes():
    # Фиксируем один timestamp на построение справочника, чтобы create/update time были согласованы внутри batch.
    now = datetime.now()
    # Готовим обычный Python-список строк, потому что справочники маленькие и детерминированные.
    rows = []
    # Начинаем surrogate key атрибутов с 1, чтобы получить простой последовательный идентификатор.
    attribute_id = 1

    # Идем по источникам в фиксированном порядке, чтобы source_id и attribute_id были воспроизводимыми.
    for source_name in ["deb_cards_info", "credit_cards_info", "client_cards_daily"]:
        # Берем метаданные текущего источника из единого конфигурационного словаря.
        cfg = SOURCE_CONFIG[source_name]
        # Читаем начальную партицию, чтобы взять реальные Spark-типы бизнес-колонок для dim_attributes.
        sample_df = read_source_partition(
            source_name,
            cfg["partition_col"],
            cfg["initial_partition"],
        )
        # Строим словарь column -> Spark type string, чтобы быстро находить тип каждой бизнес-колонки.
        schema_by_name = {field.name: field.dataType.simpleString() for field in sample_df.schema.fields}

        # Перебираем только колонки, объявленные бизнес-атрибутами для текущего источника.
        for column_name in cfg["business_columns"]:
            # client_id является ключом клиента, а не измеряемым атрибутом, поэтому не попадает в dim_attributes.
            if column_name == "client_id" or column_name in TECHNICAL_COLUMNS:
                # Переходим к следующему источнику: текущая партиция уже есть в load_log и повторно не загружается.
                continue

            # Добавляем одну строку будущего Spark DataFrame как tuple в порядке полей схемы.
            rows.append((
                attribute_id,
                column_name,
                ATTRIBUTE_DESCRIPTIONS.get(column_name, f"Бизнес-атрибут {column_name}"),
                schema_by_name.get(column_name, "string"),
                cfg["source_id"],
                cfg["update_frequency"],
                now,
                now,
            ))
            # Увеличиваем surrogate key после добавления атрибута, чтобы следующий атрибут получил новый id.
            attribute_id += 1

    # Преобразуем маленький список Python tuple в Spark DataFrame с явно заданной схемой.
    return spark.createDataFrame(rows, DIM_ATTRIBUTES_SCHEMA)


# Проверяем журнал загрузки, чтобы повторный запуск не добавлял дубли за ту же партицию.
def already_loaded(load_log_df, source_id, source_report_dt, target_table):
    return (
        load_log_df
        # Оставляем только строки, удовлетворяющие условию текущей проверки или merge-логики.
        .filter(
            (F.col("source_id") == int(source_id))
            & (F.col("source_report_dt") == str(source_report_dt))
            & (F.col("target_table") == target_table)
            & (F.col("load_status") == "SUCCESS")
        )
        # Ограничиваем результат одной строкой, потому что для проверки достаточно факта существования загрузки.
        .limit(1)
        # count является Spark action: именно здесь Spark реально выполняет ленивый план фильтрации.
        .count()
        # Преобразуем количество найденных строк в boolean: True означает, что партиция уже успешно загружалась.
        > 0
    )


# Вычисляем следующий load_id как max + 1, потому что журнал здесь хранится как parquet без sequence-генератора.
def _next_load_id(load_log_df):
    # Считаем максимальный load_id в текущем журнале, чтобы новый id был следующим числом.
    max_load_id = load_log_df.agg(F.max("load_id")).collect()[0][0]
    # Если журнал пустой, начинаем с 1; иначе возвращаем max(load_id) + 1.
    return 1 if max_load_id is None else int(max_load_id) + 1


# Достаем технический loading_id из исходной партиции, чтобы связать строку журнала с исходной загрузкой.
def _source_loading_id(source_df):
    if "loading_id" not in source_df.columns:
        return None
    # Берем максимальный loading_id в партиции как технический идентификатор исходной загрузки.
    value = source_df.agg(F.max("loading_id")).collect()[0][0]
    # Сохраняем None для отсутствующего loading_id, иначе явно приводим значение к Python int.
    return None if value is None else int(value)


# Добавляем одну запись в load_log и возвращаем новый DataFrame журнала без немедленной записи на диск.
def add_load_log_record(
    load_log_df,
    source_id,
    source_report_dt,
    target_table,
    load_status="SUCCESS",
    loading_id=None,
    error_message=None,
    load_start_dtime=None,
):
    # Если caller не передал начало загрузки, фиксируем его прямо перед созданием записи журнала.
    if load_start_dtime is None:
        # Запоминаем время старта, чтобы load_log отражал временной интервал обработки.
        load_start_dtime = datetime.now()
    # Фиксируем время окончания непосредственно перед добавлением строки в журнал.
    load_end_dtime = datetime.now()

    # Создаем одноэлементный список tuple, потому что createDataFrame ожидает коллекцию строк.
    new_row = [(
        _next_load_id(load_log_df),
        int(source_id),
        str(source_report_dt),
        load_start_dtime,
        load_end_dtime,
        target_table,
        load_status,
        loading_id,
        error_message,
    )]
    # Преобразуем новую строку журнала в DataFrame с той же схемой, что и load_log.
    new_df = spark.createDataFrame(new_row, LOAD_LOG_SCHEMA)
    # Добавляем запись через unionByName, чтобы порядок колонок не стал источником ошибки.
    return load_log_df.unionByName(new_df)


# Преобразуем широкий monthly-источник в EAV-формат client_monthly_attrs_scd1.
def verticalize_monthly(source_df, source_name, dim_attributes_df):
    # Берем метаданные текущего источника из единого конфигурационного словаря.
    cfg = SOURCE_CONFIG[source_name]
    # Достаем source_id один раз, чтобы дальше использовать его и в EAV-строках, и в join с dim_attributes.
    source_id = cfg["source_id"]
    # Из списка бизнес-колонок убираем client_id: он остается ключом строки, а не attribute_value.
    attribute_columns = [c for c in cfg["business_columns"] if c != "client_id"]
    # Проверяем, что все ожидаемые атрибуты реально присутствуют в прочитанной parquet-партиции.
    missing_columns = [c for c in attribute_columns if c not in source_df.columns]
    # Если хотя бы одной бизнес-колонки нет, лучше упасть до записи в хранилище, чем создать неполную витрину.
    if missing_columns:
        # Сообщаем точный список отсутствующих колонок, чтобы ошибку источника было легко диагностировать.
        raise ValueError(f"В источнике {source_name} нет колонок: {missing_columns}")

    # Готовим аргументы Spark stack: пары attribute_name и приведенное к string значение колонки.
    stack_args = ", ".join([f"'{c}', CAST(`{c}` AS STRING)" for c in attribute_columns])
    # Собираем SQL-выражение stack, которое превращает широкие колонки в строки EAV-формата.
    stack_expr = f"stack({len(attribute_columns)}, {stack_args}) AS (attribute_name, attribute_value)"

    # Начинаем ленивый Spark-пайплайн вертикализации; вычисления произойдут только при action или записи.
    vertical_df = (
        # Берем исходный DataFrame партиции как стартовую точку цепочки преобразований.
        source_df
        # Выбираем только поля, необходимые для целевой таблицы, и сразу задаем нужные alias/типы.
        .select(
            # Оставляем client_id отдельным ключом строки EAV и приводим его к string.
            F.col("client_id").cast("string").alias("client_id"),
            # Для monthly-витрины сохраняем отчетную дату как часть бизнес-ключа.
            F.col("report_dt").cast("string").alias("report_dt"),
            # Выполняем stack: каждая бизнес-колонка исходной строки становится отдельной строкой attribute_name/value.
            F.expr(stack_expr),
            # Добавляем source_id константой, чтобы каждая EAV-строка знала источник происхождения.
            F.lit(source_id).cast("int").alias("source_id"),
            # Переносим техническое время обновления строки из источника без изменения.
            F.col("row_update_dtime"),
            # Переносим loading_id и приводим его к long в соответствии со схемой хранилища.
            F.col("loading_id").cast("long"),
            # Переносим row_hash_val как технический hash исходной строки для контроля происхождения данных.
            F.col("row_hash_val"),
        )
        # Обогащаем вертикальные строки идентификатором attribute_id из справочника dim_attributes.
        .join(
            # Из справочника берем только поля, нужные для join: attribute_id, attribute_name и source_id.
            dim_attributes_df.select("attribute_id", "attribute_name", "source_id"),
            # Соединяем по attribute_name и source_id, потому что одинаковые имена атрибутов могут прийти из разных источников.
            on=["attribute_name", "source_id"],
            # Используем left join, чтобы строки источника не пропали; null attribute_id затем выявляется проверкой качества.
            how="left",
        )
        # Выбираем только поля, необходимые для целевой таблицы, и сразу задаем нужные alias/типы.
        .select(
            "client_id",
            # Приводим найденный attribute_id к int, как требуется в целевых EAV-таблицах.
            F.col("attribute_id").cast("int").alias("attribute_id"),
            "report_dt",
            "attribute_value",
            # source_id задает числовой первичный ключ источника для связей с dim_attributes, load_log и витринами.
            "source_id",
            "row_update_dtime",
            "loading_id",
            "row_hash_val",
        )
    )
    # Возвращаем подготовленный EAV DataFrame без записи: запись выполняется уровнем загрузки.
    return vertical_df


# Преобразуем широкий daily-источник в EAV-формат client_daily_attrs_scd2 с периодом действия строки.
def verticalize_daily(source_df, source_name, dim_attributes_df):
    # Берем метаданные текущего источника из единого конфигурационного словаря.
    cfg = SOURCE_CONFIG[source_name]
    # Достаем source_id один раз, чтобы дальше использовать его и в EAV-строках, и в join с dim_attributes.
    source_id = cfg["source_id"]
    # Из списка бизнес-колонок убираем client_id: он остается ключом строки, а не attribute_value.
    attribute_columns = [c for c in cfg["business_columns"] if c != "client_id"]
    # Проверяем, что все ожидаемые атрибуты реально присутствуют в прочитанной parquet-партиции.
    missing_columns = [c for c in attribute_columns if c not in source_df.columns]
    # Если хотя бы одной бизнес-колонки нет, лучше упасть до записи в хранилище, чем создать неполную витрину.
    if missing_columns:
        # Сообщаем точный список отсутствующих колонок, чтобы ошибку источника было легко диагностировать.
        raise ValueError(f"В источнике {source_name} нет колонок: {missing_columns}")

    # Готовим аргументы Spark stack: пары attribute_name и приведенное к string значение колонки.
    stack_args = ", ".join([f"'{c}', CAST(`{c}` AS STRING)" for c in attribute_columns])
    # Собираем SQL-выражение stack, которое превращает широкие колонки в строки EAV-формата.
    stack_expr = f"stack({len(attribute_columns)}, {stack_args}) AS (attribute_name, attribute_value)"

    # Начинаем ленивый Spark-пайплайн вертикализации; вычисления произойдут только при action или записи.
    vertical_df = (
        # Берем исходный DataFrame партиции как стартовую точку цепочки преобразований.
        source_df
        # Выбираем только поля, необходимые для целевой таблицы, и сразу задаем нужные alias/типы.
        .select(
            # Оставляем client_id отдельным ключом строки EAV и приводим его к string.
            F.col("client_id").cast("string").alias("client_id"),
            # Выполняем stack: каждая бизнес-колонка исходной строки становится отдельной строкой attribute_name/value.
            F.expr(stack_expr),
            # Для daily SCD2 сохраняем начало периода действия версии атрибута.
            F.col("row_actual_from").cast("string").alias("row_actual_from"),
            # Для daily SCD2 сохраняем конец периода действия версии атрибута.
            F.col("row_actual_to").cast("string").alias("row_actual_to"),
            # Добавляем source_id константой, чтобы каждая EAV-строка знала источник происхождения.
            F.lit(source_id).cast("int").alias("source_id"),
            # Переносим техническое время обновления строки из источника без изменения.
            F.col("row_update_dtime"),
            # Переносим loading_id и приводим его к long в соответствии со схемой хранилища.
            F.col("loading_id").cast("long").alias("loading_id"),
            # Переносим row_hash_val как технический hash исходной строки для контроля происхождения данных.
            F.col("row_hash_val"),
        )
        # Обогащаем вертикальные строки идентификатором attribute_id из справочника dim_attributes.
        .join(
            # Из справочника берем только поля, нужные для join: attribute_id, attribute_name и source_id.
            dim_attributes_df.select("attribute_id", "attribute_name", "source_id"),
            # Соединяем по attribute_name и source_id, потому что одинаковые имена атрибутов могут прийти из разных источников.
            on=["attribute_name", "source_id"],
            # Используем left join, чтобы строки источника не пропали; null attribute_id затем выявляется проверкой качества.
            how="left",
        )
        # Выбираем только поля, необходимые для целевой таблицы, и сразу задаем нужные alias/типы.
        .select(
            "client_id",
            # Приводим найденный attribute_id к int, как требуется в целевых EAV-таблицах.
            F.col("attribute_id").cast("int").alias("attribute_id"),
            "attribute_value",
            "row_actual_from",
            "row_actual_to",
            # source_id задает числовой первичный ключ источника для связей с dim_attributes, load_log и витринами.
            "source_id",
            "row_update_dtime",
            "loading_id",
            "row_hash_val",
        )
    )
    # Возвращаем подготовленный EAV DataFrame без записи: запись выполняется уровнем загрузки.
    return vertical_df


# Объединяем старое и новое состояние SCD1, оставляя одну актуальную строку на ключ client_id + attribute_id + report_dt.
def merge_scd1(old_df, new_df, business_key_columns=None):
    """
    SCD1 merge:
    - если ключ есть в old_df и new_df, берем строку из new_df;
    - если ключ есть только в old_df, оставляем old_df;
    - если ключ есть только в new_df, добавляем new_df.
    """
    # Для SCD1 бизнес-ключ не включает report_dt: новая месячная загрузка заменяет актуальное значение атрибута.
    if business_key_columns is None:
        business_key_columns = ["client_id", "attribute_id"]

    def assert_unique_keys(df, df_name):
        # Внутри каждой входной таблицы бизнес-ключ SCD1 должен быть уникальным.
        duplicate_keys = df.groupBy(*business_key_columns).count().filter(F.col("count") > 1)
        # Берем несколько примеров дублей как Spark action, чтобы ошибка сразу показывала проблемные ключи.
        examples = duplicate_keys.limit(5).collect()
        if examples:
            # Формируем компактный список ключей, чтобы диагностика была читаемой в notebook output.
            formatted_examples = [
                {column: row[column] for column in business_key_columns} | {"count": row["count"]}
                for row in examples
            ]
            raise ValueError(
                f"{df_name} contains duplicate SCD1 business keys "
                f"{business_key_columns}: {formatted_examples}"
            )

    # Merge корректен только для одинакового набора колонок: new_df должен содержать полную актуальную строку.
    missing_old_columns = set(new_df.columns) - set(old_df.columns)
    missing_new_columns = set(old_df.columns) - set(new_df.columns)
    if missing_old_columns or missing_new_columns:
        raise ValueError(
            "old_df and new_df must have the same columns. "
            f"Only in new_df: {sorted(missing_old_columns)}. "
            f"Only in old_df: {sorted(missing_new_columns)}."
        )

    # Дубли внутри old_df или new_df считаем ошибкой данных, потому что заменить ключ однозначно нельзя.
    assert_unique_keys(old_df, "old_df")
    assert_unique_keys(new_df, "new_df")

    # Ключи из новой загрузки заменяют старое актуальное состояние SCD1.
    updated_keys_df = new_df.select(*business_key_columns).distinct()
    # Оставляем из старой таблицы только строки, ключей которых нет в новой загрузке.
    unchanged_old_df = old_df.join(updated_keys_df, on=business_key_columns, how="left_anti")

    # Добавляем новые строки и возвращаем исходный порядок колонок, чтобы parquet-схема оставалась стабильной.
    return unchanged_old_df.unionByName(new_df.select(old_df.columns)).select(old_df.columns)


# Объединяем SCD2-строки как историю версий по client_id + attribute_id.
def merge_scd2(
    old_df,
    new_df,
    business_key_columns=None,
    valid_from_col="row_actual_from",
    valid_to_col="row_actual_to",
    current_valid_to_value="9999-12-31",
    compare_columns=None,
):
    """
    SCD2 merge с полуоткрытым интервалом [row_actual_from, row_actual_to):
    - если бизнес-ключа нет в old_df, вставляем новую версию;
    - если текущая версия есть и атрибут изменился, закрываем старую версию и вставляем новую;
    - если текущая версия есть и атрибут не изменился, оставляем old_df без изменений;
    - исторические строки old_df не меняем.
    """
    # История ведется по бизнес-ключу клиента и атрибута.
    if business_key_columns is None:
        business_key_columns = ["client_id", "attribute_id"]
    business_key_columns = list(business_key_columns)

    # По умолчанию сравниваем только бизнес-значение EAV-атрибута, а не технические поля загрузки.
    if compare_columns is None:
        compare_columns = ["attribute_value"]
    else:
        compare_columns = list(compare_columns)

    # Сохраняем контракт старой таблицы: порядок колонок и типы нужны для выравнивания новой порции.
    old_columns = old_df.columns
    old_types = {field.name: field.dataType for field in old_df.schema.fields}

    # Проверяем, что старая таблица содержит обязательные поля SCD2.
    required_old_columns = set(business_key_columns + [valid_from_col, valid_to_col])
    missing_old_columns = required_old_columns - set(old_df.columns)
    if missing_old_columns:
        raise ValueError(f"old_df does not contain required columns: {sorted(missing_old_columns)}")

    # Новая порция должна содержать ключ, дату начала версии и поля для сравнения.
    required_new_columns = set(business_key_columns + [valid_from_col] + compare_columns)
    missing_new_columns = required_new_columns - set(new_df.columns)
    if missing_new_columns:
        raise ValueError(f"new_df does not contain required columns: {sorted(missing_new_columns)}")

    def assert_unique_keys(df, key_columns, df_name):
        # Внутри входной таблицы ключ версии или текущий бизнес-ключ должен быть уникальным.
        duplicate_keys = df.groupBy(*key_columns).count().filter(F.col("count") > 1).limit(5).collect()
        if duplicate_keys:
            # Формируем компактный список ключей для диагностики в notebook output.
            formatted_examples = [
                {column: row[column] for column in key_columns} | {"count": row["count"]}
                for row in duplicate_keys
            ]
            raise ValueError(f"{df_name} contains duplicate keys {list(key_columns)}: {formatted_examples}")

    # В истории old_df не может быть двух версий с одинаковой датой начала действия.
    assert_unique_keys(old_df, business_key_columns + [valid_from_col], "old_df")
    # new_df моделирует новую актуальную порцию: одна строка на бизнес-ключ.
    assert_unique_keys(new_df, business_key_columns, "new_df")

    # Если в новой порции нет row_actual_to, считаем ее открытой версией.
    new_prepared_df = new_df
    if valid_to_col not in new_prepared_df.columns:
        new_prepared_df = new_prepared_df.withColumn(
            valid_to_col,
            F.lit(current_valid_to_value).cast(old_types[valid_to_col]),
        )

    # Все остальные колонки старого слоя должны быть доступны в новой порции после подготовки.
    missing_columns_after_prepare = set(old_columns) - set(new_prepared_df.columns)
    if missing_columns_after_prepare:
        raise ValueError(
            "new_df cannot be aligned to old_df, missing columns: "
            f"{sorted(missing_columns_after_prepare)}"
        )

    # Выравниваем новую порцию под схему old_df.
    new_prepared_df = new_prepared_df.select(
        *[F.col(column).cast(old_types[column]).alias(column) for column in old_columns]
    )

    def make_hash(columns):
        # Хэш нужен, чтобы сравнить несколько атрибутов одним условием.
        if not columns:
            return F.lit("__no_compare_columns__")
        return F.sha2(F.to_json(F.struct(*[F.col(column).alias(column) for column in columns])), 256)

    # В этой лабораторной актуальная SCD2-строка открыта датой 9999-12-31; NULL тоже поддерживаем.
    current_condition = F.col(valid_to_col).isNull() | (F.col(valid_to_col) == current_valid_to_value)
    old_current_df = old_df.filter(current_condition)
    old_history_df = old_df.filter(~current_condition)

    # На один бизнес-ключ может быть только одна текущая версия.
    assert_unique_keys(old_current_df, business_key_columns, "old_df current records")

    old_current_with_hash_df = (
        old_current_df
        .withColumn("__old_hash", make_hash(compare_columns))
        .withColumn("__old_exists", F.lit(True))
    )
    new_with_hash_df = (
        new_prepared_df
        .withColumn("__new_hash", make_hash(compare_columns))
        .withColumn("__new_exists", F.lit(True))
    )

    # Сопоставляем текущую версию old_df с новой порцией по бизнес-ключу.
    joined_df = old_current_with_hash_df.join(new_with_hash_df, on=business_key_columns, how="full_outer")

    # Изменившиеся ключи: текущая версия есть, новая версия есть, сравниваемые поля отличаются.
    changed_keys_df = (
        joined_df
        .filter(
            F.col("__old_exists").isNotNull()
            & F.col("__new_exists").isNotNull()
            & (F.col("__old_hash") != F.col("__new_hash"))
        )
        .select(*business_key_columns)
        .distinct()
    )

    # Новые ключи: в old_df текущей версии не было, в new_df запись есть.
    new_only_keys_df = (
        joined_df
        .filter(F.col("__old_exists").isNull() & F.col("__new_exists").isNotNull())
        .select(*business_key_columns)
        .distinct()
    )
    insert_keys_df = changed_keys_df.unionByName(new_only_keys_df).distinct()

    # Для закрытия старой версии берем дату начала новой версии.
    changed_new_from_df = (
        new_prepared_df
        .join(changed_keys_df, on=business_key_columns, how="inner")
        .select(*business_key_columns, F.col(valid_from_col).alias("__new_valid_from"))
    )

    # Полуоткрытый интервал: старая версия действует до даты начала новой версии, не включая ее.
    expired_old_current_df = (
        old_current_df
        .join(changed_new_from_df, on=business_key_columns, how="inner")
        .withColumn(valid_to_col, F.col("__new_valid_from").cast(old_types[valid_to_col]))
        .drop("__new_valid_from")
    )

    # Неизменившиеся текущие версии остаются как есть.
    unchanged_old_current_df = old_current_df.join(changed_keys_df, on=business_key_columns, how="left_anti")

    # Вставляем только новые и реально изменившиеся версии.
    new_versions_df = new_prepared_df.join(insert_keys_df, on=business_key_columns, how="inner")

    result_df = (
        old_history_df.select(*old_columns)
        .unionByName(unchanged_old_current_df.select(*old_columns))
        .unionByName(expired_old_current_df.select(*old_columns))
        .unionByName(new_versions_df.select(*old_columns))
    )

    # Возвращаем только контрактные колонки витрины в исходном порядке.
    return result_df.select(*old_columns)

def check_duplicates(df, key_columns, table_name):
    # Группируем по ключевым колонкам и считаем количество строк в каждой группе.
    duplicates = df.groupBy(*key_columns).count().filter(F.col("count") > 1)
    # Запускаем Spark action count, чтобы узнать число ключей с дублями.
    duplicate_count = duplicates.count()
    # Печатаем краткую диагностику по таблице и ключу, который проверялся.
    print(f"Дубли в {table_name} по ключу {key_columns}: {duplicate_count}")
    # Показываем сами дублирующиеся ключи, если они есть.
    duplicates.show(truncate=False)
    # Возвращаем DataFrame дублей, чтобы при необходимости с ним можно было работать дальше.
    return duplicates


# Унифицированный вывод размера и первых строк таблицы упрощает демонстрацию результата в notebook.
def show_table_info(df, table_name, n=10):
    # Выводим имя таблицы перед статистикой, чтобы лог notebook было легко читать.
    print(f"\nТаблица: {table_name}")
    # Считаем строки таблицы через Spark action и печатаем результат.
    print(f"Количество строк: {df.count()}")
    # Показываем первые n строк без усечения, чтобы видеть полные значения атрибутов и ключей.
    df.show(n, truncate=False)


# Создаем все пять таблиц хранилища: два справочника, журнал и две пустые EAV-витрины.
def create_warehouse():
    """Создает пять parquet-таблиц хранилища в директории warehouse/."""
    # Гарантируем наличие корневой директории warehouse перед любой записью таблиц.
    Path(WAREHOUSE_DIR).mkdir(parents=True, exist_ok=True)

    # Читаем или создаем DataFrame справочника источников для текущего шага.
    dim_sources_df = build_dim_sources()
    # Читаем или создаем DataFrame справочника атрибутов для join при вертикализации.
    dim_attributes_df = build_dim_attributes()
    # Читаем текущий журнал загрузок, чтобы проверить идемпотентность и дописать новые записи.
    load_log_df = spark.createDataFrame([], LOAD_LOG_SCHEMA)
    # Читаем текущую monthly EAV-витрину или создаем пустую таблицу нужной схемы.
    monthly_df = spark.createDataFrame([], CLIENT_MONTHLY_SCHEMA)
    # Читаем текущую daily EAV-витрину или создаем пустую таблицу нужной схемы.
    daily_df = spark.createDataFrame([], CLIENT_DAILY_SCHEMA)

    # Записываем справочник источников в parquet-таблицу dim_sources.
    write_table(dim_sources_df, "dim_sources")
    # Записываем справочник атрибутов в parquet-таблицу dim_attributes.
    write_table(dim_attributes_df, "dim_attributes")
    # Записываем журнал загрузок после добавления записей текущего шага.
    write_table(load_log_df, "load_log")
    # Записываем актуальное состояние monthly EAV-витрины после merge.
    write_table(monthly_df, "client_monthly_attrs_scd1")
    # Записываем актуальное состояние daily EAV-витрины после merge.
    write_table(daily_df, "client_daily_attrs_scd2")

    # Показываем размер и первые строки таблицы как часть демонстрационного вывода notebook.
    show_table_info(dim_sources_df, "dim_sources")
    # Показываем размер и первые строки таблицы как часть демонстрационного вывода notebook.
    show_table_info(dim_attributes_df, "dim_attributes")
    # Сообщаем, что базовая структура warehouse создана и справочники записаны.
    print("Пустое хранилище создано.")


# Первая загрузка берет начальные партиции из SOURCE_CONFIG и записывает их в хранилище.
def initial_load_warehouse():
    """Выполняет первую загрузку начальных партиций в хранилище."""
    # Отделяем в выводе первый сценарий загрузки от создания таблиц.
    print("\n=== Первая загрузка хранилища ===")

    # Читаем или создаем DataFrame справочника атрибутов для join при вертикализации.
    dim_attributes_df = read_table_if_exists("dim_attributes")
    # Читаем текущий журнал загрузок, чтобы проверить идемпотентность и дописать новые записи.
    load_log_df = read_table_if_exists("load_log")
    # Читаем текущую monthly EAV-витрину или создаем пустую таблицу нужной схемы.
    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    # Читаем текущую daily EAV-витрину или создаем пустую таблицу нужной схемы.
    daily_df = read_table_if_exists("client_daily_attrs_scd2")

    # Обрабатываем два monthly-источника одинаковым кодом, потому что оба пишутся в одну SCD1-витрину.
    for source_name in MONTHLY_SOURCES:
        # Берем метаданные текущего источника из единого конфигурационного словаря.
        cfg = SOURCE_CONFIG[source_name]
        # Берем дату начальной партиции из конфигурации источника.
        source_report_dt = cfg["initial_partition"]
        # Определяем целевую таблицу для текущего источника из SOURCE_CONFIG.
        target_table = cfg["target_table"]

        # Перед чтением parquet проверяем load_log, чтобы повторный запуск был идемпотентным.
        if already_loaded(load_log_df, cfg["source_id"], source_report_dt, target_table):
            # Если партиция уже была успешно загружена, сообщаем об этом и не меняем витрину.
            print(f"{source_name} за {source_report_dt} уже загружен. Пропускаю.")
            # Переходим к следующему источнику: текущая партиция уже есть в load_log и повторно не загружается.
            continue

        # Фиксируем старт обработки конкретной партиции для будущей записи load_log.
        load_start = datetime.now()
        # Читаем одну исходную партицию и сразу получаем нормализованные имена/типы колонок.
        source_df = read_source_partition(source_name, cfg["partition_col"], source_report_dt)

        # Печатаем, какой источник и какая физическая партиция сейчас обрабатываются.
        print(f"\nИсточник {source_name}, партиция {cfg['partition_col']}={source_report_dt}")
        source_df.printSchema()
        # Показываем несколько исходных строк до вертикализации, чтобы было видно входной широкий формат.
        source_df.show(5, truncate=False)

        new_rows_df = verticalize_monthly(source_df, source_name, dim_attributes_df)
        print(f"Вертикализованные строки для {source_name}")
        new_rows_df.show(10, truncate=False)

        # Применяем SCD1-merge: новая monthly-партиция заменяет старую строку с тем же ключом, если такой ключ уже был.
        monthly_df = merge_scd1(monthly_df, new_rows_df)
        # Фиксируем успешную обработку партиции в load_log, чтобы следующий запуск мог ее пропустить.
        load_log_df = add_load_log_record(
            load_log_df,
            source_id=cfg["source_id"],
            source_report_dt=source_report_dt,
            target_table=target_table,
            loading_id=_source_loading_id(source_df),
            load_start_dtime=load_start,
        )

    # Берем параметры daily-источника отдельно: он обрабатывается не в monthly-цикле и пишет в SCD2-витрину.
    cfg = SOURCE_CONFIG[DAILY_SOURCE]
    # Берем дату начальной партиции из конфигурации источника.
    source_report_dt = cfg["initial_partition"]
    # Определяем целевую таблицу для текущего источника из SOURCE_CONFIG.
    target_table = cfg["target_table"]

    # Перед чтением parquet проверяем load_log, чтобы повторный запуск был идемпотентным.
    if already_loaded(load_log_df, cfg["source_id"], source_report_dt, target_table):
        # Если партиция уже была успешно загружена, сообщаем об этом и не меняем витрину.
        print(f"{DAILY_SOURCE} за {source_report_dt} уже загружен. Пропускаю.")
    # Если daily-партиция еще не загружалась, выполняем чтение, вертикализацию, merge и запись в журнал.
    else:
        # Фиксируем старт обработки конкретной партиции для будущей записи load_log.
        load_start = datetime.now()
        # Читаем одну исходную партицию и сразу получаем нормализованные имена/типы колонок.
        source_df = read_source_partition(DAILY_SOURCE, cfg["partition_col"], source_report_dt)

        # Печатаем, какой источник и какая физическая партиция сейчас обрабатываются.
        print(f"\nИсточник {DAILY_SOURCE}, партиция {cfg['partition_col']}={source_report_dt}")
        source_df.printSchema()
        # Показываем несколько исходных строк до вертикализации, чтобы было видно входной широкий формат.
        source_df.show(5, truncate=False)

        new_rows_df = verticalize_daily(source_df, DAILY_SOURCE, dim_attributes_df)
        print(f"Вертикализованные строки для {DAILY_SOURCE}")
        new_rows_df.show(10, truncate=False)

        # Применяем SCD2-merge: сохраняем одну строку на client_id + attribute_id + row_actual_from.
        daily_df = merge_scd2(daily_df, new_rows_df)
        # Фиксируем успешную обработку партиции в load_log, чтобы следующий запуск мог ее пропустить.
        load_log_df = add_load_log_record(
            load_log_df,
            source_id=cfg["source_id"],
            source_report_dt=source_report_dt,
            target_table=target_table,
            loading_id=_source_loading_id(source_df),
            load_start_dtime=load_start,
        )

    # Записываем актуальное состояние monthly EAV-витрины после merge.
    write_table(monthly_df, "client_monthly_attrs_scd1")
    # Записываем актуальное состояние daily EAV-витрины после merge.
    write_table(daily_df, "client_daily_attrs_scd2")
    # Записываем журнал загрузок после добавления записей текущего шага.
    write_table(load_log_df, "load_log")

    # Показываем размер и первые строки таблицы как часть демонстрационного вывода notebook.
    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    daily_df = read_table_if_exists("client_daily_attrs_scd2")
    load_log_df = read_table_if_exists("load_log")

    show_table_info(monthly_df, "client_monthly_attrs_scd1")
    # Показываем размер и первые строки таблицы как часть демонстрационного вывода notebook.
    show_table_info(daily_df, "client_daily_attrs_scd2")
    # Показываем размер и первые строки таблицы как часть демонстрационного вывода notebook.
    show_table_info(load_log_df, "load_log")


# Вторая загрузка имитирует обновление: берет update_partition и применяет ту же логику merge/логирования.
def update_warehouse():
    """Выполняет вторую загрузку и не загружает уже обработанные партиции повторно."""
    print("\n=== Вторая загрузка хранилища ===")

    # Читаем или создаем DataFrame справочника атрибутов для join при вертикализации.
    dim_attributes_df = read_table_if_exists("dim_attributes")
    # Читаем текущий журнал загрузок, чтобы проверить идемпотентность и дописать новые записи.
    load_log_df = read_table_if_exists("load_log")
    # Читаем текущую monthly EAV-витрину или создаем пустую таблицу нужной схемы.
    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    # Читаем текущую daily EAV-витрину или создаем пустую таблицу нужной схемы.
    daily_df = read_table_if_exists("client_daily_attrs_scd2")

    # Обрабатываем два monthly-источника одинаковым кодом, потому что оба пишутся в одну SCD1-витрину.
    for source_name in MONTHLY_SOURCES:
        # Берем метаданные текущего источника из единого конфигурационного словаря.
        cfg = SOURCE_CONFIG[source_name]
        # Берем дату обновляемой партиции из конфигурации источника.
        source_report_dt = cfg["update_partition"]
        # Определяем целевую таблицу для текущего источника из SOURCE_CONFIG.
        target_table = cfg["target_table"]

        # Перед чтением parquet проверяем load_log, чтобы повторный запуск был идемпотентным.
        if already_loaded(load_log_df, cfg["source_id"], source_report_dt, target_table):
            # Если партиция уже была успешно загружена, сообщаем об этом и не меняем витрину.
            print(f"{source_name} за {source_report_dt} уже загружен. Пропускаю.")
            # Переходим к следующему источнику: текущая партиция уже есть в load_log и повторно не загружается.
            continue

        # Фиксируем старт обработки конкретной партиции для будущей записи load_log.
        load_start = datetime.now()
        # Читаем одну исходную партицию и сразу получаем нормализованные имена/типы колонок.
        source_df = read_source_partition(source_name, cfg["partition_col"], source_report_dt)

        # Печатаем, какой источник и какая физическая партиция сейчас обрабатываются.
        print(f"\nИсточник {source_name}, партиция {cfg['partition_col']}={source_report_dt}")
        source_df.printSchema()
        # Показываем несколько исходных строк до вертикализации, чтобы было видно входной широкий формат.
        source_df.show(5, truncate=False)

        new_rows_df = verticalize_monthly(source_df, source_name, dim_attributes_df)
        print(f"Вертикализованные строки для {source_name}")
        new_rows_df.show(10, truncate=False)

        # Применяем SCD1-merge: новая monthly-партиция заменяет старую строку с тем же ключом, если такой ключ уже был.
        monthly_df = merge_scd1(monthly_df, new_rows_df)
        # Фиксируем успешную обработку партиции в load_log, чтобы следующий запуск мог ее пропустить.
        load_log_df = add_load_log_record(
            load_log_df,
            source_id=cfg["source_id"],
            source_report_dt=source_report_dt,
            target_table=target_table,
            loading_id=_source_loading_id(source_df),
            load_start_dtime=load_start,
        )

    # Берем параметры daily-источника отдельно: он обрабатывается не в monthly-цикле и пишет в SCD2-витрину.
    cfg = SOURCE_CONFIG[DAILY_SOURCE]
    # Берем дату обновляемой партиции из конфигурации источника.
    source_report_dt = cfg["update_partition"]
    # Определяем целевую таблицу для текущего источника из SOURCE_CONFIG.
    target_table = cfg["target_table"]

    # Перед чтением parquet проверяем load_log, чтобы повторный запуск был идемпотентным.
    if already_loaded(load_log_df, cfg["source_id"], source_report_dt, target_table):
        # Если партиция уже была успешно загружена, сообщаем об этом и не меняем витрину.
        print(f"{DAILY_SOURCE} за {source_report_dt} уже загружен. Пропускаю.")
    # Если daily-партиция еще не загружалась, выполняем чтение, вертикализацию, merge и запись в журнал.
    else:
        # Фиксируем старт обработки конкретной партиции для будущей записи load_log.
        load_start = datetime.now()
        # Читаем одну исходную партицию и сразу получаем нормализованные имена/типы колонок.
        source_df = read_source_partition(DAILY_SOURCE, cfg["partition_col"], source_report_dt)

        # Печатаем, какой источник и какая физическая партиция сейчас обрабатываются.
        print(f"\nИсточник {DAILY_SOURCE}, партиция {cfg['partition_col']}={source_report_dt}")
        source_df.printSchema()
        # Показываем несколько исходных строк до вертикализации, чтобы было видно входной широкий формат.
        source_df.show(5, truncate=False)

        new_rows_df = verticalize_daily(source_df, DAILY_SOURCE, dim_attributes_df)
        print(f"Вертикализованные строки для {DAILY_SOURCE}")
        new_rows_df.show(10, truncate=False)

        # Применяем SCD2-merge: сохраняем одну строку на client_id + attribute_id + row_actual_from.
        daily_df = merge_scd2(daily_df, new_rows_df)
        # Фиксируем успешную обработку партиции в load_log, чтобы следующий запуск мог ее пропустить.
        load_log_df = add_load_log_record(
            load_log_df,
            source_id=cfg["source_id"],
            source_report_dt=source_report_dt,
            target_table=target_table,
            loading_id=_source_loading_id(source_df),
            load_start_dtime=load_start,
        )

    # Записываем актуальное состояние monthly EAV-витрины после merge.
    write_table(monthly_df, "client_monthly_attrs_scd1")
    # Записываем актуальное состояние daily EAV-витрины после merge.
    write_table(daily_df, "client_daily_attrs_scd2")
    # Записываем журнал загрузок после добавления записей текущего шага.
    write_table(load_log_df, "load_log")

    # Показываем размер и первые строки таблицы как часть демонстрационного вывода notebook.
    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    daily_df = read_table_if_exists("client_daily_attrs_scd2")
    load_log_df = read_table_if_exists("load_log")

    show_table_info(monthly_df, "client_monthly_attrs_scd1")
    # Показываем размер и первые строки таблицы как часть демонстрационного вывода notebook.
    show_table_info(daily_df, "client_daily_attrs_scd2")
    # Показываем размер и первые строки таблицы как часть демонстрационного вывода notebook.
    show_table_info(load_log_df, "load_log")


# Финальная проверка выводит содержимое таблиц и контролирует referential integrity, null attribute_id и дубли.
def run_simple_checks():
    """Показывает простые проверки результата лабораторной."""
    # Отделяем блок проверок результата в общем выводе notebook.
    print("\n=== Проверки результата ===")

    # Читаем или создаем DataFrame справочника источников для текущего шага.
    dim_sources_df = read_table_if_exists("dim_sources")
    # Читаем или создаем DataFrame справочника атрибутов для join при вертикализации.
    dim_attributes_df = read_table_if_exists("dim_attributes")
    # Читаем текущий журнал загрузок, чтобы проверить идемпотентность и дописать новые записи.
    load_log_df = read_table_if_exists("load_log")
    # Читаем текущую monthly EAV-витрину или создаем пустую таблицу нужной схемы.
    monthly_df = read_table_if_exists("client_monthly_attrs_scd1")
    # Читаем текущую daily EAV-витрину или создаем пустую таблицу нужной схемы.
    daily_df = read_table_if_exists("client_daily_attrs_scd2")

    # Последовательно выводим все пять таблиц хранилища в одном формате.
    for table_name, df in [
        ("dim_sources", dim_sources_df),
        ("dim_attributes", dim_attributes_df),
        ("load_log", load_log_df),
        ("client_monthly_attrs_scd1", monthly_df),
        ("client_daily_attrs_scd2", daily_df),
    ]:
        # Показываем размер и первые строки таблицы как часть демонстрационного вывода notebook.
        show_table_info(df, table_name)

    print("\nПроверка, что все используемые source_id есть в dim_sources")
    # Собираем все source_id, которые реально встретились в справочнике атрибутов, журнале и витринах.
    source_ids_from_facts = (
        # Из справочника берем только поля, нужные для join: attribute_id, attribute_name и source_id.
        dim_attributes_df.select("source_id")
        # Добавляем source_id из журнала загрузок, чтобы проверить и технические записи.
        .unionByName(load_log_df.select("source_id"))
        # Добавляем source_id из monthly-витрины.
        .unionByName(monthly_df.select("source_id"))
        # Добавляем source_id из daily-витрины.
        .unionByName(daily_df.select("source_id"))
        # Оставляем уникальные source_id перед проверкой наличия в dim_sources.
        .distinct()
    )
    # Ищем source_id, которые используются в данных, но отсутствуют в справочнике dim_sources.
    missing_source_ids = source_ids_from_facts.join(
        # В правой части join берем только уникальные идентификаторы источников из справочника.
        dim_sources_df.select("source_id").distinct(),
        on="source_id",
        # left_anti возвращает только те source_id, которым не нашлось пары в dim_sources.
        how="left_anti",
    )
    # Показываем нарушителей referential integrity; пустой вывод означает, что проверка пройдена.
    missing_source_ids.show(truncate=False)

    print("\nПроверка null в attribute_id после вертикализации")
    # Проверяем monthly-витрину на строки, где join со справочником атрибутов не нашел attribute_id.
    monthly_df.filter(F.col("attribute_id").isNull()).show(truncate=False)
    # Проверяем daily-витрину на строки, где join со справочником атрибутов не нашел attribute_id.
    daily_df.filter(F.col("attribute_id").isNull()).show(truncate=False)

    # Запускаем проверку дублей по бизнес-ключу целевой таблицы.
    check_duplicates(
        monthly_df,
        ["client_id", "attribute_id", "report_dt"],
        "client_monthly_attrs_scd1",
    )
    # Запускаем проверку дублей по бизнес-ключу целевой таблицы.
    check_duplicates(
        daily_df,
        ["client_id", "attribute_id", "row_actual_from"],
        "client_daily_attrs_scd2",
    )

    print("\nЖурнал загрузок")
    # Сортируем журнал по load_id, чтобы история загрузок читалась в хронологическом порядке.
    load_log_df.orderBy("load_id").show(truncate=False)


# Полный линейный запуск лабораторной работы.
# Шаг 1 финального запуска: создаем физические parquet-таблицы и справочники.
create_warehouse()
# Шаг 2 финального запуска: загружаем начальные партиции источников.
initial_load_warehouse()
# Шаг 3 финального запуска: имитируем инкрементальное обновление новыми партициями.
update_warehouse()
# Шаг 4 финального запуска: выводим контрольные проверки результата.
run_simple_checks()
