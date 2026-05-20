# %% [markdown]
# # Лабораторная работа: Spark SQL — простое учебное хранилище
#
# Реализация только пунктов 1–3:
# 1) физическая модель,
# 2) построение хранилища,
# 3) обновление при второй загрузке.
#
# Код написан по секциям как в Colab/Jupyter, максимально последовательно
# и с комментарием «что делает каждая ячейка».

# %% [markdown]
# ## Секция 1. Подготовка

# Комментарий: запускаем только один раз и только нужные импорты.
from pathlib import Path
import os
import sys
from pyspark.sql import SparkSession

# Комментарий: если есть Java 17 по умолчанию, используем её.
JAVA17_HOME = Path('/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home')
if 'JAVA_HOME' not in os.environ and JAVA17_HOME.exists():
    os.environ['JAVA_HOME'] = str(JAVA17_HOME)
    os.environ['PATH'] = str(JAVA17_HOME / 'bin') + os.pathsep + os.environ['PATH']

# Комментарий: фиксируем интерпретатор для Spark.
os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)

# Комментарий: если запуск в Colab — тут можно раскомментировать монтирование Drive.
# from google.colab import drive
# drive.mount('/content/drive')

# Комментарий: определяем базовые директории.
PROJECT_DIR = Path.cwd()
if PROJECT_DIR.name == 'notebooks':
    PROJECT_DIR = PROJECT_DIR.parent

if Path('/data').exists():
    DATA_DIR = Path('/data')
else:
    DATA_DIR = PROJECT_DIR / 'data'

# Комментарий: если есть папка data/sources — берём оттуда, иначе fallback по data/test_sources.
source_root_candidates = [
    DATA_DIR / 'sources',
    DATA_DIR / 'test_sources',
    DATA_DIR,
]

SOURCE_ROOT = None
for root in source_root_candidates:
    if (root / 'credit_cards_info').exists() and (root / 'deb_cards_info').exists() and (root / 'client_cards_daily').exists():
        SOURCE_ROOT = root
        break

if SOURCE_ROOT is None:
    raise FileNotFoundError(f'Не найдены каталоги источников в {DATA_DIR} (ищу в sources/test_sources/data)')

if (Path('/warehouse').exists() and os.access('/warehouse', os.W_OK)):
    WAREHOUSE_DIR = Path('/warehouse')
else:
    WAREHOUSE_DIR = PROJECT_DIR / 'warehouse'

WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

print('PROJECT_DIR =', PROJECT_DIR)
print('DATA_DIR =', DATA_DIR)
print('SOURCE_ROOT =', SOURCE_ROOT)
print('WAREHOUSE_DIR =', WAREHOUSE_DIR)

# Комментарий: стартуем локальный Spark в учебном режиме.
spark = (
    SparkSession.builder.appName('lab3_simple_spark_sql')
    .master('local[2]')
    .config('spark.sql.shuffle.partitions', '4')
    .config('spark.sql.warehouse.dir', str((WAREHOUSE_DIR / '_spark_warehouse').resolve()))
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

DATABASE_NAME = 'lab3_warehouse'


# %% [markdown]
# ## Секция 2. Чтение исходников

# Комментарий: читаем ровно те партиции, которые заданы в задании для initial.
credit_initial_path = SOURCE_ROOT / "credit_cards_info" / "report_dt='2023-02-28'"
deb_initial_path = SOURCE_ROOT / "deb_cards_info" / "report_dt='2023-02-28'"
daily_initial_path = SOURCE_ROOT / "client_cards_daily" / "row_actual_to='2023-04-03'"

print('Initial credit:', credit_initial_path)
print('Initial deb:', deb_initial_path)
print('Initial daily:', daily_initial_path)

credit_initial = spark.read.parquet(str(credit_initial_path))
deb_initial = spark.read.parquet(str(deb_initial_path))
daily_initial = spark.read.parquet(str(daily_initial_path))

# Комментарий: показываем схемы и несколько строк для защиты.
credit_initial.printSchema()
credit_initial.show(3, truncate=False)

deb_initial.printSchema()
deb_initial.show(3, truncate=False)

daily_initial.printSchema()
daily_initial.show(3, truncate=False)

# Комментарий: все дальнейшие операции делаем через SQL по созданным view.
credit_initial.createOrReplaceTempView('src_credit_cards_info_initial')
deb_initial.createOrReplaceTempView('src_deb_cards_info_initial')
daily_initial.createOrReplaceTempView('src_client_cards_daily_initial')


# %% [markdown]
# ## Секция 3. Физическая модель и справочники

# Комментарий: создаём чистую схему в локальном каталоге.
spark.sql(f"DROP DATABASE IF EXISTS {DATABASE_NAME} CASCADE")
spark.sql(f"CREATE DATABASE {DATABASE_NAME} LOCATION '{(WAREHOUSE_DIR / '_database').as_posix()}'")

# Комментарий: таблицы хранилища (без излишних полей из вашего требования v4.1).
spark.sql(f'''
CREATE TABLE {DATABASE_NAME}.dim_sources (
    source_id INT,
    source_name STRING,
    source_description STRING,
    update_frequency STRING,
    row_create_dtime TIMESTAMP,
    row_update_dtime TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
) USING PARQUET LOCATION '{(WAREHOUSE_DIR / 'dim_sources').as_posix()}'
''')

spark.sql(f'''
CREATE TABLE {DATABASE_NAME}.dim_attributes (
    attribute_id INT,
    attribute_name STRING,
    attribute_description STRING,
    data_type STRING,
    source_id INT,
    update_frequency STRING,
    row_create_dtime TIMESTAMP,
    row_update_dtime TIMESTAMP
) USING PARQUET LOCATION '{(WAREHOUSE_DIR / 'dim_attributes').as_posix()}'
''')

spark.sql(f'''
CREATE TABLE {DATABASE_NAME}.load_log (
    load_id BIGINT,
    source_id INT,
    source_report_dt STRING,
    load_start_dtime TIMESTAMP,
    load_end_dtime TIMESTAMP,
    target_table STRING,
    load_status STRING,
    loading_id BIGINT,
    error_message STRING
) USING PARQUET LOCATION '{(WAREHOUSE_DIR / 'load_log').as_posix()}'
''')

spark.sql(f'''
CREATE TABLE {DATABASE_NAME}.client_monthly_attrs_scd1 (
    client_id STRING,
    attribute_id INT,
    report_dt STRING,
    attribute_value STRING,
    source_id INT,
    row_update_dtime TIMESTAMP,
    loading_id BIGINT,
    row_hash_val STRING
) USING PARQUET LOCATION '{(WAREHOUSE_DIR / 'client_monthly_attrs_scd1').as_posix()}'
''')

spark.sql(f'''
CREATE TABLE {DATABASE_NAME}.client_daily_attrs_scd2 (
    client_id STRING,
    attribute_id INT,
    attribute_value STRING,
    row_actual_from STRING,
    row_actual_to STRING,
    source_id INT,
    row_update_dtime TIMESTAMP,
    loading_id BIGINT,
    row_hash_val STRING
) USING PARQUET LOCATION '{(WAREHOUSE_DIR / 'client_daily_attrs_scd2').as_posix()}'
''')

# Комментарий: dim_sources — на основе трёх источников, с периодами действия (SCD2-совместимый формат).
spark.sql(f'''
INSERT INTO {DATABASE_NAME}.dim_sources
SELECT
    s.source_id,
    s.source_name,
    s.source_description,
    s.update_frequency,
    current_timestamp() AS row_create_dtime,
    current_timestamp() AS row_update_dtime,
    current_timestamp() AS valid_from,
    TIMESTAMP '9999-12-31 00:00:00' AS valid_to
FROM (
    VALUES
        (1, 'client_cards_daily', 'Ежедневный источник client_cards_daily', 'daily'),
        (2, 'credit_cards_info',  'Месячный источник credit_cards_info',  'monthly'),
        (3, 'deb_cards_info',     'Месячный источник deb_cards_info',     'monthly')
    ) AS s(source_id, source_name, source_description, update_frequency)
''')

# Комментарий: строим dim_attributes из схем источников,
# технические поля исключаем (id/client_id/row_* /hash/partition columns).
spark.sql('DESCRIBE src_client_cards_daily_initial').createOrReplaceTempView('descr_client_cards_daily')
spark.sql('DESCRIBE src_credit_cards_info_initial').createOrReplaceTempView('descr_credit_cards_info')
spark.sql('DESCRIBE src_deb_cards_info_initial').createOrReplaceTempView('descr_deb_cards_info')

spark.sql('''
CREATE OR REPLACE TEMP VIEW source_business_columns AS
SELECT
    1 AS source_id,
    TRIM(col_name) AS attribute_name,
    TRIM(data_type) AS data_type
FROM descr_client_cards_daily
WHERE TRIM(col_name) NOT IN (
    'id', 'client_id', 'row_update_dtime', 'loading_id', 'row_hash_val',
    'row_actual_from', 'row_actual_to'
)
UNION ALL
SELECT 2, TRIM(col_name), TRIM(data_type)
FROM descr_credit_cards_info
WHERE TRIM(col_name) NOT IN (
    'id', 'client_id', 'row_update_dtime', 'loading_id', 'row_hash_val',
    'report_dt'
)
UNION ALL
SELECT 3, TRIM(col_name), TRIM(data_type)
FROM descr_deb_cards_info
WHERE TRIM(col_name) NOT IN (
    'id', 'client_id', 'row_update_dtime', 'loading_id', 'row_hash_val',
    'report_dt'
)
AND TRIM(col_name) <> ''
''')

spark.sql(f'''
INSERT INTO {DATABASE_NAME}.dim_attributes
SELECT
    ROW_NUMBER() OVER (ORDER BY source_id, attribute_name) AS attribute_id,
    attribute_name,
    CONCAT('Атрибут из источника ', source_name),
    data_type,
    source_id,
    CASE source_id WHEN 1 THEN 'daily' ELSE 'monthly' END AS update_frequency,
    current_timestamp() AS row_create_dtime,
    current_timestamp() AS row_update_dtime
FROM (
    SELECT
        s.source_name,
        b.source_id,
        b.attribute_name,
        b.data_type
    FROM source_business_columns b
    LEFT JOIN (
        VALUES
            (1, 'client_cards_daily'),
            (2, 'credit_cards_info'),
            (3, 'deb_cards_info')
    ) AS s(source_id, source_name)
    ON b.source_id = s.source_id
) t
''')

print('dim_sources:')
spark.sql(f'SELECT * FROM {DATABASE_NAME}.dim_sources ORDER BY source_id').show(truncate=False)

print('dim_attributes:')
spark.sql(f'SELECT * FROM {DATABASE_NAME}.dim_attributes ORDER BY attribute_id').show(truncate=False)


# %% [markdown]
# ## Секция 4. Вертикализация данных

# Комментарий: во всех исходниках используется id, приводим его к client_id в единый вид.
spark.sql(f'''
CREATE OR REPLACE TEMP VIEW credit_initial_wide AS
SELECT
    CAST(id AS STRING) AS client_id,
    client_income_amt,
    oi_total_amt,
    act_pl_os_rub_amt,
    payroll_client_nflag,
    inf_payroll_rub_amt,
    legal_entity_amt,
    inc_avg_risk_rub_amt,
    otf_loan_rub_amt,
    otf_fee_rub_amt,
    inf_transfer_rub_amt,
    cc_ever_nflag,
    CAST(row_update_dtime AS TIMESTAMP) AS row_update_dtime,
    CAST(loading_id AS BIGINT) AS loading_id,
    CAST(row_hash_val AS STRING) AS row_hash_val
FROM src_credit_cards_info_initial
''')

spark.sql(f'''
CREATE OR REPLACE TEMP VIEW deb_initial_wide AS
SELECT
    CAST(id AS STRING) AS client_id,
    onl_bank_active_1m_nfalg,
    auto_pay_active_qty,
    cl_income_1m_amt,
    dep_acc_1st_open_dt,
    wdr_cash_6m_amt,
    cash_op_6m_amt,
    cash_3m_qty,
    lst_balance_amt,
    card_active_1m_nflag,
    CAST(row_update_dtime AS TIMESTAMP) AS row_update_dtime,
    CAST(loading_id AS BIGINT) AS loading_id,
    CAST(row_hash_val AS STRING) AS row_hash_val
FROM src_deb_cards_info_initial
''')

spark.sql(f'''
CREATE OR REPLACE TEMP VIEW daily_initial_wide AS
SELECT
    CAST(id AS STRING) AS client_id,
    srv_mb_nflag,
    cc_stoplist,
    lne_tot_debt_int_ovrd_rub_amt,
    lne_tot_debt_ovrd_rub_amt,
    CAST(row_actual_from AS STRING) AS row_actual_from,
    CAST(row_actual_to AS STRING) AS row_actual_to,
    CAST(row_update_dtime AS TIMESTAMP) AS row_update_dtime,
    CAST(loading_id AS BIGINT) AS loading_id,
    CAST(row_hash_val AS STRING) AS row_hash_val
FROM src_client_cards_daily_initial
''')

# Комментарий: широкие таблицы превращаем в вертикальные через LATERAL VIEW STACK.
spark.sql(f'''
CREATE OR REPLACE TEMP VIEW monthly_initial_credit AS
SELECT
    x.client_id,
    a.attribute_id,
    '2023-02-28' AS report_dt,
    x.attribute_value,
    2 AS source_id,
    x.row_update_dtime,
    x.loading_id,
    x.row_hash_val
    FROM (
        SELECT
            w.client_id,
            w.row_update_dtime,
            w.loading_id,
            w.row_hash_val,
            v.attribute_name,
            v.attribute_value
        FROM credit_initial_wide w
        LATERAL VIEW STACK(
            11,
            'client_income_amt', CAST(w.client_income_amt AS STRING),
            'oi_total_amt', CAST(w.oi_total_amt AS STRING),
            'act_pl_os_rub_amt', CAST(w.act_pl_os_rub_amt AS STRING),
            'payroll_client_nflag', CAST(w.payroll_client_nflag AS STRING),
            'inf_payroll_rub_amt', CAST(w.inf_payroll_rub_amt AS STRING),
            'legal_entity_amt', CAST(w.legal_entity_amt AS STRING),
            'inc_avg_risk_rub_amt', CAST(w.inc_avg_risk_rub_amt AS STRING),
            'otf_loan_rub_amt', CAST(w.otf_loan_rub_amt AS STRING),
            'otf_fee_rub_amt', CAST(w.otf_fee_rub_amt AS STRING),
            'inf_transfer_rub_amt', CAST(w.inf_transfer_rub_amt AS STRING),
            'cc_ever_nflag', CAST(w.cc_ever_nflag AS STRING)
        ) v AS attribute_name, attribute_value
) x
LEFT JOIN {DATABASE_NAME}.dim_attributes a
    ON a.source_id = 2 AND a.attribute_name = x.attribute_name
''')

spark.sql(f'''
CREATE OR REPLACE TEMP VIEW monthly_initial_debit AS
SELECT
    x.client_id,
    a.attribute_id,
    '2023-02-28' AS report_dt,
    x.attribute_value,
    3 AS source_id,
    x.row_update_dtime,
    x.loading_id,
    x.row_hash_val
FROM (
    SELECT
        w.client_id,
        w.row_update_dtime,
        w.loading_id,
        w.row_hash_val,
        v.attribute_name,
        v.attribute_value
    FROM deb_initial_wide w
    LATERAL VIEW STACK(
        9,
        'onl_bank_active_1m_nfalg', CAST(w.onl_bank_active_1m_nfalg AS STRING),
        'auto_pay_active_qty', CAST(w.auto_pay_active_qty AS STRING),
        'cl_income_1m_amt', CAST(w.cl_income_1m_amt AS STRING),
        'dep_acc_1st_open_dt', CAST(w.dep_acc_1st_open_dt AS STRING),
        'wdr_cash_6m_amt', CAST(w.wdr_cash_6m_amt AS STRING),
        'cash_op_6m_amt', CAST(w.cash_op_6m_amt AS STRING),
        'cash_3m_qty', CAST(w.cash_3m_qty AS STRING),
        'lst_balance_amt', CAST(w.lst_balance_amt AS STRING),
        'card_active_1m_nflag', CAST(w.card_active_1m_nflag AS STRING)
    ) v AS attribute_name, attribute_value
) x
LEFT JOIN {DATABASE_NAME}.dim_attributes a
    ON a.source_id = 3 AND a.attribute_name = x.attribute_name
''')

spark.sql(f'''
CREATE OR REPLACE TEMP VIEW daily_initial_attr AS
SELECT
    x.client_id,
    a.attribute_id,
    x.attribute_value,
    x.row_actual_from,
    x.row_actual_to,
    1 AS source_id,
    x.row_update_dtime,
    x.loading_id,
    x.row_hash_val
FROM (
    SELECT
        w.client_id,
        w.row_actual_from,
        w.row_actual_to,
        w.row_update_dtime,
        w.loading_id,
        w.row_hash_val,
        v.attribute_name,
        v.attribute_value
    FROM daily_initial_wide w
    LATERAL VIEW STACK(
        4,
        'srv_mb_nflag', CAST(w.srv_mb_nflag AS STRING),
        'cc_stoplist', CAST(w.cc_stoplist AS STRING),
        'lne_tot_debt_int_ovrd_rub_amt', CAST(w.lne_tot_debt_int_ovrd_rub_amt AS STRING),
        'lne_tot_debt_ovrd_rub_amt', CAST(w.lne_tot_debt_ovrd_rub_amt AS STRING)
    ) v AS attribute_name, attribute_value
) x
LEFT JOIN {DATABASE_NAME}.dim_attributes a
    ON a.source_id = 1 AND a.attribute_name = x.attribute_name
''')


# %% [markdown]
# ## Секция 5. Первая загрузка

# Комментарий: пишем начальные данные в витрины и фиксируем их в load_log.
spark.sql(f'INSERT INTO {DATABASE_NAME}.client_monthly_attrs_scd1 SELECT * FROM monthly_initial_credit')
spark.sql(f'INSERT INTO {DATABASE_NAME}.client_monthly_attrs_scd1 SELECT * FROM monthly_initial_debit')
spark.sql(f'INSERT INTO {DATABASE_NAME}.client_daily_attrs_scd2 SELECT * FROM daily_initial_attr')

spark.sql(f'''
INSERT INTO {DATABASE_NAME}.load_log
SELECT
    CAST(unix_micros(current_timestamp()) + 1 AS BIGINT) AS load_id,
    2 AS source_id,
    '2023-02-28' AS source_report_dt,
    current_timestamp() AS load_start_dtime,
    current_timestamp() AS load_end_dtime,
    'client_monthly_attrs_scd1' AS target_table,
    'SUCCESS' AS load_status,
    COALESCE(MAX(loading_id), 0) AS loading_id,
    CAST(NULL AS STRING) AS error_message
FROM monthly_initial_credit
UNION ALL
SELECT
    CAST(unix_micros(current_timestamp()) + 2 AS BIGINT),
    3,
    '2023-02-28',
    current_timestamp(),
    current_timestamp(),
    'client_monthly_attrs_scd1',
    'SUCCESS',
    COALESCE(MAX(loading_id), 0),
    CAST(NULL AS STRING)
FROM monthly_initial_debit
UNION ALL
SELECT
    CAST(unix_micros(current_timestamp()) + 3 AS BIGINT),
    1,
    '2023-04-03',
    current_timestamp(),
    current_timestamp(),
    'client_daily_attrs_scd2',
    'SUCCESS',
    COALESCE(MAX(loading_id), 0),
    CAST(NULL AS STRING)
FROM daily_initial_attr
''')

print('Первые 20 записей client_monthly_attrs_scd1:')
spark.sql(f'SELECT * FROM {DATABASE_NAME}.client_monthly_attrs_scd1 ORDER BY client_id, attribute_id, report_dt LIMIT 20').show(truncate=False)
print('Первые 20 записей client_daily_attrs_scd2:')
spark.sql(f'SELECT * FROM {DATABASE_NAME}.client_daily_attrs_scd2 ORDER BY client_id, attribute_id, row_actual_from LIMIT 20').show(truncate=False)


# %% [markdown]
# ## Секция 6. Вторая загрузка

# Комментарий: читаем заданные партиции для update.
credit_update_path = SOURCE_ROOT / "credit_cards_info" / "report_dt='2023-03-31'"
deb_update_path = SOURCE_ROOT / "deb_cards_info" / "report_dt='2023-03-31'"
daily_update_path = SOURCE_ROOT / "client_cards_daily" / "row_actual_to='9999-12-31'"

# Комментарий: проверяем, не грузили ли уже этот источник+дата+таблицу успешно.
credit_loaded = (
    spark.sql(f'''
    SELECT COUNT(1) FROM {DATABASE_NAME}.load_log
    WHERE source_id = 2
      AND source_report_dt = '2023-03-31'
      AND target_table = 'client_monthly_attrs_scd1'
      AND load_status = 'SUCCESS'
    ''').first()[0] > 0
)

deb_loaded = (
    spark.sql(f'''
    SELECT COUNT(1) FROM {DATABASE_NAME}.load_log
    WHERE source_id = 3
      AND source_report_dt = '2023-03-31'
      AND target_table = 'client_monthly_attrs_scd1'
      AND load_status = 'SUCCESS'
    ''').first()[0] > 0
)

daily_loaded = (
    spark.sql(f'''
    SELECT COUNT(1) FROM {DATABASE_NAME}.load_log
    WHERE source_id = 1
      AND source_report_dt = '9999-12-31'
      AND target_table = 'client_daily_attrs_scd2'
      AND load_status = 'SUCCESS'
    ''').first()[0] > 0
)

print('credit update already loaded:', credit_loaded)
print('deb update already loaded:', deb_loaded)
print('daily update already loaded:', daily_loaded)

# Комментарий: если уже есть SUCCESS в load_log, для конкретного источника ничего не делаем.
if not credit_loaded:
    credit_update = spark.read.parquet(str(credit_update_path))
    credit_update.createOrReplaceTempView('src_credit_cards_info_update')
    spark.sql('''
    CREATE OR REPLACE TEMP VIEW monthly_update_credit AS
    SELECT
        t.client_id,
        a.attribute_id,
        '2023-03-31' AS report_dt,
        t.attribute_value,
        2 AS source_id,
        t.row_update_dtime,
        t.loading_id,
        t.row_hash_val
        FROM (
            SELECT
                CAST(id AS STRING) AS client_id,
                CAST(row_update_dtime AS TIMESTAMP) AS row_update_dtime,
                CAST(loading_id AS BIGINT) AS loading_id,
                CAST(row_hash_val AS STRING) AS row_hash_val,
                v.attribute_name,
                v.attribute_value
            FROM src_credit_cards_info_update
            LATERAL VIEW STACK(
                11,
                'client_income_amt', CAST(client_income_amt AS STRING),
                'oi_total_amt', CAST(oi_total_amt AS STRING),
                'act_pl_os_rub_amt', CAST(act_pl_os_rub_amt AS STRING),
                'payroll_client_nflag', CAST(payroll_client_nflag AS STRING),
                'inf_payroll_rub_amt', CAST(inf_payroll_rub_amt AS STRING),
                'legal_entity_amt', CAST(legal_entity_amt AS STRING),
                'inc_avg_risk_rub_amt', CAST(inc_avg_risk_rub_amt AS STRING),
                'otf_loan_rub_amt', CAST(otf_loan_rub_amt AS STRING),
                'otf_fee_rub_amt', CAST(otf_fee_rub_amt AS STRING),
                'inf_transfer_rub_amt', CAST(inf_transfer_rub_amt AS STRING),
            'cc_ever_nflag', CAST(cc_ever_nflag AS STRING)
        ) v AS attribute_name, attribute_value
    ) t
    LEFT JOIN {DATABASE_NAME}.dim_attributes a
        ON a.source_id = 2 AND a.attribute_name = t.attribute_name
    '''.format(DATABASE_NAME=DATABASE_NAME))
else:
    spark.sql('CREATE OR REPLACE TEMP VIEW monthly_update_credit AS SELECT * FROM monthly_initial_credit WHERE 1=0')

if not deb_loaded:
    deb_update = spark.read.parquet(str(deb_update_path))
    deb_update.createOrReplaceTempView('src_deb_cards_info_update')
    spark.sql('''
    CREATE OR REPLACE TEMP VIEW monthly_update_debit AS
    SELECT
        t.client_id,
        a.attribute_id,
        '2023-03-31' AS report_dt,
        t.attribute_value,
        3 AS source_id,
        t.row_update_dtime,
        t.loading_id,
        t.row_hash_val
    FROM (
        SELECT
            CAST(id AS STRING) AS client_id,
            CAST(row_update_dtime AS TIMESTAMP) AS row_update_dtime,
            CAST(loading_id AS BIGINT) AS loading_id,
            CAST(row_hash_val AS STRING) AS row_hash_val,
            v.attribute_name,
            v.attribute_value
        FROM src_deb_cards_info_update
        LATERAL VIEW STACK(
            9,
            'onl_bank_active_1m_nfalg', CAST(onl_bank_active_1m_nfalg AS STRING),
            'auto_pay_active_qty', CAST(auto_pay_active_qty AS STRING),
            'cl_income_1m_amt', CAST(cl_income_1m_amt AS STRING),
            'dep_acc_1st_open_dt', CAST(dep_acc_1st_open_dt AS STRING),
            'wdr_cash_6m_amt', CAST(wdr_cash_6m_amt AS STRING),
            'cash_op_6m_amt', CAST(cash_op_6m_amt AS STRING),
            'cash_3m_qty', CAST(cash_3m_qty AS STRING),
            'lst_balance_amt', CAST(lst_balance_amt AS STRING),
            'card_active_1m_nflag', CAST(card_active_1m_nflag AS STRING)
        ) v AS attribute_name, attribute_value
    ) t
    LEFT JOIN {DATABASE_NAME}.dim_attributes a
        ON a.source_id = 3 AND a.attribute_name = t.attribute_name
    '''.format(DATABASE_NAME=DATABASE_NAME))
else:
    spark.sql('CREATE OR REPLACE TEMP VIEW monthly_update_debit AS SELECT * FROM monthly_initial_debit WHERE 1=0')

if not daily_loaded:
    daily_update = spark.read.parquet(str(daily_update_path))
    daily_update.createOrReplaceTempView('src_client_cards_daily_update')
    spark.sql(f'''
    CREATE OR REPLACE TEMP VIEW daily_update AS
    SELECT
        t.client_id,
        a.attribute_id,
        t.attribute_value,
        t.row_actual_from,
        t.row_actual_to,
        1 AS source_id,
        t.row_update_dtime,
        t.loading_id,
        t.row_hash_val
    FROM (
        SELECT
            CAST(id AS STRING) AS client_id,
            CAST(row_actual_from AS STRING) AS row_actual_from,
            CAST(row_actual_to AS STRING) AS row_actual_to,
            CAST(row_update_dtime AS TIMESTAMP) AS row_update_dtime,
            CAST(loading_id AS BIGINT) AS loading_id,
            CAST(row_hash_val AS STRING) AS row_hash_val,
            v.attribute_name,
            v.attribute_value
        FROM src_client_cards_daily_update
        LATERAL VIEW STACK(
            4,
            'srv_mb_nflag', CAST(srv_mb_nflag AS STRING),
            'cc_stoplist', CAST(cc_stoplist AS STRING),
            'lne_tot_debt_int_ovrd_rub_amt', CAST(lne_tot_debt_int_ovrd_rub_amt AS STRING),
            'lne_tot_debt_ovrd_rub_amt', CAST(lne_tot_debt_ovrd_rub_amt AS STRING)
        ) v AS attribute_name, attribute_value
    ) t
    LEFT JOIN {DATABASE_NAME}.dim_attributes a
        ON a.source_id = 1 AND a.attribute_name = t.attribute_name
    '''.format(DATABASE_NAME=DATABASE_NAME))
else:
    spark.sql('CREATE OR REPLACE TEMP VIEW daily_update AS SELECT * FROM daily_initial_attr WHERE 1=0')


# Комментарий: объединяем старые + новые версии и убираем дубли по бизнес-ключам.
spark.sql(f'''
CREATE OR REPLACE TEMP VIEW monthly_for_save AS
SELECT * FROM {DATABASE_NAME}.client_monthly_attrs_scd1
UNION ALL
SELECT * FROM monthly_update_credit
UNION ALL
SELECT * FROM monthly_update_debit
''')

spark.sql(f'''
CREATE OR REPLACE TEMP VIEW client_monthly_attrs_scd1_new AS
SELECT
    client_id, attribute_id, report_dt, attribute_value, source_id, row_update_dtime, loading_id, row_hash_val
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY client_id, attribute_id, report_dt
            ORDER BY row_update_dtime DESC, loading_id DESC
        ) AS rn
    FROM monthly_for_save
) t
WHERE rn = 1
''')

spark.sql(f'''
CREATE OR REPLACE TEMP VIEW daily_for_save AS
SELECT * FROM {DATABASE_NAME}.client_daily_attrs_scd2
UNION ALL
SELECT * FROM daily_update
''')

spark.sql(f'''
CREATE OR REPLACE TEMP VIEW client_daily_attrs_scd2_new AS
SELECT
    client_id, attribute_id, attribute_value, row_actual_from, row_actual_to, source_id, row_update_dtime, loading_id, row_hash_val
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY client_id, attribute_id, row_actual_from
            ORDER BY row_update_dtime DESC, loading_id DESC
        ) AS rn
    FROM daily_for_save
) t
WHERE rn = 1
''')

# Комментарий: перезаписываем итоговые таблицы (учебный вариант update).
spark.sql(f'INSERT OVERWRITE TABLE {DATABASE_NAME}.client_monthly_attrs_scd1 SELECT * FROM client_monthly_attrs_scd1_new')
spark.sql(f'INSERT OVERWRITE TABLE {DATABASE_NAME}.client_daily_attrs_scd2 SELECT * FROM client_daily_attrs_scd2_new')

# Комментарий: пишем log только для реально выгруженных блоков.
if not credit_loaded:
    spark.sql(f'''
    INSERT INTO {DATABASE_NAME}.load_log
    SELECT
        CAST(unix_micros(current_timestamp()) + 10 AS BIGINT),
        2,
        '2023-03-31',
        current_timestamp(),
        current_timestamp(),
        'client_monthly_attrs_scd1',
        'SUCCESS',
        COALESCE(MAX(loading_id), 0),
        CAST(NULL AS STRING)
    FROM monthly_update_credit
    ''')

if not deb_loaded:
    spark.sql(f'''
    INSERT INTO {DATABASE_NAME}.load_log
    SELECT
        CAST(unix_micros(current_timestamp()) + 20 AS BIGINT),
        3,
        '2023-03-31',
        current_timestamp(),
        current_timestamp(),
        'client_monthly_attrs_scd1',
        'SUCCESS',
        COALESCE(MAX(loading_id), 0),
        CAST(NULL AS STRING)
    FROM monthly_update_debit
    ''')

if not daily_loaded:
    spark.sql(f'''
    INSERT INTO {DATABASE_NAME}.load_log
    SELECT
        CAST(unix_micros(current_timestamp()) + 30 AS BIGINT),
        1,
        '9999-12-31',
        current_timestamp(),
        current_timestamp(),
        'client_daily_attrs_scd2',
        'SUCCESS',
        COALESCE(MAX(loading_id), 0),
        CAST(NULL AS STRING)
    FROM daily_update
    ''')

print('После update: первые 20 в client_monthly_attrs_scd1')
spark.sql(f'SELECT * FROM {DATABASE_NAME}.client_monthly_attrs_scd1 ORDER BY client_id, attribute_id, report_dt LIMIT 20').show(truncate=False)
print('После update: первые 20 в client_daily_attrs_scd2')
spark.sql(f'SELECT * FROM {DATABASE_NAME}.client_daily_attrs_scd2 ORDER BY client_id, attribute_id, row_actual_from LIMIT 20').show(truncate=False)


# %% [markdown]
# ## Секция 7. Мини-проверки

# Комментарий: базовые проверки и диагностика.
spark.sql(f'''
SELECT 'dim_sources' AS table_name, COUNT(*) AS cnt FROM {DATABASE_NAME}.dim_sources
UNION ALL SELECT 'dim_attributes', COUNT(*) FROM {DATABASE_NAME}.dim_attributes
UNION ALL SELECT 'load_log', COUNT(*) FROM {DATABASE_NAME}.load_log
UNION ALL SELECT 'client_monthly_attrs_scd1', COUNT(*) FROM {DATABASE_NAME}.client_monthly_attrs_scd1
UNION ALL SELECT 'client_daily_attrs_scd2', COUNT(*) FROM {DATABASE_NAME}.client_daily_attrs_scd2
''').show(truncate=False)

spark.sql(f'''
SELECT
    'monthly_null_attribute_id' AS check_name,
    COUNT(*) AS bad_rows
FROM {DATABASE_NAME}.client_monthly_attrs_scd1
WHERE attribute_id IS NULL
UNION ALL
SELECT 'daily_null_attribute_id', COUNT(*)
FROM {DATABASE_NAME}.client_daily_attrs_scd2
WHERE attribute_id IS NULL
''').show(truncate=False)

spark.sql(f'''
SELECT client_id, attribute_id, report_dt, COUNT(*) AS cnt
FROM {DATABASE_NAME}.client_monthly_attrs_scd1
GROUP BY client_id, attribute_id, report_dt
HAVING COUNT(*) > 1
''').show(truncate=False)

spark.sql(f'''
SELECT client_id, attribute_id, row_actual_from, COUNT(*) AS cnt
FROM {DATABASE_NAME}.client_daily_attrs_scd2
GROUP BY client_id, attribute_id, row_actual_from
HAVING COUNT(*) > 1
''').show(truncate=False)

print('load_log:')
spark.sql(f'SELECT * FROM {DATABASE_NAME}.load_log ORDER BY source_id, source_report_dt, load_id').show(truncate=False)

# Проверяем, что source_id из фактов есть в справочнике источников.
spark.sql(f'''
SELECT DISTINCT source_id
FROM {DATABASE_NAME}.client_monthly_attrs_scd1
WHERE source_id NOT IN (SELECT source_id FROM {DATABASE_NAME}.dim_sources)
UNION ALL
SELECT DISTINCT source_id
FROM {DATABASE_NAME}.client_daily_attrs_scd2
WHERE source_id NOT IN (SELECT source_id FROM {DATABASE_NAME}.dim_sources)
''').show(truncate=False)


# %% [markdown]
# ## Секция 8. Краткое объяснение
#
# `dim_sources` — справочник источников:
# из каких raw-источников пришли атрибуты и как часто они обновляются.
#
# `dim_attributes` — справочник бизнес-атрибутов клиента:
# только бизнес-поля без технических колонок (`row_update_dtime`, `loading_id`, ...).
#
# `load_log` — журнал загрузок:
# не даёт перезагрузить одну и ту же партию дважды (по комбинации
# `source_id + source_report_dt + target_table` с `load_status='SUCCESS'`).
#
# `client_monthly_attrs_scd1` — вертикальная таблица месячных атрибутов (SCD1):
# ключ — `(client_id, attribute_id, report_dt)`, т.е. на один месяц остаётся 1 актуальная запись.
#
# `client_daily_attrs_scd2` — вертикальная таблица ежедневных атрибутов (SCD2):
# ключ — `(client_id, attribute_id, row_actual_from)`, переносим интервалы `row_actual_from`/`row_actual_to`.
#
# Имитация обновления: сначала initial-пакет, потом update-пакет;
# при update сначала проверяем `load_log`, потом добавляем, дедупим по ключу.
