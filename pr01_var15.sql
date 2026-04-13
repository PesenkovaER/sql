-- =====================================================
-- Задание 1. Таблица продаж с данными за 3 месяца
-- =====================================================

CREATE TABLE sales_var15 (
    sale_id        UInt64,
    sale_timestamp DateTime64(3),
    product_id     UInt32,
    category       LowCardinality(String),
    customer_id    UInt64,
    region         LowCardinality(String),
    quantity       UInt16,
    unit_price     Decimal64(2),
    discount_pct   Float32,
    is_online      UInt8,
    ip_address     IPv4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_timestamp)
ORDER BY (sale_timestamp, customer_id, product_id);

-- Генерация 150 строк за 3 месяца (Октябрь, Ноябрь, Декабрь 2024)
-- Параметры для варианта 15:
-- sale_id: 15001 ... 15150
-- customer_id: 1500 ... 1599
-- product_id: 150 ... 169
-- quantity: 1 ... 20 (max = 20)
-- unit_price: от 25.00
-- Категории: Office, Electronics, Clothing, Food, Pets

INSERT INTO sales_var15 
(sale_id, sale_timestamp, product_id, category, customer_id, region, quantity, unit_price, discount_pct, is_online, ip_address)
SELECT
    number + 15001 AS sale_id,
    toDateTime64('2024-10-01 00:00:00', 3) + INTERVAL (number % 90) DAY + INTERVAL (number % 24) HOUR,
    150 + (number % 20) AS product_id,
    CASE (number % 5)
        WHEN 0 THEN 'Office'
        WHEN 1 THEN 'Electronics'
        WHEN 2 THEN 'Clothing'
        WHEN 3 THEN 'Food'
        ELSE 'Pets'
    END AS category,
    1500 + (number % 100) AS customer_id,
    CASE (number % 4)
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        ELSE 'West'
    END AS region,
    (number % 20) + 1 AS quantity,
    25.00 + (number % 500) AS unit_price,
    (number % 100) / 100.0 AS discount_pct,
    (number % 2) AS is_online,
    IPv4StringToNum(concat(
        toString(number % 256), '.', 
        toString((number + 64) % 256), '.', 
        toString((number + 128) % 256), '.', 
        toString(number % 256)
    )) AS ip_address
FROM numbers(150);

-- =====================================================
-- Задание 2. Аналитические запросы
-- =====================================================

-- 2.1 Общая выручка по категориям
SELECT
    category,
    round(SUM(quantity * unit_price * (1 - discount_pct)), 2) AS total_revenue
FROM sales_var15
GROUP BY category
ORDER BY total_revenue DESC;

-- 2.2 Топ-3 клиента по количеству покупок
SELECT
    customer_id,
    COUNT(*) AS purchase_count,
    SUM(quantity) AS total_quantity
FROM sales_var15
GROUP BY customer_id
ORDER BY purchase_count DESC
LIMIT 3;

-- 2.3 Средний чек по месяцам
SELECT
    toYYYYMM(sale_timestamp) AS month,
    round(AVG(quantity * unit_price), 2) AS avg_check
FROM sales_var15
GROUP BY month
ORDER BY month;

-- 2.4 Фильтрация по партиции (октябрь 2024)
SELECT *
FROM sales_var15
WHERE sale_timestamp >= '2024-10-01' AND sale_timestamp < '2024-11-01';

-- Проверка через EXPLAIN, что используется только партиция 202410
EXPLAIN SELECT *
FROM sales_var15
WHERE sale_timestamp >= '2024-10-01' AND sale_timestamp < '2024-11-01';

-- =====================================================
-- Задание 3. ReplacingMergeTree — справочник товаров
-- =====================================================

DROP TABLE IF EXISTS products_var15;

CREATE TABLE products_var15 (
    product_id    UInt32,
    product_name  String,
    category      LowCardinality(String),
    supplier      String,
    base_price    Decimal64(2),
    weight_kg     Float32,
    is_available  UInt8,
    updated_at    DateTime,
    version       UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (product_id);

-- 3.1 Вставляем 10 товаров (NNN % 10 + 5 = 15 % 10 + 5 = 10) с version = 1
INSERT INTO products_var15 VALUES
(150, 'Notebook Pro', 'Office', 'TechCorp', 499.99, 1.5, 1, now(), 1),
(151, 'Smartphone X', 'Electronics', 'TechCorp', 699.99, 0.18, 1, now(), 1),
(152, 'T-Shirt', 'Clothing', 'FashionInc', 19.99, 0.15, 1, now(), 1),
(153, 'Apple', 'Food', 'FreshCo', 2.99, 0.15, 1, now(), 1),
(154, 'Dog Food', 'Pets', 'PetShop', 29.99, 2.5, 1, now(), 1),
(155, 'Desk Lamp', 'Office', 'HomeGoods', 34.99, 0.8, 1, now(), 1),
(156, 'Laptop', 'Electronics', 'TechCorp', 999.99, 2.2, 1, now(), 1),
(157, 'Jeans', 'Clothing', 'FashionInc', 49.99, 0.6, 1, now(), 1),
(158, 'Milk', 'Food', 'FreshCo', 1.99, 1.0, 1, now(), 1),
(159, 'Cat Toy', 'Pets', 'PetShop', 9.99, 0.1, 1, now(), 1);

-- 3.2 Для 3 товаров вставляем обновлённые записи с version = 2
INSERT INTO products_var15 VALUES
(150, 'Notebook Pro Max', 'Office', 'TechCorp', 449.99, 1.5, 0, now(), 2),
(151, 'Smartphone X Plus', 'Electronics', 'TechCorp', 649.99, 0.18, 1, now(), 2),
(156, 'Laptop Ultra', 'Electronics', 'TechCorp', 899.99, 2.2, 1, now(), 2);

-- 3.3 Проверяем — видны обе версии
SELECT * FROM products_var15;

-- 3.4 Принудительное слияние
OPTIMIZE TABLE products_var15 FINAL;

-- 3.5 Проверяем — осталась только версия 2
SELECT * FROM products_var15;

-- 3.6 Альтернатива OPTIMIZE — используем FINAL
SELECT * FROM products_var15 FINAL;

-- =====================================================
-- Задание 4. SummingMergeTree — агрегация метрик
-- =====================================================

DROP TABLE IF EXISTS daily_metrics_var15;

CREATE TABLE daily_metrics_var15 (
    metric_date    Date,
    campaign_id    UInt32,
    channel        LowCardinality(String),
    impressions    UInt64,
    clicks         UInt64,
    conversions    UInt32,
    spend_cents    UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (metric_date, campaign_id, channel);

-- Параметры для варианта 15:
-- Дней: NNN % 5 + 3 = 15 % 5 + 3 = 3 дня
-- Кампаний: NNN % 3 + 2 = 15 % 3 + 2 = 2 кампании
-- campaign_id начинается с: NNN × 10 + 1 = 151

-- 4.1 Вставляем данные за 3 дня для 2 кампаний, по 2 канала каждая (3*2*2 = 12 строк)
INSERT INTO daily_metrics_var15
SELECT
    toDate('2024-10-01') + INTERVAL (number % 3) DAY AS metric_date,
    151 + (number % 2) AS campaign_id,
    CASE (number % 2) WHEN 0 THEN 'Email' ELSE 'Social' END AS channel,
    1000 + (number % 5000) AS impressions,
    50 + (number % 300) AS clicks,
    1 + (number % 20) AS conversions,
    5000 + (number % 10000) AS spend_cents
FROM numbers(12);

-- 4.2 Вставляем повторные строки с теми же ключами (для проверки суммирования)
INSERT INTO daily_metrics_var15
SELECT
    toDate('2024-10-01') + INTERVAL (number % 3) DAY AS metric_date,
    151 + (number % 2) AS campaign_id,
    CASE (number % 2) WHEN 0 THEN 'Email' ELSE 'Social' END AS channel,
    500 + (number % 1000) AS impressions,
    10 + (number % 100) AS clicks,
    1 + (number % 10) AS conversions,
    1000 + (number % 5000) AS spend_cents
FROM numbers(6);

-- 4.3 Проверяем данные до оптимизации
SELECT COUNT(*) AS total_rows FROM daily_metrics_var15;

-- 4.4 Выполняем OPTIMIZE для принудительного суммирования
OPTIMIZE TABLE daily_metrics_var15 FINAL;

-- 4.5 Проверяем, что данные просуммировались (должно быть 3 дня × 2 кампании × 2 канала = 12 строк)
SELECT 
    metric_date,
    campaign_id,
    channel,
    impressions,
    clicks,
    conversions,
    spend_cents
FROM daily_metrics_var15
ORDER BY metric_date, campaign_id, channel;

-- 4.6 CTR (Click-Through Rate) по каналам
SELECT
    channel,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    round(SUM(clicks) / SUM(impressions), 4) AS CTR
FROM daily_metrics_var15
GROUP BY channel;

-- =====================================================
-- Задание 5. Комплексный анализ и самопроверка
-- =====================================================

-- 5.1 Проверка партиций таблицы sales_var15
SELECT
    partition,
    count() AS parts,
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE database = 'db_var15'
  AND table = 'sales_var15'
  AND active
GROUP BY partition
ORDER BY partition;

-- 5.2 JOIN между таблицами — топ-5 товаров по выручке
SELECT
    p.product_name,
    p.category,
    round(sum(s.quantity * s.unit_price * (1 - s.discount_pct)), 2) AS revenue
FROM sales_var15 AS s
INNER JOIN products_var15 AS p
    ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 5;

-- 5.3 Типы данных всех трёх таблиц
DESCRIBE TABLE sales_var15;
DESCRIBE TABLE products_var15;
DESCRIBE TABLE daily_metrics_var15;

-- 5.4 Запрос с массивом
DROP TABLE IF EXISTS tags_var15;

CREATE TABLE tags_var15 (
    item_id  UInt32,
    item_name String,
    tags     Array(String)
) ENGINE = MergeTree()
ORDER BY item_id;

INSERT INTO tags_var15 VALUES
(1, 'Item A', ['sale', 'popular', 'new']),
(2, 'Item B', ['premium', 'limited']),
(3, 'Item C', ['sale', 'clearance']);

SELECT
    arrayJoin(tags) AS tag,
    count() AS items_count
FROM tags_var15
GROUP BY tag
ORDER BY items_count DESC;

-- 5.5 Контрольная сумма
SELECT
    'sales' AS tbl, 
    count() AS rows, 
    sum(quantity) AS check_sum 
FROM db_var15.sales_var15

UNION ALL

SELECT
    'products', 
    count(), 
    sum(toUInt64(product_id)) 
FROM db_var15.products_var15 FINAL

UNION ALL

SELECT
    'metrics', 
    count(), 
    sum(clicks) 
FROM daily_metrics_var15;