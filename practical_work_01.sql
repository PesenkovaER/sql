-- Сезонность продуктов. Для каждого типа продукта (product_type) определите месяц, в котором он продается лучше всего (максимальная сумма продаж).
WITH monthly_sales AS (
    SELECT 
        p.product_type,
        DATE_TRUNC('month', s.sales_transaction_date) AS month_date,
        SUM(s.sales_amount) AS total_sales
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY p.product_type, DATE_TRUNC('month', s.sales_transaction_date)
),
ranked AS (
    SELECT 
        product_type,
        month_date,
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY product_type ORDER BY total_sales DESC) AS rn
    FROM monthly_sales
)
SELECT 
    product_type,
    TO_CHAR(month_date, 'YYYY-MM') AS best_month,
    total_sales AS max_sales_amount
FROM ranked
WHERE rn = 1
ORDER BY product_type;

-- Покрытие дилеров. Найдите дилерский центр, у которого наибольшее количество клиентов в радиусе 100 миль.
WITH dealer_customer_distance AS (
    SELECT 
        d.dealership_id,
        c.customer_id,
        point(c.longitude, c.latitude) <@> point(d.longitude, d.latitude) AS distance_miles
    FROM dealerships d
    CROSS JOIN customers c
),
dealer_nearby_counts AS (
    SELECT 
        dealership_id,
        COUNT(customer_id) AS customers_within_100_miles
    FROM dealer_customer_distance
    WHERE distance_miles <= 100
    GROUP BY dealership_id
)
SELECT 
    dealership_id,
    customers_within_100_miles
FROM dealer_nearby_counts
ORDER BY customers_within_100_miles DESC
LIMIT 1;

-- Извлечение из JSON. Из таблицы customer_sales извлеките поле sales (массив внутри JSON), разверните его (jsonb_array_elements) и посчитайте общую сумму продаж, хранящуюся внутри JSON.
SELECT SUM((elem ->> 'sales_amount')::numeric) AS total_sales_amount
FROM customer_sales,
     jsonb_array_elements(customer_json -> 'sales') AS elem;