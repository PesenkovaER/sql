-- ЗАДАНИЕ 1. Использование JOIN
-- Вывести все продажи через интернет (модель, цена, дата)
SELECT 
    p.model,
    s.sales_amount,
    s.sales_transaction_date
FROM sales s
INNER JOIN products p ON s.product_id = p.product_id
WHERE s.channel = 'internet'
ORDER BY s.sales_transaction_date DESC;


-- ЗАДАНИЕ 2. Использование UNION ALL
-- Объединить dealership_id из sales и dealerships с указанием источника
SELECT 
    dealership_id,
    'from_sales' AS source
FROM sales
WHERE dealership_id IS NOT NULL

UNION ALL

SELECT 
    dealership_id,
    'from_dealerships' AS source
FROM dealerships
ORDER BY dealership_id;


-- ЗАДАНИЕ 3. Преобразование данных (CASE)
-- Классифицировать продажи по сумме
SELECT 
    s.customer_id,
    s.sales_amount,
    CASE 
        WHEN s.sales_amount < 1000 THEN 'Small'
        WHEN s.sales_amount BETWEEN 1000 AND 10000 THEN 'Medium'
        WHEN s.sales_amount > 10000 THEN 'Large'
        ELSE 'Unknown'
    END AS sales_category
FROM sales s
ORDER BY s.sales_amount DESC;
