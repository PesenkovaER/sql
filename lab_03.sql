-- Задание 1: Найти количество клиентов из штата 'CA'
SELECT COUNT(*) AS customer_count
FROM customers
WHERE state = 'CA';


-- Задание 2: Вывести количество продаж по годам
SELECT 
    EXTRACT(YEAR FROM sales_transaction_date) AS year,
    COUNT(*) AS sales_count
FROM sales
GROUP BY EXTRACT(YEAR FROM sales_transaction_date)
ORDER BY year;


-- Задание 3: Вывести категории товаров, в которых представлено более 5 моделей
SELECT 
    product_type,
    COUNT(*) AS model_count
FROM products
GROUP BY product_type
HAVING COUNT(*) > 5;