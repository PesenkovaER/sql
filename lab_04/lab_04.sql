-- Задание 1. 5 самых последних зарегистрированных клиентов в каждом штате
SELECT customer_id, first_name, last_name, state, date_added
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY state ORDER BY date_added DESC) AS rn
    FROM customers
) t
WHERE rn <= 5;


-- Задание 2. Разделить продажи на 5 групп по величине суммы сделки
SELECT sales_amount, NTILE(5) OVER (ORDER BY sales_amount) AS group_number
FROM sales
WHERE sales_amount IS NOT NULL;


-- Задание 3. Скользящее среднее продаж за 10 последних транзакций для dealership_id = 1
SELECT sales_transaction_date, sales_amount, AVG(sales_amount) OVER (ORDER BY sales_transaction_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS moving_avg_10
FROM sales
WHERE dealership_id = 1 AND sales_amount IS NOT NULL
ORDER BY sales_transaction_date;
