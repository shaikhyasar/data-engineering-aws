SELECT 
    tr.transaction_id,
    tr.customer_id,
    tr.quantity,
    tr.unit_price,
    tr.description AS product_description,
    tr.invoice_date AS datetime,
    con.country_2_alpha
FROM transactions tr
JOIN customers cus
ON tr.customer_id = cus.customer_id
JOIN countries con
ON con.country_name = cus.customer_country