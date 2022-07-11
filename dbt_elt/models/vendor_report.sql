WITH customer_report AS (
    SELECT * FROM {{ ref('fct_debit') }}
    WHERE 
)

SELECT * FROM customer_report
ORDER BY created_at