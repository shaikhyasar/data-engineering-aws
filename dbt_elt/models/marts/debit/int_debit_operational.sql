WITH

transactions AS (
    SELECT
        *
    FROM {{ref('stg_transactions')}}
),

operational_wallet AS (
    SELECT
        *
    FROM {{ ref('stg_operational_wallet')}}
),

operational_debit AS (
    SELECT
        tr.id AS id,
        tw.name AS debited_from,
        11 AS debited_role,
        tr.created_at AS created_at
    FROM transactions tr
    JOIN operational_wallet tw USING (wallet_id_id)
)

SELECT * FROM operational_debit