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
wallet_to_wallet AS (
    SELECT 
        *
    FROM {{ ref('stg_transactions_wallet_to_wallet')}}
),
wallet_to_wallet_obj AS (
    SELECT
        tr.id,
        tr.created_at,
        ww.wallet_id_id
    FROM transactions tr
    JOIN wallet_to_wallet ww
    ON tr.id = ww.transaction_id_id

),
operational_credit AS (
    SELECT
        tr.id AS id,
        tw.name AS credited_to,
        11 AS credited_role,
        tr.created_at AS created_at
    FROM wallet_to_wallet_obj tr
    JOIN operational_wallet tw USING (wallet_id_id)
)

SELECT * FROM operational_credit