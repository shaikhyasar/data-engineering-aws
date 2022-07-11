WITH

transactions AS (
    SELECT
        *
    FROM {{ref('stg_transactions')}}
),

non_wallet AS (
    SELECT
        *
    FROM {{ ref('stg_non_user_wallet')}}
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

non_wallet_credit AS (
    SELECT
        tr.id AS id,
        tw.mobile_no AS credited_to,
        9 AS credited_role,
        tr.created_at AS created_at
    FROM wallet_to_wallet_obj tr
    JOIN non_wallet tw USING (wallet_id_id)
)

SELECT * FROM non_wallet_credit
