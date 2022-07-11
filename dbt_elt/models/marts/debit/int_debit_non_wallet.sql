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

non_wallet_debit AS (
    SELECT
        tr.id AS id,
        tw.mobile_no AS debited_from,
        9 AS debited_role,
        tr.created_at AS created_at
    FROM transactions tr
    JOIN non_wallet tw USING (wallet_id_id)
)

SELECT * FROM non_wallet_debit