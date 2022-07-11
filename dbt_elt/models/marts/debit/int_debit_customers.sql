WITH

transactions AS (
    SELECT
        *
    FROM {{ref('stg_transactions')}}
),

user_wallet AS (
    SELECT
        *
    FROM {{ ref('stg_user_wallet')}}
),

accounts_customuser AS (
    SELECT
        id AS user_id_id,
        username,
        user_role
    FROM {{ ref('stg_user_accounts')}}
),

temp_debit AS (
    SELECT
        tr.id AS id,
        uw.user_id_id AS user_id_id,
        tr.created_at AS created_at
    FROM transactions tr
    JOIN user_wallet uw USING (wallet_id_id)
),


customer_debit AS (
    SELECT
        temp.id AS id,
        user.username AS debited_from,
        CAST(user.user_role AS INTEGER) AS debited_role,
        temp.created_at AS debited_at
    FROM accounts_customuser user
    JOIN temp_debit temp USING (user_id_id)
)

SELECT * FROM customer_debit