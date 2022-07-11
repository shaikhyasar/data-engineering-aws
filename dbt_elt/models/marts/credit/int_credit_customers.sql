WITH

transactions AS (
    SELECT
        *
    FROM {{ref('stg_transactions')}}
),

wallet_to_wallet AS (
    SELECT 
        *
    FROM {{ ref('stg_transactions_wallet_to_wallet')}}
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

wallet_to_wallet_obj AS (
    SELECT
        tr.id,
        tr.created_at,
        ww.wallet_id_id
    FROM transactions tr
    JOIN wallet_to_wallet ww
    ON tr.id = ww.transaction_id_id

),

temp_credit AS (
    SELECT
        tr.id AS id,
        uw.user_id_id AS user_id_id,
        tr.created_at AS created_at
    FROM wallet_to_wallet_obj tr
    JOIN user_wallet uw USING (wallet_id_id)
),


customer_credit AS (
    SELECT
        temp.id AS id,
        user.username AS credited_to,
        CAST(user.user_role AS INTEGER) AS credited_role,
        temp.created_at AS credited_at
    FROM accounts_customuser user
    JOIN temp_credit temp USING (user_id_id)
)

SELECT * FROM customer_credit