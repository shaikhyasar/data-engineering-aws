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

coc_wallet AS (
    SELECT
        *
    FROM {{ ref('stg_coc_account_wallet')}}
),
accounts_coc AS (
    SELECT
        id AS coc_account_id_id,
        branch_name
    FROM {{ ref('stg_coc_accounts')}}
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
        uw.coc_account_id_id AS coc_account_id_id,
        tr.created_at AS created_at
    FROM wallet_to_wallet_obj tr
    JOIN coc_wallet uw USING (wallet_id_id)
),


coc_credit AS (
    SELECT
        temp.id AS id,
        user.branch_name AS credited_to,
        4 AS credited_role,
        temp.created_at AS credited_at
    FROM accounts_coc user
    JOIN temp_credit temp USING (coc_account_id_id)
)

SELECT * FROM coc_credit