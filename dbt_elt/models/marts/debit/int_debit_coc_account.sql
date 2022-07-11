WITH

transactions AS (
    SELECT
        *
    FROM {{ref('stg_transactions')}}
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

temp_debit AS (
    SELECT
        tr.id AS id,
        uw.coc_account_id_id AS coc_account_id_id,
        tr.created_at AS created_at
    FROM transactions tr
    JOIN coc_wallet uw USING (wallet_id_id)
),


coc_debit AS (
    SELECT
        temp.id AS id,
        user.branch_name AS debited_from,
        4 AS debited_role,
        temp.created_at AS debited_at
    FROM accounts_coc user
    JOIN temp_debit temp USING (coc_account_id_id)
)

SELECT * FROM coc_debit