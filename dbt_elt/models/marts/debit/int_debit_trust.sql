WITH

transactions AS (
    SELECT
        *
    FROM {{ref('stg_transactions')}}
),

trust_wallet AS (
    SELECT
        *
    FROM {{ ref('stg_trust_account_wallet')}}
),

trust AS (
    SELECT
        id AS trust_account_id_id,
        bank_name AS name
    FROM {{ ref('stg_trust_accounts')}}
),

temp_debit AS (
    SELECT
        tr.id AS id,
        tw.trust_account_id_id AS trust_account_id_id,
        tr.created_at AS created_at
    FROM transactions tr
    JOIN trust_wallet tw USING (wallet_id_id)
),

trust_debit AS (
    SELECT
        temp.id AS id,
        user.name AS debited_from,
        3 AS debited_role,
        temp.created_at AS debited_at
    FROM trust user
    JOIN temp_debit temp USING (trust_account_id_id)
)

SELECT * FROM trust_debit