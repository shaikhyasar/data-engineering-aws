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
        tw.trust_account_id_id AS trust_account_id_id,
        tr.created_at AS created_at
    FROM wallet_to_wallet_obj tr
    JOIN trust_wallet tw USING (wallet_id_id)
),

trust_credit AS (
    SELECT
        temp.id AS id,
        user.name AS credited_to,
        3 AS credited_role,
        temp.created_at AS credited_at
    FROM trust user
    JOIN temp_credit temp USING (trust_account_id_id)
)

SELECT * FROM trust_credit
