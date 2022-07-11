WITH

transactions AS (
    SELECT
        *
    FROM {{ref('stg_transactions')}}
),

enterprise_wallet AS (
    SELECT
        *
    FROM {{ ref('stg_enterprise_account_wallet')}}
),

enterprise AS (
    SELECT
        id AS enterprise_id_id,
        company_name AS name
    FROM {{ ref('stg_enterprises')}}
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
temp_credit AS (
    SELECT
        tr.id AS id,
        tw.enterprise_id_id AS enterprise_id_id,
        tr.created_at AS created_at
    FROM wallet_to_wallet_obj tr
    JOIN enterprise_wallet tw USING (wallet_id_id)
),

enterprise_credit AS (
    SELECT
        temp.id AS id,
        user.name AS credited_to,
        6 AS credited_role,
        temp.created_at AS credited_at
    FROM enterprise user
    JOIN temp_credit temp USING (enterprise_id_id)
)

SELECT * FROM enterprise_credit