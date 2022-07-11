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

temp_debit AS (
    SELECT
        tr.id AS id,
        tw.enterprise_id_id AS enterprise_id_id,
        tr.created_at AS created_at
    FROM transactions tr
    JOIN enterprise_wallet tw USING (wallet_id_id)
),

enterprise_debit AS (
    SELECT
        temp.id AS id,
        user.name AS debited_from,
        6 AS debited_role,
        temp.created_at AS debited_at
    FROM enterprise user
    JOIN temp_debit temp USING (enterprise_id_id)
)

SELECT * FROM enterprise_debit