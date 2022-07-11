WITH

transactions AS (
    SELECT
        *
    FROM {{ref('stg_transactions')}}
),

organization_wallet AS (
    SELECT
        *
    FROM {{ ref('stg_organization_wallet')}}
),

organizations AS (
    SELECT
        id AS organization_id_id,
        name
    FROM {{ ref('stg_organizations')}}
),

temp_debit AS (
    SELECT
        tr.id AS id,
        ow.organization_id_id AS organization_id_id,
        tr.created_at AS created_at
    FROM transactions tr
    JOIN organization_wallet ow USING (wallet_id_id)
),

organization_debit AS (
    SELECT
        temp.id AS id,
        user.name AS debited_from,
        2 AS debited_role,
        temp.created_at AS debited_at
    FROM organizations user
    JOIN temp_debit temp USING (organization_id_id)
)

SELECT * FROM organization_debit
