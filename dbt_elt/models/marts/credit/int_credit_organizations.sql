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
        ow.organization_id_id AS organization_id_id,
        tr.created_at AS created_at
    FROM wallet_to_wallet_obj tr
    JOIN organization_wallet ow USING (wallet_id_id)
),

organization_credit AS (
    SELECT
        temp.id AS id,
        user.name AS credited_to,
        2 AS credited_role,
        temp.created_at AS credited_at
    FROM organizations user
    JOIN temp_credit temp USING (organization_id_id)
)

SELECT * FROM organization_credit