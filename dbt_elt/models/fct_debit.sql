WITH

transactions AS (
    SELECT
        *
    FROM {{ref('stg_transactions')}}
    {% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})

{% endif %}
),

credit_coc AS (
    SELECT
        *
    FROM {{ref('int_credit_coc_account')}}
),

credit_customers AS (
    SELECT
        *
    FROM {{ref('int_credit_customers')}}

),
credit_enterprise AS (
    SELECT
        *
    FROM {{ref('int_credit_enterprise')}}

),

credit_non_wallet AS (
    SELECT
        *
    FROM {{ref('int_credit_non_wallet')}}

),

credit_operational AS (
    SELECT
        *
    FROM {{ref('int_credit_operational')}}

),

credit_organizations AS (
    SELECT
        *
    FROM {{ref('int_credit_organizations')}}

),

credit_trust AS (
    SELECT
        *
    FROM {{ref('int_credit_trust')}}

),

debit_coc AS (
    SELECT
        *
    FROM {{ref('int_debit_coc_account')}}
),

debit_customers AS (
    SELECT
        *
    FROM {{ref('int_debit_customers')}}

),

debit_enterprise AS (
    SELECT
        *
    FROM {{ref('int_credit_enterprise')}}

),

debit_non_wallet AS (
    SELECT
        *
    FROM {{ref('int_debit_non_wallet')}}

),

debit_operational AS (
    SELECT
        *
    FROM {{ref('int_debit_operational')}}

),

debit_organizations AS (
    SELECT
        *
    FROM {{ref('int_debit_organizations')}}

),

debit_trust AS (
    SELECT
        *
    FROM {{ref('int_debit_trust')}}

),

credit_account AS (
    SELECT * FROM credit_coc UNION ALL
    SELECT * FROM credit_customers UNION ALL
    SELECT * FROM credit_enterprise UNION ALL
    SELECT * FROM credit_non_wallet UNION ALL
    SELECT * FROM credit_operational UNION ALL
    SELECT * FROM credit_organizations UNION ALL
    SELECT * FROM credit_trust
),

debit_account AS (
    SELECT * FROM debit_coc UNION ALL
    SELECT * FROM debit_customers UNION ALL
    SELECT * FROM debit_enterprise UNION ALL
    SELECT * FROM debit_non_wallet UNION ALL
    SELECT * FROM debit_operational UNION ALL
    SELECT * FROM debit_organizations UNION ALL
    SELECT * FROM debit_trust
),

final AS (
    SELECT
        tr.transaction_id,
        tr.created_at,
        cr.credited_to,
        CASE WHEN cr.credited_role = 3 THEN 'Trust Account'
		        WHEN cr.credited_role = 4 THEN 'COC Account'
		        WHEN cr.credited_role = 5 THEN 'Agent'
		        WHEN cr.credited_role = 6 THEN 'Enterprise'
		        WHEN cr.credited_role = 7 THEN 'Customer'
		        WHEN cr.credited_role = 8 THEN 'Merchant'
		        WHEN cr.credited_role = 10 THEN 'Vendor'
		        ELSE 'Non wallet'
	    END credited_role,
        CASE WHEN tr.txn_type = 1 THEN 'TRUST'
    	        WHEN tr.txn_type = 2 THEN 'WALLET_TRANSFER'
    	        WHEN tr.txn_type = 3 THEN 'CASH_IN'
    	        WHEN tr.txn_type = 4 THEN 'CASH_OUT'
    	        WHEN tr.txn_type = 5 AND cr.credited_role = 9 THEN 'NON_WALLET_CASH_IN'
    	        WHEN tr.txn_type = 5 THEN 'NON_WALLET_CASH_OUT'
    	        WHEN tr.txn_type = 7 THEN 'NIGELEC'
    	        WHEN tr.txn_type = 8 THEN 'HO_to_COC'
    	        WHEN tr.txn_type = 9 THEN 'RECHARGE'
                WHEN txn_type = 10 THEN 'ALIZZA VOYAGES'
	    END transaction_type,
        tr.amount,
        dr.debited_from,
        CASE WHEN dr.debited_role = 3 THEN 'Trust Account'
		        WHEN dr.debited_role = 4 THEN 'COC Account'
		        WHEN dr.debited_role = 5 THEN 'Agent'
		        WHEN dr.debited_role = 6 THEN 'Enterprise'
		        WHEN dr.debited_role = 7 THEN 'Customer'
		        WHEN dr.debited_role = 8 THEN 'Merchant'
		        WHEN dr.debited_role = 10 THEN 'Vendor'
		        ELSE 'Non wallet'
	    END debited_role,
	    CASE WHEN tr.status = 1 THEN 'Success'
                WHEN tr.status = 2 THEN 'Pending'
                WHEN tr.status = 3 THEN 'Requested'
                WHEN tr.status = 4 THEN 'Cancel'
                WHEN tr.status = 5 THEN 'Refunded'
                WHEN tr.status = 6 THEN 'Disburse'
                WHEN tr.status = 7 THEN 'Time Out'
		        ELSE 'Revert'
        END status,
        tr.note AS charges
    FROM transactions tr
    JOIN credit_account cr USING (id)
    JOIN debit_account dr USING (id)
)

SELECT * FROM final









