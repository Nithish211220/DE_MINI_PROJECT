
WITH trunc_date AS (
    SELECT
        PRODUCT_ID,
        DATE_TRUNC('month',PAYMENT_DATE) AS transaction_month
    FROM {{ ref('stg_TRANSACTION') }}
),

max_month AS (
    SELECT MAX(transaction_month) AS latest_month FROM trunc_date
),

churn_status AS (
    SELECT
        t.PRODUCT_ID,
        m.latest_month,
        CASE
            WHEN MAX(t.transaction_month) <= DATEADD('month', -3, m.latest_month) THEN 'Inactive'
            ELSE 'Active'
        END AS L3m_status,
        CASE
            WHEN MAX(t.transaction_month) <= DATEADD('month', -1, m.latest_month) THEN 'Inactive'
            ELSE 'Active'
        END AS LM_status,
        CASE
            WHEN MAX(t.transaction_month) <= DATEADD('month', -12, m.latest_month) THEN 'Inactive'
            ELSE 'Active'
        END AS Ltm_status
    FROM trunc_date t
    CROSS JOIN max_month m
    GROUP BY t.PRODUCT_ID,m.latest_month
)

SELECT * FROM churn_status
 