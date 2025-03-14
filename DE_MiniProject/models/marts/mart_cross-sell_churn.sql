-- WITH first_payment AS (
--     SELECT
--         customer_id,
--         MIN(payment_date) AS first_payment_date
--     FROM
--         {{ ref('stg_TRANSACTION') }}
--     GROUP BY
--         customer_id
-- ),

-- trans_data AS
-- (
--     select 
--         customer_id,
--         PRODUCT_ID,
--         PAYMENT_DATE
--     FROM
--     {{ref("stg_TRANSACTION")}}
-- )

-- SELECT 
--     trans_data.CUSTOMER_ID,
--     COUNT(DISTINCT PRODUCT_ID) AS Cross_Sell,
--     DENSE_RANK() OVER (ORDER BY Cross_Sell DESC) AS Product_Rank 
-- FROM trans_data 
-- LEFT JOIN first_payment  AS fp 
--   ON trans_data.customer_id=fp.customer_id
-- WHERE fp.first_payment_date <> PAYMENT_DATE
-- GROUP BY trans_data.customer_id 
-- ORDER BY Product_Rank 


 
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
 