WITH first_payment AS (
    SELECT
        customer_id,
        MIN(payment_date) AS first_payment_date
    FROM
        {{ ref('stg_TRANSACTION') }}
    GROUP BY
        customer_id
),

trans_data AS
(
    select 
        customer_id,
        PRODUCT_ID,
        PAYMENT_DATE
    FROM
    {{ref("stg_TRANSACTION")}}
)

SELECT 
    trans_data.CUSTOMER_ID,
    COUNT(DISTINCT PRODUCT_ID) AS Cross_Sell,
    DENSE_RANK() OVER (ORDER BY Cross_Sell DESC) AS Product_Rank 
FROM trans_data 
LEFT JOIN first_payment  AS fp 
  ON trans_data.customer_id=fp.customer_id
WHERE fp.first_payment_date <> PAYMENT_DATE
GROUP BY trans_data.customer_id 
ORDER BY Product_Rank 
