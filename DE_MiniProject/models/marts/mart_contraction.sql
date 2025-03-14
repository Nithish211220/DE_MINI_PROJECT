WITH cte_revenue_drop as(
    SELECT
        PRODUCT_ID,
        PAYMENT_DATE,
        REVENUE,
        LAG(REVENUE) OVER (PARTITION BY PRODUCT_ID ORDER BY PAYMENT_DATE) AS PREV_REVENUE
    FROM {{ ref('stg_TRANSACTION') }}
    WHERE revenue > 0 AND revenue_type = 1
),
cte_replacing_null as(
    SELECT COALESCE(PREV_REVENUE,0) as Previous_Revenue,
    PRODUCT_ID,
    PAYMENT_DATE,
    PREV_REVENUE,
    REVENUE,
     FROM cte_revenue_drop
)
    SELECT
    PRODUCT_ID,
    PAYMENT_DATE,
    SUM(Previous_Revenue) as Previous_Revenue,
    SUM(REVENUE) as Current_Revenue,
    SUM(Previous_Revenue) - SUM(REVENUE) as Revenue_Loss,
    DENSE_RANK() OVER (ORDER BY Revenue_Loss DESC) AS loss_rank
FROM cte_replacing_null
GROUP BY PRODUCT_ID,PAYMENT_DATE  
ORDER BY loss_rank asc 