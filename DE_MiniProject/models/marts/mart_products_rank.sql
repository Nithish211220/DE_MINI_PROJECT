WITH product_revenue AS (
    SELECT
        product_id,
        SUM(revenue) AS total_revenue
    FROM {{ ref('stg_TRANSACTION') }}
    where revenue_type = 1 and revenue > 0
    GROUP BY
        product_id
),
ranked_products AS (
    
    SELECT
    product_id,
    total_revenue,
    DENSE_RANK() OVER(ORDER BY total_revenue DESC) AS product_rank
FROM product_revenue
)

SELECT * FROM ranked_products
