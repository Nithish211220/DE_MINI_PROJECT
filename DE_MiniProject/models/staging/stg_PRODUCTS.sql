WITH cte_cleaned AS (
    SELECT 
        TRIM(PRODUCT_ID) AS product_id, 
        TRIM(PRODUCT_FAMILY) AS product_family, 
        TRIM(PRODUCT_SUB_FAMILY) AS product_sub_family
    FROM {{ source('sf_source', 'PRODUCTS') }}
    WHERE PRODUCT_ID IS NOT NULL 
    --   AND PRODUCT_FAMILY IS NOT NULL 
    --   AND PRODUCT_SUB_FAMILY IS NOT NULL
),
cte_final AS (
    SELECT DISTINCT * FROM cte_cleaned
)
SELECT * FROM cte_final
