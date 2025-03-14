    WITH cte_handle_null AS (
        SELECT 
            customer_id,
            TRIM(customername)  AS customername,
            TRIM(company) AS company
        FROM {{ source('sf_source', 'CUSTOMERS') }}
        WHERE customer_id IS NOT NULL
        -- OR company IS NOT NULL 
        -- OR customername IS NOT NULL 
    ),
    cte_final AS (
        SELECT 
            CAST(customer_id AS INT) AS customer_id,
            customername AS customer_name,
            company AS company_name
        FROM cte_handle_null
    )
    SELECT * FROM cte_final


