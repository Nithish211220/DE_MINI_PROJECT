WITH cleaned_data AS (
    SELECT 
        {{ cast_columns({
            'customer_id': 'INT',
            'revenue_type': 'INT',
            'revenue': 'DECIMAL(10,2)',
            'quantity': 'INT'
        }) }}
        
        product_id,
        TO_DATE(payment_month, 'DD-MM-YYYY') AS payment_date,
       
        dimension_1 AS dim_1,
        dimension_2 AS dim_2,
        dimension_3 AS dim_3,
        dimension_4 AS dim_4,
        dimension_5 AS dim_5,
        dimension_6 AS dim_6,
        dimension_7 AS dim_7,
        dimension_8 AS dim_8,
        dimension_9 AS dim_9,
        dimension_10 AS dim_10,

        companies AS company_name
    FROM {{ source('sf_source', 'TRANSACTION') }}
    WHERE {{ filter_not_null([
        'customer_id', 'product_id', 'payment_month', 'revenue', 'quantity',
        'dimension_1', 'dimension_2', 'dimension_3', 'dimension_4', 'dimension_5',
        'dimension_6', 'dimension_7', 'dimension_8', 'dimension_9', 'dimension_10',
        'companies'
    ]) }}
)

SELECT * FROM cleaned_data
