WITH first_payment AS (
    SELECT
        customer_id,
        MIN(payment_date) AS first_payment_date
    FROM
        {{ ref('stg_TRANSACTION') }}
    GROUP BY
        customer_id
),

new_customers_by_fy AS (
    SELECT
        customer_id,
        EXTRACT(YEAR FROM first_payment_date) AS fiscal_year
    FROM
        first_payment
),

date_range AS (
    SELECT
        MIN(payment_date) AS start_date,
        MAX(payment_date) AS end_date
    FROM
        {{ ref('stg_TRANSACTION') }}
),

last_payment AS (
    SELECT
        customer_id,
        MAX(payment_date) AS last_payment_date
    FROM
        {{ ref('stg_TRANSACTION') }}
    GROUP BY
        customer_id
),

churned_customers AS (
    SELECT
        customer_id,
        EXTRACT(YEAR FROM last_payment_date) AS fiscal_year
    FROM
        last_payment,
        date_range
    WHERE
        last_payment_date < end_date
),

fiscal_years AS (
    SELECT DISTINCT
        EXTRACT(YEAR FROM payment_date) AS fiscal_year
    FROM
        {{ ref('stg_TRANSACTION') }}
)

SELECT
    fy.fiscal_year,
    COUNT(DISTINCT new_customers_by_fy.customer_id) AS new_customers_count,
    COUNT(DISTINCT churned_customers.customer_id) AS churned_customers_count
FROM
    fiscal_years fy
    LEFT JOIN new_customers_by_fy ON fy.fiscal_year = new_customers_by_fy.fiscal_year
    LEFT JOIN churned_customers ON fy.fiscal_year = churned_customers.fiscal_year
GROUP BY
    fy.fiscal_year
ORDER BY
    fy.fiscal_year
