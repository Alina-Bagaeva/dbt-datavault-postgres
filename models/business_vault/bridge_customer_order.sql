{{
    config(
        enabled=True,
        materialized='table',
        schema='business_vault'
    )
}}

SELECT
    l.LINK_CUSTOMER_ORDER_PK AS bridge_key,
    hc.CUSTOMER_PK,
    ho.ORDER_PK,
    sc.first_name,
    sc.last_name,
    so.order_date,
    so.status,
    l.LOAD_DATE,
    l.RECORD_SOURCE,
    ROW_NUMBER() OVER (PARTITION BY hc.CUSTOMER_PK, ho.ORDER_PK ORDER BY so.order_date DESC) AS rn
FROM
    {{ ref('link_customer_order') }} l
JOIN
    {{ ref('hub_customer') }} hc ON l.CUSTOMER_PK = hc.CUSTOMER_PK
JOIN
    {{ ref('hub_order') }} ho ON l.ORDER_PK = ho.ORDER_PK
LEFT JOIN
    {{ ref('sat_customer') }} sc ON hc.CUSTOMER_PK = sc.CUSTOMER_PK
LEFT JOIN
    {{ ref('sat_order') }} so ON ho.ORDER_PK = so.ORDER_PK