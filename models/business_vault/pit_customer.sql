{{ 
    config(
        materialized='table',
        enabled=True
    )
}}

{%- set src_pk = 'CUSTOMER_PK' -%}
{%- set satellites = ['sat_customer', 'sat_customer_crm'] -%}
{%- set as_of_dates_table = 'as_of_date' -%}

{{ automate_dv.pit(
    src_pk=src_pk,
    satellites=satellites,
    as_of_dates_table=as_of_dates_table
) }}