-- models/obt_sales.sql

{{ config(
    materialized='table'
) }}

with f_sales_aggregated as (
    select
        SalesId,
        customerkey,
        employeekey,
        orderdatekey,
        shippeddatekey,
        sum(quantity) as total_quantity,
        sum(extendedprice) as total_extendedprice,
        sum(discountamount) as total_discountamount,
        sum(soldamount) as total_soldamount,
        count(distinct productkey) as product_count
    from {{ ref('fact_sales') }}
    group by SalesId, customerkey, employeekey, orderdatekey, shippeddatekey
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_employee as (
    select * from {{ ref('dim_employee') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
)
select
    d_customer.*,
    d_employee.*,
    d_date.*,
    f.SalesId,
    f.orderdatekey,
    f.shippeddatekey,
    f.total_quantity,
    f.total_extendedprice,
    f.total_discountamount,
    f.total_soldamount,
    f.product_count
from f_sales_aggregated as f
left join d_customer on f.customerkey = d_customer.customerkey
left join d_employee on f.employeekey = d_employee.employeekey
left join d_date on f.orderdatekey = d_date.datekey