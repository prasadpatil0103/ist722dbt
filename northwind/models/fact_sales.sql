-- models/fact_sales.sql

{{ config(
    materialized='table'
) }}

with stg_orders as (
    select
        OrderID,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey,
        replace(to_date(shippeddate)::varchar, '-', '')::int as shippeddatekey
    from {{ source('northwind', 'Orders') }}
),

stg_order_details as (
    select
        orderid,
        productid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        quantity,
        unitprice,
        discount,
        quantity * unitprice as extendedprice,
        quantity * unitprice * discount as discountamount,
        quantity * unitprice * (1 - discount) as soldamount
    from {{ source('northwind', 'Order_Details') }}
)

select
    od.orderid || '-' || od.productid as SalesId,
    od.productkey,
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    o.shippeddatekey,
    od.quantity,
    od.extendedprice,
    od.discountamount,
    od.soldamount
from stg_order_details od
join stg_orders o on od.orderid = o.orderid