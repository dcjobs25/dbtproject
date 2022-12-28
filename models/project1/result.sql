with prod as (
    select
        pr.product_id,
        pr.product_name,
        pr.unit_price,
        sp.company_name as suppliers,
        ct.category_name
    from {{source('source_name', 'products')}} pr
    left join {{source('source_name', 'suppliers')}} sp on pr.supplier_id = sp.supplier_id
    left join {{source('source_name', 'categories')}} ct on pr.category_id = ct.category_id
),
ord_det as (
    select 
        od.order_id,
        pd.*,
        od.quantity,
        od.vlr_discount
    from {{ref('order_details')}} od
    left join prod pd on od.product_id = pd.product_id
),
ord as (
    select 
        ord.order_id,
        ord.order_date,
        cs.company_name customer,
        em.name employee,
        em.age,
        em.length_service
    from {{source('source_name', 'orders')}} ord
    left join {{ref('customers')}} cs on ord.customer_id = cs.customer_id
    left join {{ref('employees')}} em on ord.employee_id = em.employee_id
    left join {{source('source_name', 'shippers')}} sh on ord.ship_via = sh.shipper_id
),
tab_final as (
    select 
        od.*,
        ord.order_date,
        ord.customer,
        ord.employee,
        ord.age,
        ord.length_service
    from ord_det od
    join ord on od.order_id = ord.order_id
)
select *
from tab_final