with orderdetail_calc as (
    select 
        od.order_id,
        od.product_id,
        od.unit_price as unit_price_order,
        pr.unit_price as unit_price_prod,
        od.quantity,
        pr.product_name,
        pr.supplier_id,
        pr.category_id,
        od.unit_price * od.quantity as total_order,
        pr.unit_price * od.quantity as total_orig,
        total_orig - total_order as vlr_discount
    from {{source('source_name', 'order_details')}} od
    left join {{source('source_name', 'products')}} pr on od.product_id = pr.product_id
)
select * from orderdetail_calc