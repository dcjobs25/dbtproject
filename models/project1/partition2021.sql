select *
from {{ref('result')}}
where date_part(year, order_date) = 2021