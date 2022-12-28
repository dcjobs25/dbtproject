with calc_employees as (
    select *,
        date_part(year, current_date) - date_part(year, birth_date) as age,
        date_part(year, current_date) - date_part(year, hire_date) as length_service,
        first_name||' '||last_name as name
    from {{source('source_name', 'employees')}}
)
select * 
from calc_employees