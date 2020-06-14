with cte as
(select district,
       count(*) cnt
from (select global_id,
        name,
        district
    from moscow_household_services
    where (name like '%парик%'
    or name like '%космет%')
    and district not like '%поселение%')t
    group by district),
greatest as (select district,
       cnt,
       'Наибольшее кол-во' query_type
from cte
order by cnt desc
limit 5),
smallest as (
    select district,
           cnt,
           'Наименьшее кол-во' as query_type
    from cte
    order by cnt asc
    limit 5
)
select district,
       cnt,
       query_type
from greatest
union
select district,
       cnt,
       query_type
from smallest
order by cnt  desc
