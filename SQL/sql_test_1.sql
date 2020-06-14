
select street,
       count(*) cnt
from(
select global_id,
       name,
       split_part(address,',', 2) street
from moscow_household_services
where name like '%фото%')t
group by street
order by cnt desc
limit 1