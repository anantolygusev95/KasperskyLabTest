with clock_district as
         (select distinct district
          from moscow_household_services msh
          where 1 = 1
            and msh.name like '%часов%'
            and msh.district not like '%поселение%'),
     all_district as (
         select distinct district
         from moscow_household_services mhs
         where 1 = 1
           and mhs.district not like '%поселение%'
     )
select count(*) cnt_district_not_watch_repair
from all_district al
where 1 = 1
  and al.district not in (select district from clock_district)
