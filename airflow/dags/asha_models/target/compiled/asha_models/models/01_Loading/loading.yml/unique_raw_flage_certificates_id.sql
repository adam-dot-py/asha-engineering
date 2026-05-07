
    
    

select
    id as unique_field,
    count(*) as n_records

from "asha_dev"."main_bronze"."raw_flage_certificates"
where id is not null
group by id
having count(*) > 1


