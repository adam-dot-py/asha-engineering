

select
 IDSK,
 ID, 
 ASHA_COMMENT
from "main_silver"."std_all_tsm_survey_responses"
where ASHA_COMMENT is not null