
  
    
    

    create  table
      "asha_dev"."main_silver"."std_tsm_responses__dbt_tmp"
  
    as (
      SELECT
    id AS ID,
    start_time AS StartTime,
    email AS EmailAddress,
    name AS Name,
    taking_everything_into_account_how_satisfied_or_dissatisfied_are_you_with_the_service_provided_by_ash_shahada
        AS TP01,

    has_ash_shahada_carried_out_a_repair_to_your_home_in_the_last_12_months
        AS TP02_TP03_confirm,

    how_satisfied_or_dissatisfied_are_you_with_the_overall_repairs_service_from_ash_shahada_over_the_last_12_months
        AS TP02,

    how_satisfied_or_dissatisfied_are_you_with_the_time_taken_to_complete_your_most_recent_repair_after_you_reported_it
        AS TP03,

    how_satisfied_or_dissatisfied_are_you_that_ash_shahada_provides_a_home_that_is_well_maintained
        AS TP04,

    thinking_about_the_condition_of_the_property_or_building_you_live_in_how_satisfied_or_dissatisfied_are_you_that_ash_shahada_provides_a_home_that_is_safe
        AS TP05,

    how_satisfied_or_dissatisfied_are_you_that_ash_shahada_listens_to_your_views_and_acts_upon_them
        AS TP06,

    how_satisfied_or_dissatisfied_are_you_that_ash_shahada_keeps_you_informed_about_things_that_matter_to_you
        AS TP07,

    to_what_extent_do_you_agree_or_disagree_with_the_following_ash_shahada_treats_me_fairly_and_with_respect
        AS TP08,

    have_you_made_a_complaint_to_ash_shahada_in_the_last_12_months
        AS TP09_confirm,

    how_satisfied_or_dissatisfied_are_you_with_ash_shahadas_approach_to_complaints_handling
        AS TP09,

    do_you_live_in_a_building_with_communal_areas_either_inside_or_outside_that_ash_shahada_is_responsible_for_maintaining
        AS TP10_confirm,

    how_satisfied_or_dissatisfied_are_you_that_ash_shahada_keeps_these_communal_areas_clean_and_well_maintained
        AS TP10,

    how_satisfied_or_dissatisfied_are_you_that_ash_shahada_makes_a_positive_contribution_to_your_neighbourhood
        AS TP11,

    how_satisfied_or_dissatisfied_are_you_with_ash_shahadas_approach_to_handling_antisocial_behaviour
        AS TP12,

    how_likely_are_you_to_recommend_us_to_a_friend_or_colleague
        AS ASHA_NPS,

    to_help_us_improve_please_leave_a_comment_to_explain_any_of_your_scorings
        AS ASHA_COMMENT,
    
    'TSM Survey' AS SurveySource,
    CONCAT(id, '-tsm') AS IDSK

FROM "asha_dev"."main_staging"."stg_tsm_responses"
    );
  
  