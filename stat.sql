-- use bigquery
with ddnet_race_with_map_infos as (
    select
        a.Map as Map,
        a.Name as Name,
        a.Time as Time,
        a.Timestamp as Timestamp,
        a.Server as Server,
        b.Server as Server_Type,
        b.Points as Points,
        b.Stars as Stars,
        b.Mapper as Mapper,
        b.Timestamp as Timestamp_MAP,
        ROW_NUMBER() over (
            partition by a.Map,
            Name
            order by
                a.Timestamp desc
        ) as map_name_rank_time_desc,
        ROW_NUMBER() over (
            partition by a.Map,
            Name
            order by
                a.Timestamp
        ) as map_name_rank_time_asc
    from
        ddnet_stats.race a
        left join ddnet_stats.maps b on a.Map = b.Map
    where
        a.Server = 'CHN'
),
ddnet_race_with_map_infos_2021 as (
    select
        Map,
        Name,
        Time,
        Timestamp,
        Server,
        Server_Type,
        Points,
        Stars,
        Mapper,
        Timestamp_MAP,
        ROW_NUMBER() over (
            partition by Map,
            Name
            order by
                Timestamp desc
        ) as map_name_rank_time_desc_2021,
        ROW_NUMBER() over (
            partition by Map,
            Name
            order by
                Timestamp
        ) as map_name_rank_time_asc_2021,
        ROW_NUMBER() over (
            partition by Map,
            Name
            order by
                Time desc
        ) as map_name_rank_time_spend_desc,
        ROW_NUMBER() over (
            partition by Map,
            Name
            order by
                Time
        ) as map_name_rank_time_spend_asc_2021
    from
        ddnet_race_with_map_infos
    where
        Timestamp < '2022-01-01'
        and Timestamp >= '2021-01-01'
),
most_finished_map_2021_table as (
    select
        Name,
        most_finished_map_2021,
        map_finished_count,
        most_finished_map_2021_spend_minues
    from
        (
            select
                Name,
                Map as most_finished_map_2021,
                map_finished_count,
                map_spend_minues as most_finished_map_2021_spend_minues,
                ROW_NUMBER() over (
                    partition by Name
                    order by
                        map_finished_count desc,
                        map_spend_minues desc
                ) as map_name_rank_finished_count_desc
            from
                (
                    select
                        Map,
                        Name,
                        sum(Time / 60) as map_spend_minues,
                        count(*) as map_finished_count
                    from
                        ddnet_race_with_map_infos_2021
                    group by
                        Map,
                        Name
                )
        )
    where
        map_name_rank_finished_count_desc = 1
), stat_with_repeat as (
    select
            Name,
            count(Map) as finish_map_count,
            count(distinct Map) as finish_distinct_map_count,
            sum(Time / 3600) as finish_time_sum_hours,
            sum(Points) as finish_points_total,
            sum(
                case
                    when Server_Type = 'Fun' then Points
                    else 0
                end
            ) as finish_fun_map_count,
            sum(
                case
                    when Server_Type = 'Race' then Points
                    else 0
                end
            ) as finish_race_map_count,
            sum(
                case
                    when Server_Type = 'Solo' then Points
                    else 0
                end
            ) as finish_solo_map_count,
            sum(
                case
                    when Server_Type = 'DDmaX' then Points
                    else 0
                end
            ) as finish_ddmax_map_count,
            sum(
                case
                    when Server_Type = 'Dummy' then Points
                    else 0
                end
            ) as finish_dummy_map_count,
            sum(
                case
                    when Server_Type = 'Brutal' then Points
                    else 0
                end
            ) as finish_brutal_map_count,
            sum(
                case
                    when Server_Type = 'Insane' then Points
                    else 0
                end
            ) as finish_insane_map_count,
            sum(
                case
                    when Server_Type = 'Novice' then Points
                    else 0
                end
            ) as finish_novice_map_count,
            sum(
                case
                    when Server_Type = 'Moderate' then Points
                    else 0
                end
            ) as finish_moderate_map_count,
            sum(
                case
                    when Server_Type = 'Oldschool' then Points
                    else 0
                end
            ) as finish_oldschool_map_count
        from
            ddnet_race_with_map_infos_2021
        group by
            Name
), stat_2021_without_repeat as (
    select
            Name,
            sum(Points) as total_points_earned,
            sum(
                case
                    when Server_Type = 'Fun' then Points
                    else 0
                end
            ) as finish_fun_point,
            sum(
                case
                    when Server_Type = 'Race' then Points
                    else 0
                end
            ) as finish_race_point,
            sum(
                case
                    when Server_Type = 'Solo' then Points
                    else 0
                end
            ) as finish_solo_point,
            sum(
                case
                    when Server_Type = 'DDmaX' then Points
                    else 0
                end
            ) as finish_ddmax_point,
            sum(
                case
                    when Server_Type = 'Dummy' then Points
                    else 0
                end
            ) as finish_dummy_point,
            sum(
                case
                    when Server_Type = 'Brutal' then Points
                    else 0
                end
            ) as finish_brutal_point,
            sum(
                case
                    when Server_Type = 'Insane' then Points
                    else 0
                end
            ) as finish_insane_point,
            sum(
                case
                    when Server_Type = 'Novice' then Points
                    else 0
                end
            ) as finish_novice_point,
            sum(
                case
                    when Server_Type = 'Moderate' then Points
                    else 0
                end
            ) as finish_moderate_point,
            sum(
                case
                    when Server_Type = 'Oldschool' then Points
                    else 0
                end
            ) as finish_oldschool_point,
            sum(
                case
                    when Server_Type = 'Fun' then 1
                    else 0
                end
            ) as finish_fun_new_map_count,
            sum(
                case
                    when Server_Type = 'Race' then 1
                    else 0
                end
            ) as finish_race_new_map_count,
            sum(
                case
                    when Server_Type = 'Solo' then 1
                    else 0
                end
            ) as finish_solo_new_map_count,
            sum(
                case
                    when Server_Type = 'DDmaX' then 1
                    else 0
                end
            ) as finish_ddmax_new_map_count,
            sum(
                case
                    when Server_Type = 'Dummy' then 1
                    else 0
                end
            ) as finish_dummy_new_map_count,
            sum(
                case
                    when Server_Type = 'Brutal' then 1
                    else 0
                end
            ) as finish_brutal_new_map_count,
            sum(
                case
                    when Server_Type = 'Insane' then 1
                    else 0
                end
            ) as finish_insane_new_map_count,
            sum(
                case
                    when Server_Type = 'Novice' then 1
                    else 0
                end
            ) as finish_novice_new_map_count,
            sum(
                case
                    when Server_Type = 'Moderate' then 1
                    else 0
                end
            ) as finish_moderate_new_map_count,
            sum(
                case
                    when Server_Type = 'Oldschool' then 1
                    else 0
                end
            ) as finish_oldschool_new_map_count
        from
            ddnet_race_with_map_infos_2021
        where
            map_name_rank_time_desc_2021 = 1
        group by
            Name
)
-- maybe to-do
-- 最晚完成时间，什么时候算第二天，4 点？
-- 最多组队的人，同伴名？
select
    a.Name,
    total_points_earned,
    finish_time_sum_hours,
    finish_distinct_map_count,
    finish_map_count,
    finish_points_total,
    finish_race_point,
    finish_solo_point,
    finish_ddmax_point,
    finish_dummy_point,
    finish_brutal_point,
    finish_insane_point,
    finish_novice_point,
    finish_moderate_point,
    finish_oldschool_point,
    finish_fun_point,
    finish_race_new_map_count,
    finish_solo_new_map_count,
    finish_ddmax_new_map_count,
    finish_dummy_new_map_count,
    finish_brutal_new_map_count,
    finish_insane_new_map_count,
    finish_novice_new_map_count,
    finish_moderate_new_map_count,
    finish_oldschool_new_map_count,
    finish_fun_new_map_count,
    finish_fun_map_count,
    finish_race_map_count,
    finish_solo_map_count,
    finish_ddmax_map_count,
    finish_dummy_map_count,
    finish_brutal_map_count,
    finish_insane_map_count,
    finish_novice_map_count,
    finish_moderate_map_count,
    finish_oldschool_map_count,
    c.most_finished_map_2021,
    c.map_finished_count,
    c.most_finished_map_2021_spend_minues
from
    stat_with_repeat a
    join stat_2021_without_repeat b
    on a.Name = b.Name
    join most_finished_map_2021_table c
    on a.Name = c.Name
order by
    total_points_earned desc
limit 100;