-- use bigquery
with ddnet_race_with_map_infos as (
    select
        a.Map as Map,
        a.Name as Name,
        a.Time as Time,
        a.Timestamp as Timestamp,
        a.Server as Server,
        b.Map as Map_1,
        b.Server as Server_1,
        b.Points as Points,
        b.Stars as Stars,
        b.Mapper as Mapper,
        b.Timestamp as Timestamp_1
    from
        ddnet_stats.race a
        left join ddnet_stats.maps b on a.Map = b.Map
    where
        a.Server = 'CHN'
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
    finish_ddmae_point,
    finish_dummy_point,
    finish_brutal_point,
    finish_insane_point,
    finish_novice_point,
    finish_moderate_point,
    finish_oldschool_point,
    finish_fun_point,
    finish_race_new_map_count,
    finish_solo_new_map_count,
    finish_ddmae_new_map_count,
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
    finish_ddmae_map_count,
    finish_dummy_map_count,
    finish_brutal_map_count,
    finish_insane_map_count,
    finish_novice_map_count,
    finish_moderate_map_count,
    finish_oldschool_map_count
from
    (
        select
            Name,
            count(Map) as finish_map_count,
            count(distinct Map) as finish_distinct_map_count,
            sum(Time / 3600) as finish_time_sum_hours,
            sum(Points) as finish_points_total,
            sum(
                case
                    when Server_1 = 'Fun' then Points
                    else 0
                end
            ) as finish_fun_map_count,
            sum(
                case
                    when Server_1 = 'Race' then Points
                    else 0
                end
            ) as finish_race_map_count,
            sum(
                case
                    when Server_1 = 'Solo' then Points
                    else 0
                end
            ) as finish_solo_map_count,
            sum(
                case
                    when Server_1 = 'DDmaX' then Points
                    else 0
                end
            ) as finish_ddmae_map_count,
            sum(
                case
                    when Server_1 = 'Dummy' then Points
                    else 0
                end
            ) as finish_dummy_map_count,
            sum(
                case
                    when Server_1 = 'Brutal' then Points
                    else 0
                end
            ) as finish_brutal_map_count,
            sum(
                case
                    when Server_1 = 'Insane' then Points
                    else 0
                end
            ) as finish_insane_map_count,
            sum(
                case
                    when Server_1 = 'Novice' then Points
                    else 0
                end
            ) as finish_novice_map_count,
            sum(
                case
                    when Server_1 = 'Moderate' then Points
                    else 0
                end
            ) as finish_moderate_map_count,
            sum(
                case
                    when Server_1 = 'Oldschool' then Points
                    else 0
                end
            ) as finish_oldschool_map_count
        from
            ddnet_race_with_map_infos
        where
            Timestamp < '2022-01-01'
            and Timestamp >= '2021-01-01'
        group by
            Name
    ) a
    join (
        select
            Name,
            sum(Points) as total_points_earned,
            sum(
                case
                    when Server_1 = 'Fun' then Points
                    else 0
                end
            ) as finish_fun_point,
            sum(
                case
                    when Server_1 = 'Race' then Points
                    else 0
                end
            ) as finish_race_point,
            sum(
                case
                    when Server_1 = 'Solo' then Points
                    else 0
                end
            ) as finish_solo_point,
            sum(
                case
                    when Server_1 = 'DDmaX' then Points
                    else 0
                end
            ) as finish_ddmae_point,
            sum(
                case
                    when Server_1 = 'Dummy' then Points
                    else 0
                end
            ) as finish_dummy_point,
            sum(
                case
                    when Server_1 = 'Brutal' then Points
                    else 0
                end
            ) as finish_brutal_point,
            sum(
                case
                    when Server_1 = 'Insane' then Points
                    else 0
                end
            ) as finish_insane_point,
            sum(
                case
                    when Server_1 = 'Novice' then Points
                    else 0
                end
            ) as finish_novice_point,
            sum(
                case
                    when Server_1 = 'Moderate' then Points
                    else 0
                end
            ) as finish_moderate_point,
            sum(
                case
                    when Server_1 = 'Oldschool' then Points
                    else 0
                end
            ) as finish_oldschool_point,
            sum(
                case
                    when Server_1 = 'Fun' then 1
                    else 0
                end
            ) as finish_fun_new_map_count,
            sum(
                case
                    when Server_1 = 'Race' then 1
                    else 0
                end
            ) as finish_race_new_map_count,
            sum(
                case
                    when Server_1 = 'Solo' then 1
                    else 0
                end
            ) as finish_solo_new_map_count,
            sum(
                case
                    when Server_1 = 'DDmaX' then 1
                    else 0
                end
            ) as finish_ddmae_new_map_count,
            sum(
                case
                    when Server_1 = 'Dummy' then 1
                    else 0
                end
            ) as finish_dummy_new_map_count,
            sum(
                case
                    when Server_1 = 'Brutal' then 1
                    else 0
                end
            ) as finish_brutal_new_map_count,
            sum(
                case
                    when Server_1 = 'Insane' then 1
                    else 0
                end
            ) as finish_insane_new_map_count,
            sum(
                case
                    when Server_1 = 'Novice' then 1
                    else 0
                end
            ) as finish_novice_new_map_count,
            sum(
                case
                    when Server_1 = 'Moderate' then 1
                    else 0
                end
            ) as finish_moderate_new_map_count,
            sum(
                case
                    when Server_1 = 'Oldschool' then 1
                    else 0
                end
            ) as finish_oldschool_new_map_count
        from
            (
                select
                    Name,
                    Points,
                    Server_1,
                    Timestamp,
                    ROW_NUMBER() over (
                        partition by Map,
                        Name
                        order by
                            Timestamp desc
                    ) as rk
                from
                    ddnet_race_with_map_infos
            )
        where
            rk = 1
            and Timestamp < '2022-01-01'
            and Timestamp >= '2021-01-01'
        group by
            Name
    ) b on a.Name = b.Name
order by
    total_points_earned desc