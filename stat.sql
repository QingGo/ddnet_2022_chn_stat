-- use bigquery
-- to-do?
-- 最多组队的人，同伴名？
-- 原始过图记录+地图全表
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
        EXTRACT(
            DAYOFWEEK
            FROM
                a.Timestamp AT TIME ZONE "Asia/Almaty"
        ) as weekday,
        EXTRACT(
            DATE
            FROM
                a.Timestamp AT TIME ZONE "Asia/Almaty"
        ) as date,
        EXTRACT(
            HOUR
            FROM
                a.Timestamp AT TIME ZONE "Asia/Almaty"
        ) as hour,
        EXTRACT(
            MINUTE
            FROM
                a.Timestamp AT TIME ZONE "Asia/Almaty"
        ) as minute,
        EXTRACT(
            SECOND
            FROM
                a.Timestamp AT TIME ZONE "Asia/Almaty"
        ) as second,
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
-- 原始过图记录+地图 2021 表
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
        weekday,
        date,
        hour,
        minute,
        second,
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
        Timestamp < '2022-01-01 06:00:00'
        and Timestamp >= '2021-01-01 06:00:00'
),
-- 过图次数最多的地图
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
),
-- 不对记录去重统计的基本信息表
stat_2021_with_repeat as (
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
),
-- 对记录去重统计的基本信息表
stat_2021_without_repeat as (
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
),
-- 熬夜战神表
day_latest_finish_time_2021 as (
    select
        Name,
        day_latest_finish_time,
        day_latest_finish_map
    from
        (
            select
                Name,
                FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", Timestamp, "Asia/Almaty") as day_latest_finish_time,
                Map as day_latest_finish_map,
                ROW_NUMBER() over (
                    partition by Name
                    order by
                        MOD((hour + 18), 24) desc,
                        minute desc,
                        second desc
                ) as day_latest_finish_rank
            from
                ddnet_race_with_map_infos_2021
        )
    where
        day_latest_finish_rank = 1
),
-- 最常过图的时间段
day_most_finish_hour_2021 as (
    select
        Name,
        most_finish_hour,
        most_finish_hour_count
    from
        (
            select
                Name,
                hour as most_finish_hour,
                count(*) as most_finish_hour_count,
                ROW_NUMBER() over (
                    partition by Name
                    order by
                        count(*) desc
                ) as most_finish_hour_rank
            from
                ddnet_race_with_map_infos_2021
            group by
                Name,
                hour
        )
    where
        most_finish_hour_rank = 1
),
-- 最常过图是星期几
day_most_finish_weekday_2021 as (
    select
        Name,
        most_finish_weekday,
        most_finish_weekday_count
    from
        (
            select
                Name,
                weekday as most_finish_weekday,
                count(*) as most_finish_weekday_count,
                ROW_NUMBER() over (
                    partition by Name
                    order by
                        count(*) desc
                ) as most_finish_weekday_rank
            from
                ddnet_race_with_map_infos_2021
            group by
                Name,
                weekday
        )
    where
        most_finish_weekday_rank = 1
),
-- 在多少天有过图记录
days_count_has_records_2021 as (
    select
        Name,
        count(*) as days_count_has_records_2021
    from
        (
            select
                Name,
                date,
                count(*)
            from
                ddnet_race_with_map_infos_2021
            group by
                Name,
                date
        )
    group by
        Name
),
-- 2021 总玩家数
player_count_2021 as (
    select
        count(*) as player_count_2021
    from
        stat_2021_without_repeat
),
-- 2021 新增分数排名
points_earned_rank_2021 as (
    select
        Name,
        rank() over (
            order by
                total_points_earned desc
        ) as total_points_earned_rank
    from
        stat_2021_without_repeat
),
-- 2021 过图时间排名
finish_time_sum_hours_rank_2021 as (
    select
        Name,
        rank() over (
            order by
                finish_time_sum_hours desc
        ) as finish_time_sum_hours_rank
    from
        stat_2021_with_repeat
)
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
    c.most_finished_map_2021_spend_minues,
    d.day_latest_finish_time,
    d.day_latest_finish_map,
    e.most_finish_hour,
    e.most_finish_hour_count,
    f.most_finish_weekday,
    f.most_finish_weekday_count,
    g.days_count_has_records_2021,
    h.player_count_2021,
    i.total_points_earned_rank,
    j.finish_time_sum_hours_rank
from
    stat_2021_with_repeat a
    join stat_2021_without_repeat b on a.Name = b.Name
    join most_finished_map_2021_table c on a.Name = c.Name
    join day_latest_finish_time_2021 d on a.Name = d.Name
    join day_most_finish_hour_2021 e on a.Name = e.Name
    join day_most_finish_weekday_2021 f on a.Name = f.Name
    join days_count_has_records_2021 g on a.Name = g.Name
    cross join player_count_2021 h
    join points_earned_rank_2021 i on a.Name = i.Name
    join finish_time_sum_hours_rank_2021 j on a.Name = j.Name
order by
    total_points_earned desc;