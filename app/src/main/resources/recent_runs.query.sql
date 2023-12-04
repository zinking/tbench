

show tables in zhnwang_tpcds_meta;

use zhnwang_tpcds_meta;

desc extended query_run;

select distinct profile_name from query_run;


select distinct profile_name from query_run where profile_name like '202_____-%';


select * from query_run where profile_name = '20230213-regression-sf1000-iceberg' limit 10;


with input as (
select
    split(profile_name, '-')[2] as pf,
    split(profile_name, '-')[0] as dt,
    split(profile_name, '-')[3] as fmt,
    qset,
    qid,
    run
from
    query_run
where
    profile_name like '202_____-%'
order by
    split(profile_name, '-')[0] desc
)
select
    '20220215' as ts,
    pf,
    qid,
    fmt,
    collect_list(run) as runs,
    collect_list(dt) as runt,
    qset,
    '' as extra
from
    input
group by
    pf, qid, fmt, qset
;


select
    ts, profile, qid, fmt, runt[0] as last_run_ts,
    runs[0], runs[1], runs[2], runs[3], runs[4],
    runs[5], runs[6], runs[7], runs[8], runs[9],
    runs[10], runs[11], runs[12], extra
from
    zhnwang_tpcds_meta.tpcds_recent_runs
;


select ${curdate}

drop

insert


