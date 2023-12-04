
-- mysql ddl
CREATE TABLE tpcds_recent_runs (
    ts              varchar(255),
    profile         varchar(255),
    qid             varchar(255),
    fmt             varchar(255),
    last_run_ts     varchar(255),
    run0            BIGINT,
    run1 BIGINT,
    run2 BIGINT,
    run3 BIGINT,
    run4 BIGINT,
    run5 BIGINT,
    run6 BIGINT,
    run7 BIGINT,
    run8 BIGINT,
    run9 BIGINT,
    run10 BIGINT,
    run11 BIGINT,
    run12 BIGINT,
    extra           varchar(255),
    primary key (ts, profile, fmt, qid)
);





drop table if exists zhnwang_tpcds_meta.tpcds_recent_runs;
CREATE TABLE zhnwang_tpcds_meta.tpcds_recent_runs (
    ts              LONG,
    profile         STRING,
    qid             STRING,
    fmt             STRING,
    runs            ARRAY<LONG>,
    runt            ARRAY<STRING>,
    qset            STRING,
    extra           STRING
) USING iceberg
options (
    primaryKey = 'ts, profile, qid'
)
partitioned by (ts);

-- pk: db, ts