with t1 as (
    SELECT
    row_number() over() as key,
    protopayload_auditlog.servicedata_v1_bigquery.jobQueryRequest.query as req
    FROM `bbc-data-platform.BigQuery_stackdriver.cloudaudit_googleapis_com_data_access_20190504`
    where protopayload_auditlog.servicedata_v1_bigquery.jobQueryRequest.query is not null
),
t2 as (
  select key,  REGEXP_EXTRACT_ALL(req, '[A-Za-z0-9_]+' ) as areq from t1
),
t3 as (
  select t2.key, uc from t2 left outer join unnest(t2.areq) uc
),
t4 as (
  select
    t3.*,
    st.*
  from
    t3
    left outer join (select distinct table_name as matched_table_name from `work.tmp_och_schema_tables_cols`) st on t3.uc=st.matched_table_name
 )
 select
  t4.key, t4.uc,
  coalesce(t4.matched_table_name, sc.matched_table_name) as matched_table_name,
  sc.matched_column_name
 from
  t4
  left outer join (
    select distinct
      table_name as matched_table_name ,
      sc.column_name as matched_column_name
    from `work.tmp_och_schema_tables_cols` sc
    where sc.table_name in (select distinct matched_table_name from t4)
  ) sc
  on t4.uc=sc.matched_column_name
