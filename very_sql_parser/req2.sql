with t1 as (
    SELECT
    row_number() over() as key,
    lower(protopayload_auditlog.servicedata_v1_bigquery.jobQueryRequest.query) as req
    FROM `bbc-data-platform.BigQuery_stackdriver.cloudaudit_googleapis_com_data_access_20190504`
    where protopayload_auditlog.servicedata_v1_bigquery.jobQueryRequest.query is not null
  --and lower(protopayload_auditlog.servicedata_v1_bigquery.jobQueryRequest.query) like '%select `booking_mode` as `booking_mode`,%'
),
t2 as (
  --on splitte la reauete en mots
  select key,  req, REGEXP_EXTRACT_ALL(req, '[A-Za-z0-9_]+' ) as areq from t1
),
t3 as (
  --on deplie la liste/array pour la rendre facilement manipulable
  select t2.key, t2.req, uc
  from t2
         left outer join unnest(t2.areq) uc
),
-- on detecte ici si un des mots de la requete correspond a une table
t4 as (
  select
    t3.*,
    st.matched_table_name
  from
    t3
    left outer join (
      select distinct lower(table_name) as matched_table_name
      from `work.tmp_och_schema_tables_cols`
    ) st
      on t3.uc=st.matched_table_name
 ),
--on liste ici les champs des tables deja detectes
t5 as (
  select distinct t4.key, t4.matched_table_name from t4
)
    select
      t4.* except (matched_table_name),
      sc.table_name as matched_table_name,
      sc.column_name as matched_column_name
    from
      t4
      left outer join `work.tmp_och_schema_tables_cols` sc on t4.uc=sc.column_name
    where
     sc.table_name in( select distinct t5.matched_table_name from t5 where t5.key=t4.key)
order by
  key