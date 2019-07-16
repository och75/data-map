/*
SELECT protopayload_auditlog.methodName, count(*) FROM `bbc-data-platform.BigQuery_stackdriver.cloudaudit_googleapis_com_activity_20190710`
group by 1

Row	methodName	f0_
1	tableservice.insert 125
2	tableservice.update 2
3	tableservice.delete 269

SELECT protopayload_auditlog.methodName, count(*) FROM `bbc-data-platform.BigQuery_stackdriver.cloudaudit_googleapis_com_data_access_20190710`
group by 1

Row methodName  f0_
1 jobservice.jobcompleted 8785
2 tabledataservice.list 2313
3 jobservice.insert 12549
4 jobservice.cancel 8
5 jobservice.getqueryresults 146156
6 jobservice.query 829


*/

with t1 as (
  SELECT
    row_number() over() as key,
    resource.labels.project_id,
    protopayload_auditlog.methodName,
    protopayload_auditlog.resourceName,
    protopayload_auditlog.status.message,
    protopayload_auditlog.authenticationInfo.principalEmail,
    protopayload_auditlog.servicedata_v1_bigquery.tableInsertRequest,
    protopayload_auditlog.servicedata_v1_bigquery.tableUpdateRequest,
    protopayload_auditlog.servicedata_v1_bigquery.datasetListRequest,
    protopayload_auditlog.servicedata_v1_bigquery.datasetInsertRequest,
    protopayload_auditlog.servicedata_v1_bigquery.datasetUpdateRequest,
    lower(protopayload_auditlog.servicedata_v1_bigquery.jobQueryRequest.query) as req,
    --farm finger print pour idnetifier les req recurrentes
    -- regexp_substr pour choper category et sub-category
    -- attraper les jointures pour determiner comment les tables sont jointes entre elles
  FROM
    `bbc-data-platform.BigQuery_stackdriver.cloudaudit_googleapis_com_data_access_20190710`
  where
    protopayload_auditlog.servicedata_v1_bigquery.jobQueryRequest.query is not null
),
t2 as (
  --on splitte la reauete en mots
  select key,  t1.*, REGEXP_EXTRACT_ALL(req, '[A-Za-z0-9_]+' ) as areq from t1
),
t3 as (
  --on deplie la liste/array pour la rendre facilement manipulable
  select t2.key, t2.*, uc
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