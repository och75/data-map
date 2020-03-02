/*
TODO check pour trouver les tables en insert/update et en select
*/
-------
with t1 as (
  select
    coalesce(t4.cdiud_table, ls) as tables,
    type_operation,
    1 as nb_occ
  from
    temp.explore_queries_4 t4
    left outer join unnest(t4.ls_select_from_tables ) ls
)
select distinct
  concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME) as table,
  sum( sum(coalesce(t1.nb_occ,0)) ) over( partition by concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME)) as nb_used,
  countif(type_operation ='select') ) over( partition by concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME)) as nb_select,
  sum( countif(type_operation ='insert') ) over( partition by concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME)) as nb_insert,
  sum( countif(type_operation ='update') ) over( partition by concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME)) as nb_update,
  sum( countif(type_operation ='create') ) over( partition by concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME)) as nb_create,
  sum( countif(type_operation ='drop') ) over( partition by concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME)) as nb_drop
from
  warehouse.INFORMATION_SCHEMA.TABLES tb
    left outer join t1
      on concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME) = t1.tables
      and t1.tables like 'bbc-data-platform.warehouse%'
group by
  tb.table_catalog,tb.table_schema, tb.TABLE_NAME, type_operation
order by 2 desc;

/*
TODO faire les stats d'utilisation ensuite par champ, par tables
TODO une vision table.cchamp

TODO ajouter une clause pour faire la recherche des champs insérés / mis à jour
TODO ajouter res pour enrichir le champs select pour les requetes ins/upd : faire la même recherche qu'en select pour trouver les from_tables en enlevant de la liste celle identifiées en insert select

*/

update temp.explore_queries_4 t4
  set t4.ls_select_fields=ls_fields
from (
 select
    t.key,
    array_agg(distinct concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME, '.', tb.COLUMN_NAME) ) as ls_fields
  from
    temp.explore_queries_4 t
    cross join
      unnest(t.ls_words) word
      inner join
        warehouse.INFORMATION_SCHEMA.COLUMNS tb
          on word=tb.COLUMN_NAME
          and concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME) in unnest(t.ls_select_from_tables)
  where
    t.type_operation='select'
  group by
    1
) t2
where
  t2.key=t4.key;

/*
 TODO créer une table regroupant toutes metadatas de tous les schémas pour neplus avoir à le fixer
 TODO dans le code

*/
update temp.explore_queries_4 t4
  set t4.ls_select_from_tables=t2.ls_tables
from (
 select
    t.key,
    array_agg(distinct concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME) ) as ls_tables
  from
    temp.explore_queries_4 t
    cross join
      unnest(t.ls_words_with_points) word
      inner join
        warehouse.INFORMATION_SCHEMA.TABLES tb
          on (
                word=concat(tb.table_schema, '.', tb.TABLE_NAME)
                or word=concat(tb.table_catalog, '.', tb.table_schema, '.', tb.TABLE_NAME)
              )
  where
    t.type_operation='select'
  group by
    1
) t2
where
  t2.key=t4.key;


create or replace table temp.explore_queries_4 as
select
    t.*,
    case
        when type_operation='update' then regexp_extract(query, 'update ([a-z._0-9-]*) ')
        when type_operation='insert' then regexp_extract(replace(query, 'into ',''), 'insert ([a-z._0-9-]*) ')
        when type_operation='drop' then regexp_extract(replace(query, 'if exists ',''), 'drop table ([a-z._0-9-]*)')
        when type_operation='delete' then regexp_extract(replace(query, 'from ',''), 'delete ([a-z._0-9-]*) ')
        when type_operation='create' then regexp_extract(replace(query, 'or replace ',''), 'create table ([a-z._0-9-]*) ')
        else null
    end
    as cdiud_table,
    cast(null as ARRAY<String>) as ls_select_from_tables,
    cast(null as ARRAY<String>) as ls_select_fields,
    cast(null as ARRAY<String>) as ls_ins_upd_fields,
from
  `temp.explore_queries_3`  t;



  create or replace table temp.explore_queries_3 as
select
  key,
  Date,
  ProjectId,
  UserId,
  Query,
    REGEXP_EXTRACT(Query, "\\*[\\s\\S]*\\Wtype[\\s]*=[\\s]*'([^']*)'[\\s\\S]*\\*/") as TagType,
    REGEXP_EXTRACT(Query, "\\*[\\s\\S]*\\Wcategory[\\s]*=[\\s]*'([^']*)'[\\s\\S]*\\*/") as TagCategory,
    REGEXP_EXTRACT(Query, "\\*[\\s\\S]*\\Wsub_category[\\s]*=[\\s]*'([^']*)'[\\s\\S]*\\*/") as TagSubCategory,

  case
    when Query like '%drop %' then 'drop'
    when Query like '%create %' then 'create'
    when Query like '%delete %' then 'delete'
    when Query like '%update %' then 'update'
    when Query like '%insert %' then 'insert'
    when Query like '%select %' then 'select'
  end as type_operation,
  REGEXP_EXTRACT_ALL(Query, '[A-Za-z0-9_]+' ) as ls_words,
  REGEXP_EXTRACT_ALL(Query, '[A-Za-z0-9_.-]+' ) as ls_words_with_points,
  original_query

from
  temp.explore_queries_2
;


create or replace table temp.explore_queries_2 as
select
    t.* except (Query),
lower(
    replace(
      replace(
        replace(
          REPLACE(Query,'’', ''),
          '\n', ' '
       ),
       '`',''
     ),
    '(',' ('
   )
) as Query,
Query as original_query
from
    temp.explore_queries t;



create or replace table temp.explore_queries as
SELECT
    row_number() over() as key,
    timestamp AS Date,
    resource.labels.project_id AS ProjectId,
    protopayload_auditlog.serviceName AS ServiceName,
    protopayload_auditlog.methodName AS MethodName,
    protopayload_auditlog.status.code AS StatusCode,
    protopayload_auditlog.status.message AS StatusMessage,
    protopayload_auditlog.authenticationInfo.principalEmail AS UserId,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId AS JobId,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS Query,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.projectId AS DestinationTableProjectId,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.datasetId AS DestinationTableDatasetId,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId AS DestinationTableId,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.createDisposition AS CreateDisposition,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.writeDisposition AS WriteDisposition,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.dryRun AS DryRun,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.state AS JobState,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.code AS JobErrorCode,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.message AS JobErrorMessage,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.createTime AS JobCreateTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime AS JobStartTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime AS JobEndTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.billingTier AS BillingTier,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes AS TotalBilledBytes,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes AS TotalProcessedBytes,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / pow(2,30) AS TotalBilledGigabytes,
    (protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / pow(2,30)) / pow(2,10) AS TotalBilledTerabytes,
    ((protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / pow(2,30)) / pow(2,10)) * 5 AS TotalCost,
FROM
  `BigQuery_stackdriver.cloudaudit_googleapis_com_data_access_20200227*`
WHERE
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query is not null
;