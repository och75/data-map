
update temp.explore_queries_4 t4
  set t4.select_from_tables=t2.ls_tables
from (
 select
    t.key,
    array_agg(tb.TABLE_NAME) as ls_tables
  from
    temp.explore_queries_4 t
    cross join
      unnest(t.ls_words) word
      inner join
        warehouse.INFORMATION_SCHEMA.TABLES tb
          on word=tb.TABLE_NAME
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
        when type_operation='update' then regexp_extract(query, 'update ([a-z._0-9]*) ')
        when type_operation='insert' then regexp_extract(query, 'insert into ([a-z._0-9]*) ')
        when type_operation='drop' then regexp_extract(replace(query, 'if exists ',''), 'drop table ([a-z._0-9]*)')
        when type_operation='delete' then regexp_extract(replace(query, 'from ',''), 'delete ([a-z._0-9]*) ')
        when type_operation='create' then regexp_extract(replace(query, 'or replace ',''), 'create table ([a-z._0-9]*) ')
        else null
    end
    as cdiud_tables,
    cast(null as ARRAY<String>) as select_from_tables
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
  REGEXP_EXTRACT_ALL(Query, '[A-Za-z0-9_]+' ) as ls_words
from
  temp.explore_queries_2
where
  UserId like 'airflow%';


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
) as Query
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