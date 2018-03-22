# Databricks notebook source
# MAGIC %md
# MAGIC #### Import necessary files

# COMMAND ----------

import psycopg2 as pg
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import uuid
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run local def

# COMMAND ----------

# MAGIC %run /Users/stan@mediamath.com/utils/essentials

# COMMAND ----------

# MAGIC %run /Users/admin/Credentials

# COMMAND ----------

db = credentials['dag_psycopg2_connection_string']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setting up connection

# COMMAND ----------

a=db.split(' ')
dbname = a[0].split('=')[1][1:-1]
user = a[1].split('=')[1][1:-1]
host = a[2].split('=')[1][1:-1]
pw=a[3].split('=')[1][1:-1]


# COMMAND ----------

conn_string =  db
# print(conn_string)
conn = pg.connect(conn_string)
cursor = conn.cursor()
# print "Connected!\n"
db_engine = create_engine('postgresql://'+user+':'+pw+'@'+host+':5432/'+dbname)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query to get data on a daily basis

# COMMAND ----------

today=datetime.now()

unique_id =uuid.uuid4().int
# pixel_id="637821,993220,637822,637823,529192,637820" #first touch pixels
pixel_id="422895,422897,421712,422899,422896,986695" #last touch pixels
campaign_ids='423856,266234,161586,161824,166561,206737,215676,217961,251718,276066,270827,320669,320066,414370,414358,414382,414385,437139'
engagement_campaigns='164009, 146298, 143629, 146828, 151361, 151641, 136333, 148672, 160589, 160768, 163300, 166541, 156952, 172142, 172133, 172143, 172851, 172854, 167117, 172898, 172896, 157697, 173106, 173111, 174930, 179853, 179887, 179846, 157904, 188845, 188902, 189494, 189495, 189496, 189497, 189498, 189499, 189492, 189493, 189668, 190374, 164857, 207885,215680,217964,250906,270703,303040,313806,313809,313805'
campaign_id = ','.join([campaign_ids,engagement_campaigns])
today=datetime.now()
days_ago=60
start_date = (today - timedelta(days=days_ago)).strftime('%Y-%m-%d')
end_date=(today - timedelta(days=1)).strftime('%Y-%m-%d')
unique_id =uuid.uuid4().int
params={
  "{{organization_id}}":100174,
  "{{start_date}}":start_date,
  "{{end_date}}":end_date,
  "{{uuid}}":unique_id,
  "{{pixel_id}}":pixel_id,
  "{{campaign_id}}":campaign_id
  
}
print(params)

# COMMAND ----------

#### impressions table  ####
sqlContext.sql(format_query('drop table if exists tmp_gap_ft_opto_impressions{{uuid}}',params))
sqlContext.sql(format_query('''
create table tmp_gap_ft_opto_impressions{{uuid}} stored as ORC as 
	SELECT *
	FROM mm_impressions
	WHERE organization_id = 100174
    and impression_date between '{{start_date}}' and '{{end_date}}'
	AND campaign_id in ({{campaign_id}})
''',params))

#### Clicks Table ####
sqlContext.sql(format_query('drop table if exists tmp_gap_ft_opto_clicks{{uuid}}',params))
sqlContext.sql(format_query('''
create table tmp_gap_ft_opto_clicks{{uuid}} stored as ORC as 
	select 
      mm_uuid,
      imp_auction_id
    
	FROM mm_attributed_events
	WHERE organization_id = 100174
    and event_date between date_sub('{{start_date}}',1) and date_add('{{end_date}}',1)
	and event_report_timestamp between '{{start_date}}' and '{{end_date}}'
	AND campaign_id in ({{campaign_id}})
    and event_type='click'
''',params))

#### Join Table ####
sqlContext.sql(format_query('drop table if exists tmp_gap_ft_opto_join{{uuid}}',params))
sqlContext.sql(format_query('''
create table tmp_gap_ft_opto_join{{uuid}} stored as ORC as 
	select 
      case when click.imp_auction_id is null then imp.mm_uuid else click.mm_uuid end as  mod_mm_uuid,
      imp.*,
      case when click.imp_auction_id is null then 'V' else 'C' end as pv_pc_flag
	FROM tmp_gap_ft_opto_impressions{{uuid}} imp
    join tmp_gap_ft_opto_clicks{{uuid}} click
    on imp.auction_id = click.imp_auction_id
''',params))

#### Events Table ###
sqlContext.sql(format_query('drop table if exists tmp_gap_ft_opto_events{{uuid}}',params))
events = sqlContext.sql(format_query('''
create table tmp_gap_ft_opto_events{{uuid}} stored as ORC  as 
	SELECT mm_uuid, timestamp_gmt, user_agent, referrer, v1, v2, s1, s2, advertiser_id
	FROM mm_events
	WHERE organization_id=100174
	AND event_date between '{{start_date}}' and '{{end_date}}'
	AND pixel_id in ({{pixel_id}})
''',params))





# COMMAND ----------

sqlContext.sql(format_query('drop table if exists tmp_gap_ft_conversions{{uuid}}',params))

join = sqlContext.sql(format_query('''
create table tmp_gap_ft_conversions{{uuid}} stored as ORC as 
select * from 
(select 
	from_unixtime(unix_timestamp(imp.timestamp_gmt) + 1) as timestamp_gmt,
    report_timestamp as report_timestamp,
	imp.campaign_id,
	imp.campaign_name,
	imp.strategy_id,
	imp.strategy_name,
	ev.mm_uuid,
	ev.user_agent,
	ev.referrer,
	imp.advertiser_id,
	imp.advertiser_name,
	ev.v1 as revenue,
	ev.v2,
	ev.s1,
	ev.s2,
    pv_pc_flag,
	row_number() over (partition by imp.mm_uuid, ev.timestamp_gmt, ev.advertiser_id order by imp.timestamp_gmt) as rank
FROM	
	tmp_gap_ft_opto_events{{uuid}} ev 
	join tmp_gap_ft_opto_join{{uuid}} imp 
		on imp.mod_mm_uuid = ev.mm_uuid
		and imp.advertiser_id = ev.advertiser_id
WHERE
	unix_timestamp(ev.timestamp_gmt) - unix_timestamp(imp.timestamp_gmt) <= 691200 
) a 
where rank=1
''',params))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a df for the above query
# MAGIC 1. Use the "toPandas()" function to convert the spark dataframe into a pandas dataframe
# MAGIC 2. Clear the temp table 

# COMMAND ----------

final_df =sqlContext.sql(format_query('''
select
    coalesce(c.mm_date,a.mm_date,b.mm_date) as mm_date,
    a.advertiser_id,
    a.advertiser_name,
    a.campaign_id,
    a.campaign_name,
    a.strategy_id,
    a.strategy_name,
    coalesce(total_spend,0) as total_spend,
    coalesce( media_cost,0) as media_cost,
    coalesce(impressions,0) as impressions,
    coalesce(clicks,0) as clicks,
    coalesce(online_conversion,0) as online_conversion,
    coalesce(online_revenue,0) as online_revenue,
    coalesce(offline_conversion,0) as offline_conversion,
    coalesce(offline_revenue,0) as offline_revenue,
    coalesce(pv_online_conversion,0) as pv_online_conversion,
    coalesce(pv_online_revenue,0) as pv_online_revenue,
    coalesce(pc_online_conversion,0) as pc_online_conversion,
    coalesce(pc_online_revenue,0) as pc_online_revenue
from
  (select 
    to_date(timestamp_gmt)  as mm_date,
    a.advertiser_id,
    a.advertiser_name,
    a.campaign_id,
    a.campaign_name,
    a.strategy_id,
    a.strategy_name,
    sum(total_spend_cpm/1000) as total_spend,
    sum(media_cost_cpm / 1000) as media_cost,
    count(*) as impressions
  from
    mm_impressions a
  where
    organization_id = 100174
  and
    impression_date between date_sub('{{start_date}}',1) and date_add('{{end_date}}',1)
  and
    to_date(report_timestamp) between '{{start_date}}' and '{{end_date}}'
  group by
    to_date(timestamp_gmt),
    a.advertiser_id,
    a.advertiser_name,
    a.campaign_id,
    a.campaign_name,
    a.strategy_id,
    a.strategy_name) a
   join 
  (select
    to_date(event_report_timestamp) as mm_date,
    a.advertiser_id,
    a.advertiser_name,
    a.campaign_id,
    a.campaign_name,
    a.strategy_id,
    a.strategy_name,
    count(*) as clicks
  from
    mm_attributed_events a
  where 
    organization_id = 100174
  and 
    event_date between date_sub('{{start_date}}',1) and date_add('{{end_date}}',1)
  and 
    event_type='click'

  group by
    to_date(event_report_timestamp)  ,
    a.advertiser_id,
    a.advertiser_name,
    a.campaign_id,
    a.campaign_name,
    a.strategy_id,
    a.strategy_name
  ) b
  on a.strategy_id = b.strategy_id and a.mm_date=b.mm_date
   join
  (
      select
        to_date(cast(report_timestamp as timestamp))  as mm_date
        ,a.advertiser_id
        ,a.advertiser_name
        ,a.campaign_id
        ,a.campaign_name
       , a.strategy_id
        ,a.strategy_name
        ,count(*) as conversions,
        sum(case when lower(s2) = 'retail' then 1 else 0 end) as offline_conversion,
    sum(case when lower(s2) = 'retail' then cast(revenue as double) else 0 end) as offline_revenue,
    sum(case when lower(s2) <> 'retail' then 1 else 0 end) as online_conversion,
    sum(case when lower(s2) <> 'retail' then cast(revenue as double) else 0 end) as online_revenue,
    sum(case when lower(s2) <> 'retail' and pv_pc_flag='C' then 1 else 0 end) as pc_online_conversion,
    sum(case when lower(s2) <> 'retail' and pv_pc_flag='C' then cast(revenue as double) else 0 end) as pc_online_revenue,
    sum(case when lower(s2) <> 'retail' and pv_pc_flag='V' then 1 else 0 end) as pv_online_conversion,
    sum(case when lower(s2) <> 'retail' and pv_pc_flag='V' then cast(revenue as double) else 0 end) as pv_online_revenue
  from 
    tmp_gap_ft_conversions{{uuid}} a
  group by
    to_date(cast(report_timestamp as timestamp)) ,
    a.advertiser_id,
    a.advertiser_name,
    a.campaign_id,
    a.campaign_name,
    a.strategy_id,
    a.strategy_name
  ) c
    on c.strategy_id = a.strategy_id and a.mm_date=c.mm_date
''',params))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Add an additional column "batch" to know when it was last updated

# COMMAND ----------

import datetime

now = datetime.datetime.now()
final_pandas_df=final_df.toPandas()
final_pandas_df["batch"] = now

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add the above data into gap_first_touch_agg data in DAG
# MAGIC 
# MAGIC Drop the remaining temp tables

# COMMAND ----------


final_pandas_df.to_sql('gap_first_touch_agg',if_exists="append",con=db_engine,index=False)
sqlContext.sql(format_query('drop table if exists tmp_gap_ft_join{{uuid}}',params))
sqlContext.sql(format_query('drop table if exists tmp_gap_ft_opto_impressions{{uuid}}',params))
sqlContext.sql(format_query('drop table if exists tmp_gap_ft_opto_events{{uuid}}',params))
sqlContext.sql(format_query('drop table if exists tmp_gap_ft_opto_join{{uuid}}',params))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removing duplicates
# MAGIC 
# MAGIC 1. Assign a row number based on the mm_date, campaign_id, strategy_id and sort  them in a descending order by the auto increment column "ID"
# MAGIC 2. Delete all rows that are more than 1 (only keep the latest)
# MAGIC 3. Close connections so that they are not idling and hogging it

# COMMAND ----------


dups = '''
delete from gap_first_touch_agg where id in (SELECT id
              FROM (SELECT id, mm_date,advertiser_id,campaign_id,strategy_id,
                             ROW_NUMBER() OVER (partition BY mm_date,advertiser_id,campaign_id,strategy_id ORDER BY id desc) AS rnum
                     FROM gap_first_touch_agg) t
              WHERE t.rnum > 1)
'''
rows_affected = cursor.execute(dups)
conn.commit()
cursor.close()
conn.close()
# print(rows_affected)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create the file in s3 for clients to download.  Prevent errors in future if the file is too big by providing a link instead of attaching the file together with the Email
# MAGIC 2. Expire the file within 1 day so that they only have access to the latest 1 

# COMMAND ----------

def upload_to_client_s3(filename,s3path):
  s3 = boto3.client(
    's3',
    aws_access_key_id=credentials['aws_reporting_keys']['access_key'],
    aws_secret_access_key=credentials['aws_reporting_keys']['secret_key']
  )
  s3.upload_file(filename,'mm-analytics-client-distribution', s3path + filename)
  url = s3.generate_presigned_url(
    ClientMethod='get_object',
    Params={
        'Bucket': 'mm-analytics-client-distribution',
        'Key': s3path + filename
    },
    ExpiresIn=60 *60*24
  )
  return url

# COMMAND ----------

# MAGIC %md
# MAGIC #####Give the DAG db some time to clear the duplicates before sending them out 
# MAGIC 
# MAGIC **this queries are executed sequentially, so the duplicates should have been removed by the time of execution of the next cell

# COMMAND ----------

import time
time.sleep(5)


# COMMAND ----------

file_name = 'GAP_performance_report_first_touch_{}.csv'.format(datetime.datetime.now().strftime('%Y-%m-%d'))


# COMMAND ----------

df = pd.read_sql_query('''
select
  mm_date
  ,advertiser_id
  ,advertiser_name
  ,campaign_id
  ,campaign_name
  ,strategy_id
  ,strategy_name
  ,total_spend
  ,media_cost
  ,impressions
  ,clicks
  ,online_conversion
  ,online_revenue
  ,offline_conversion
  ,offline_revenue
  ,pv_online_conversion	
  ,pc_online_conversion
  ,pv_online_revenue
  ,pc_online_revenue
	


from gap_first_touch_agg order by mm_date''',con=db_engine)
final_df_file = df.to_csv(file_name,index=False)

# COMMAND ----------

# MAGIC %run "/Users/acandela@mediamath.com/reporting_utils/email_tools"

# COMMAND ----------

EMAIL_BODY = ''' 
Hi GAP!

GAP Performance report - first touch has been updated. You can download the file from the following link.

{}

*** The link will expire in 1 day***


We are still in the midst of testing this new process out. If you find anything amiss or would want any changes, feel free to reach out to me at stan@mediamath.com

Thank you
MediaMath Analytics
'''

SUBJECT = 'GAP Performance Report (First Touch)'

# COMMAND ----------

# MAGIC %run "/Users/acandela@mediamath.com/reporting_utils/email_tools"

# COMMAND ----------

e = EMAIL()
e.send_email(SUBJECT,EMAIL_BODY.format(upload_to_client_s3(file_name,'tmp/')),["Mark_chong@gap.com","Kelli_Hashimoto@gap.com"],cc=["Matthew_Seabrook@gap.com"],bcc=['mm_gap@mediamath.com','nhuang@mediamath.com','stan@mediamath.com'])
# e.send_email(SUBJECT,EMAIL_BODY.format(upload_to_client_s3(file_name,'tmp/')),['stan@mediamath.com'])

# COMMAND ----------

import os
os.remove(file_name)

# COMMAND ----------

