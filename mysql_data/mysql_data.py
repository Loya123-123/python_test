import os
import numpy as np
import pandas as pd
import openpyxl
from mysqlUtil import mysqlUtil

desc_sql="desc t28000_member.person;"

info_sql="""
select 
paas_info.schema_name,
paas_info.tb_name,
paas_info.tb_comment,
source_info.col_name,
source_info.col_type,
source_info.data_type,
source_info.is_not_null,
paas_info.col_comment,
source_info.key_info,
source_info.k_refer_tb_name,
source_info.k_refer_col_name
 FROM (
SELECT
  ff.COLUMN_NAME AS col_name,
  max(ff.COLUMN_TYPE) AS col_type,
  max(ff.DATA_TYPE) AS data_type,
  max(is_not_null) AS is_not_null,
  max(ff.COLUMN_COMMENT) AS col_comment,
  group_concat(ff.CONSTRAINT_NAME) AS key_info,
  group_concat(ff.REFERENCED_TABLE_NAME) AS k_refer_tb_name,
  group_concat(ff.REFERENCED_COLUMN_NAME) AS k_refer_col_name
FROM
  (
    SELECT
      aa.COLUMN_NAME,
      aa.COLUMN_TYPE,
      aa.DATA_TYPE,
      CASE
        WHEN aa.IS_NULLABLE = 'YES' THEN 'N'
        WHEN aa.IS_NULLABLE = 'NO' THEN 'Y'
      END AS is_not_null,
      aa.COLUMN_COMMENT,
      cc.TABLE_COMMENT,
      CASE
        WHEN ee.CONSTRAINT_NAME IS NULL OR ee.CONSTRAINT_NAME = '' THEN ''
        WHEN ee.CONSTRAINT_NAME = 'PRIMARY' THEN 'PK'
        ELSE 'FK'
      END AS CONSTRAINT_NAME,
      ee.REFERENCED_TABLE_NAME,
      ee.REFERENCED_COLUMN_NAME
    FROM
      information_schema. COLUMNS aa
    LEFT JOIN (
      SELECT DISTINCT
        bb.TABLE_SCHEMA,
        bb.TABLE_NAME,
        bb.TABLE_COMMENT
      FROM
        information_schema. TABLES bb
    ) cc ON (
      aa.TABLE_SCHEMA = cc.TABLE_SCHEMA
      AND aa.TABLE_NAME = cc.TABLE_NAME
    )
    LEFT JOIN (
      SELECT
        dd.CONSTRAINT_NAME,
        dd.TABLE_SCHEMA,
        dd.TABLE_NAME,
        dd.COLUMN_NAME,
        dd.REFERENCED_TABLE_NAME,
        dd.REFERENCED_COLUMN_NAME
      FROM
        information_schema.KEY_COLUMN_USAGE dd
    ) ee ON (
      aa.TABLE_SCHEMA = ee.TABLE_SCHEMA
      AND aa.TABLE_NAME = ee.TABLE_NAME
      AND aa.COLUMN_NAME = ee.COLUMN_NAME
    )
    WHERE
      aa.TABLE_SCHEMA = 't28000_member'
    AND aa.TABLE_NAME = 'person'
  ) ff
GROUP BY ff.COLUMN_NAME 
) source_info 
LEFT JOIN (
SELECT
  CONCAT('t',bc.tenant_id ,'_',bc.server_name) as schema_name,
  bc.table_name as tb_name,
  bc. NAME as tb_comment,
  fd.column_name as col_name,
  fd.name as col_comment,
  fd.data_type as col_type
FROM
  business_component bc
LEFT JOIN (SELECT * FROM field) fd ON bc.uuid = fd.bc
WHERE
  bc.table_name in ('person')
) paas_info 
on source_info.col_name = paas_info.col_name
;
"""

def mysql_data_change(sq_data):
    a = list(sq_data)
    lists = []
    for i in a:
        b = list(i)
        lists.append(b)
    return lists

db = mysqlUtil()
info_data = db.getAll(info_sql)

desc_data = db.getAll(desc_sql)

all_date=mysql_data_change(info_data)

desc = mysql_data_change(desc_data)

df1 = pd.DataFrame(all_date)
df1.columns=['schema_name','tb_name','tb_comment','col_name','col_type','data_type','is_not_null','col_comment','key_info','k_refer_tb_name','k_refer_col_name']
df2 = pd.DataFrame(desc)
df2.columns=['col_name','col_type','is_not_null','key_info','default','extra']

df3 = pd.merge(df2, df1, on='col_name')

df4=df3[['schema_name','tb_name','tb_comment','col_name','col_type_x','data_type','is_not_null_x','col_comment','key_info_x','k_refer_tb_name','k_refer_col_name']]
df4.columns=['schema_name','tb_name','tb_comment','col_name','col_type','data_type','is_not_null','col_comment','key_info','k_refer_tb_name','k_refer_col_name']

print(df4)

writer=pd.ExcelWriter('D:/workspace/pycharm-work/mysql_data/table_info.xlsx')

df4.to_excel(writer,sheet_name='person')
writer.save()