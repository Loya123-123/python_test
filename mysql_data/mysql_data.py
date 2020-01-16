import os
import pandas as pd
import openpyxl
from mysqlUtil import mysqlUtil
import time


time_str = time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime())
#
# desc_sql="desc t28000_member.person;"
#
# info_sql="""
# select
# paas_info.schema_name,
# paas_info.tb_name,
# paas_info.tb_comment,
# source_info.col_name,
# source_info.col_type,
# source_info.data_type,
# source_info.is_not_null,
# paas_info.col_comment,
# source_info.key_info,
# source_info.k_refer_tb_name,
# source_info.k_refer_col_name
#  FROM (
# SELECT
#   ff.COLUMN_NAME AS col_name,
#   max(ff.COLUMN_TYPE) AS col_type,
#   max(ff.DATA_TYPE) AS data_type,
#   max(is_not_null) AS is_not_null,
#   max(ff.COLUMN_COMMENT) AS col_comment,
#   group_concat(ff.CONSTRAINT_NAME) AS key_info,
#   group_concat(ff.REFERENCED_TABLE_NAME) AS k_refer_tb_name,
#   group_concat(ff.REFERENCED_COLUMN_NAME) AS k_refer_col_name
# FROM
#   (
#     SELECT
#       aa.COLUMN_NAME,
#       aa.COLUMN_TYPE,
#       aa.DATA_TYPE,
#       CASE
#         WHEN aa.IS_NULLABLE = 'YES' THEN 'N'
#         WHEN aa.IS_NULLABLE = 'NO' THEN 'Y'
#       END AS is_not_null,
#       aa.COLUMN_COMMENT,
#       cc.TABLE_COMMENT,
#       CASE
#         WHEN ee.CONSTRAINT_NAME IS NULL OR ee.CONSTRAINT_NAME = '' THEN ''
#         WHEN ee.CONSTRAINT_NAME = 'PRIMARY' THEN 'PK'
#         ELSE 'FK'
#       END AS CONSTRAINT_NAME,
#       ee.REFERENCED_TABLE_NAME,
#       ee.REFERENCED_COLUMN_NAME
#     FROM
#       information_schema. COLUMNS aa
#     LEFT JOIN (
#       SELECT DISTINCT
#         bb.TABLE_SCHEMA,
#         bb.TABLE_NAME,
#         bb.TABLE_COMMENT
#       FROM
#         information_schema. TABLES bb
#     ) cc ON (
#       aa.TABLE_SCHEMA = cc.TABLE_SCHEMA
#       AND aa.TABLE_NAME = cc.TABLE_NAME
#     )
#     LEFT JOIN (
#       SELECT
#         dd.CONSTRAINT_NAME,
#         dd.TABLE_SCHEMA,
#         dd.TABLE_NAME,
#         dd.COLUMN_NAME,
#         dd.REFERENCED_TABLE_NAME,
#         dd.REFERENCED_COLUMN_NAME
#       FROM
#         information_schema.KEY_COLUMN_USAGE dd
#     ) ee ON (
#       aa.TABLE_SCHEMA = ee.TABLE_SCHEMA
#       AND aa.TABLE_NAME = ee.TABLE_NAME
#       AND aa.COLUMN_NAME = ee.COLUMN_NAME
#     )
#     WHERE
#       aa.TABLE_SCHEMA = 't28000_member'
#     AND aa.TABLE_NAME = 'person'
#   ) ff
# GROUP BY ff.COLUMN_NAME
# ) source_info
# LEFT JOIN (
# SELECT
#   CONCAT('t',bc.tenant_id ,'_',bc.server_name) as schema_name,
#   bc.table_name as tb_name,
#   bc. NAME as tb_comment,
#   fd.column_name as col_name,
#   fd.name as col_comment,
#   fd.data_type as col_type
# FROM
#   business_component bc
# LEFT JOIN (SELECT * FROM field) fd ON bc.uuid = fd.bc
# WHERE
#   bc.table_name in ('person')
# ) paas_info
# on source_info.col_name = paas_info.col_name
# ;
# """

merge_sql_tmp="""
INSERT OVERWRITE TABLE {odps_table} PARTITION(ds='${{bdp.system.bizdate}}')
SELECT  {col_name_all}
FROM    (
            SELECT  {col_name_all}
                    ,ROW_NUMBER() OVER(PARTITION BY a.{PK} ORDER BY a.update_time DESC ) AS num
            FROM    (
                        SELECT  {col_name_all}
                        FROM    {odps_table}
                        WHERE   ds = '${{befor_yesteday}}'
                        UNION ALL
                        SELECT  {col_name_all}
                        FROM    {odps_table}_delta
                        WHERE   ds = '${{bdp.system.bizdate}}'
                    ) a
        ) b
WHERE   num = 1
;
"""

desc_sql_tmp ="desc {dabase}.{table_name};"

info_sql_tmp="""
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
      aa.TABLE_SCHEMA = '{dabase}'
    AND aa.TABLE_NAME = '{table_name}'
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
  bc.table_name in ('{table_name}')
) paas_info 
on source_info.col_name = paas_info.col_name
GROUP BY paas_info.schema_name,
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
;
"""

file='数据中台ODS层设计文档_v1.0@20200108.xlsx'

def mysql_data_change(sq_data):
    '''

    :param sq_data: sql查询结果
    :return: numpy数据输出
    '''
    a = list(sq_data)
    lists = []
    for i in a:
        b = list(i)
        lists.append(b)
    return lists

def col_position(all_date,desc):
    '''
    :param all_date:元数据查询的完整表信息
    :param desc:desc有排序的表字段信息
    :return:元数据的完整有排序的表信息
    '''
    df1 = pd.DataFrame(all_date)
    df2 = pd.DataFrame(desc)
    df1.columns = ['schema_name', 'tb_name', 'tb_comment', 'col_name', 'col_type', 'data_type', 'is_not_null',
                   'col_comment', 'key_info', 'k_refer_tb_name', 'k_refer_col_name']
    df2.columns = ['col_name', 'type', 'null', 'key', 'default', 'extra']
    df3 = pd.merge(df2, df1, on='col_name')
    df4 = df3[['schema_name', 'tb_name', 'tb_comment', 'col_name', 'col_type', 'data_type',
               'is_not_null', 'col_comment', 'key_info', 'k_refer_tb_name', 'k_refer_col_name']]
    return df4

def type_map():
    '''
    :return: 获取字段类型映射
    '''
    data = pd.read_excel(file, sheet_name=2, header=1)
    type_dict = {}
    for index, row in data.iterrows():
        mysql_type = row['mysql字段类型']
        odps_type = row['odps字段类型']
        type_dict[mysql_type] = odps_type
    return type_dict

def db_table():
    '''
    :return:获取mysql库中的库名以及表名，以逗号分隔
    '''
    data=pd.read_excel(file,sheet_name=1,header=0)
    data1=data[['源库名称','源表名称','ODS层表名称','是否存在update/delete行为','增量/全量']].dropna(subset=['源库名称'])
    return data1

file_xlsx="table_info_%s.xlsx"%time_str
writer=pd.ExcelWriter(file_xlsx)

file_sql="create_table_info_%s.sql"%time_str
file_merge_sql="merge_sql_info_%s.sql"%time_str

data1=db_table()
for index, row in data1.iterrows():
    dabase = row['源库名称']
    table_name = row['源表名称']
    odps_table = row['ODS层表名称']
    is_update = row['是否存在update/delete行为']
    is_full = row['增量/全量']
    desc_sql=desc_sql_tmp.format(dabase=dabase,table_name=table_name)
    info_sql=info_sql_tmp.format(dabase=dabase,table_name=table_name)
    db = mysqlUtil()
    info_data = db.getAll(info_sql)
    desc_data = db.getAll(desc_sql)
    all_date=mysql_data_change(info_data)
    desc = mysql_data_change(desc_data)
    df4=col_position(all_date,desc)
    array_PK = df4[df4['key_info'] == 'PK']['col_name'].values
    PK=array_PK[0]
    df4.to_excel(writer,sheet_name=table_name)

    for index, row in df4.iterrows():
        tb_name=row['tb_name']
        tb_comment=row['tb_comment']
        break

    type_dict=type_map()
    col_infos = []
    col_names = []
    for index, row in df4.iterrows():
        col_name=row['col_name']
        col_type=row['col_type']
        col_comment = row['col_comment']
        if col_comment=='None':
            col_comment=''
        data_type=row['data_type']
        if data_type in type_dict.keys():
            odps_type = type_dict[data_type]
        col_info = "{col_name} {odps_type} COMMENT '{col_comment}'".format(col_name=col_name,odps_type=odps_type,col_comment=col_comment)
        col_infos.append(col_info)
        col_names.append(col_name)
    col_name_all=',\n'.join(col_names)
    col_info_all=',\n'.join(col_infos)
    if (is_update=='U'and is_full=='初始化一次') or is_full == '全量/天':
        create_tb_sql = '''CREATE TABLE IF NOT EXISTS {odps_table} (
            {col_info_all}) COMMENT '{tb_comment}'
        PARTITIONED BY (ds string) LIFECYCLE 14;
        '''.format(odps_table=odps_table,col_info_all=col_info_all,tb_comment=tb_comment)
        with open(os.path.join(os.getcwd(), file_sql), 'a+', encoding='utf-8') as f:
            f.write(create_tb_sql)
    elif is_full =='增量/天' :
        create_tb_sql = '''CREATE TABLE IF NOT EXISTS {odps_table} (
            {col_info_all}) COMMENT '{tb_comment}'
            PARTITIONED BY (ds string) ;
        '''.format(odps_table=odps_table,col_info_all=col_info_all,tb_comment=tb_comment)
        with open(os.path.join(os.getcwd(), file_sql), 'a+', encoding='utf-8') as f:
            f.write(create_tb_sql)
    if is_update=='U'and is_full=='初始化一次' :
        create_tb_sql = '''CREATE TABLE IF NOT EXISTS {odps_table}_delta (
                {col_info_all}) COMMENT '{tb_comment}'
            PARTITIONED BY (ds string) ;
            '''.format(odps_table=odps_table, col_info_all=col_info_all, tb_comment=tb_comment)
        with open(os.path.join(os.getcwd(), file_sql), 'a+', encoding='utf-8') as f:
            f.write(create_tb_sql)
        nmerge_sql=merge_sql_tmp.format(odps_table=odps_table,col_name_all=col_name_all,PK=PK)
        with open(os.path.join(os.getcwd(), file_merge_sql), 'a+', encoding='utf-8') as f:
            f.write(nmerge_sql)

    print("{table_name} 表建表语句已生成".format(table_name=table_name))

writer.save()
