import pandas as pd
import openpyxl

file='数据中台ODS层设计文档_v1.0@20200108.xlsx'

# def type_map():
#     '''
#     :return: 获取字段类型映射
#     '''
#     data = pd.read_excel(file, sheet_name=2, header=1)
#     type_dict = {}
#     for index, row in data.iterrows():
#         mysql_type = row['mysql字段类型']
#         odps_type = row['odps字段类型']
#         type_dict[mysql_type] = odps_type
#     data_type='int'
#     type_dict=type_map()
#     if data_type in type_dict.keys():
#         odps_type=type_dict.values()
#
#     for x,y in type_dict.items():
#         print(x,y)


# def db_table():
#     '''
#     :return:获取mysql库中的库名以及表名，以逗号分隔
#     '''
#     data=pd.read_excel(file,sheet_name=1,header=0)
#     data1=data[['源库名称','源表名称','ODS层表名称','是否存在update/delete行为','增量/全量']].dropna(subset=['源库名称'])
#     table_db_list=[]
#     for index, row in data.iterrows():
#         mysql_db = row['源库名称']
#         mysql_table = row['源表名称']
#         odps_table = row['ODS层表名称']
#         is_update = row['是否存在update/delete行为']
#         is_full = row['增量/全量']
#         db_table='{mysql_db},{mysql_table}'.format(mysql_db=mysql_db,mysql_table=mysql_table)
#         if db_table=='nan,nan':
#             continue
#         mysql_table_info='{mysql_db},{mysql_table},{is_update},{odps_table},{is_full}'.format(mysql_db=mysql_db,mysql_table=mysql_table,is_update=is_update,odps_table=odps_table,is_full=is_full)
#         print(mysql_table_info)
#         table_db_list.append(mysql_table_info)
#         print(data1)
#
#     return table_db_list






# print(mysql_type)
# print(odps_type)

merge_sql_tmp="""
INSERT OVERWRITE TABLE {odps_table} PARTITION(ds='${{bdp.system.bizdae}}')
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
                        WHERE   ds = '${{bdp.system.bizdae}}'
                    ) a
        ) b
WHERE   num = 1
;
"""
col_name_all='id'
PK='id'
odps_table='name'

nmerge_sql=merge_sql_tmp.format(odps_table=odps_table,col_name_all=col_name_all,PK=PK)

print(nmerge_sql)