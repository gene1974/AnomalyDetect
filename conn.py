import matplotlib.pyplot as plt
import pandas as pd
import pymysql

class SQLQueryer():
    def __init__(self, mode = 'result'):
        self.connect = pymysql.connect(
            host='rm-m5ev35ddqqd0m219q.mysql.rds.aliyuncs.com',
            port=3306,
            user='anomaly_analysis',
            password='123!@#QWE',
            database='anomaly-analysis'
        )
        self.cursor = self.connect.cursor()
        self.table = 'sp_bsbs_data'
        self.process_connect = pymysql.connect(
            host='223.4.65.62',
            port=408,
            user='zj_org_assess',
            password='123!@#qwe',
            database='zj_org_assess'
        )

    def query_sql(self, query, limit = None):
        if limit is not None:
            query = query + ' limit {}'.format(limit)
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        return data
    
    def get_df(self, query, limit = None): # sql to dataframe
        if limit is not None:
            query = query + ' limit {}'.format(limit)
        df = pd.read_sql(query, self.connect)
        return df
    
    def get_process_df(self, query, limit = None): # sql to dataframe
        if limit is not None:
            query = query + ' limit {}'.format(limit)
        df = pd.read_sql(query, self.process_connect)
        return df

    def __del__(self):
        self.connect.close()
        self.process_connect.close()

if __name__ == '__main__':
    queryer = SQLQueryer()

'''
结果数据：
地址：rm-m5ev35ddqqd0m219q.mysql.rds.aliyuncs.com
端口：3306
用户名：anomaly_analysis
密码：123!@#QWE
数据库：anomaly-analysis
结果数据相关表名：sp_bsbs_data

过程数据：
地址：223.4.65.62
端口：408
用户名：zj_org_assess
密码：123!@#qwe
数据库：zj_org_assess
表名	注释
dw_item_device_records_clean	上报-实验室检测过程记录
dw_item_matter_records_clean	上报-实验室检测仪器信息
dw_item_medium_records_clean	上报-实验室检标准物质记录
dw_item_process_records_clean	上报-实验室检测过程记录
dw_rp_process_records_clean	上报-报告编制记录数据
dw_sp_receive_records_clean	上报-样品接收记录

'''