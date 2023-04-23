# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import os
import pandas as pd
import sys
from conn import SQLQueryer

class Queryer(SQLQueryer):
    def __init__(self, mode='result'):
        super().__init__(mode)

    # get original datas of a single product from database
    def get_data(self, product = '葡萄酒'):
        file_name = 'data/{}'.format(product)
        if os.path.exists('{}.pkl'.format(file_name)):
            df = pd.read_pickle('{}.pkl'.format(file_name))
            sys.stdout.write('Get local cache: {}\n'.format(file_name))
            return df
        sys.stdout.write('[INFO]Begin get product: {}\n'.format(product))
        query = '''
            SELECT
                sp_s_19 AS product,
                spdata_0 AS test,
                CAST(spdata_1 AS DECIMAL(10,6)) AS value,
                DATE(created_at) AS date,
                created_at AS datetime
            FROM sp_bsbs_data 
            WHERE sp_s_19=\'{}\'
                AND CAST(spdata_1 AS DECIMAL(10,2)) > 0
                AND spdata_2=\'合格项\'
            ORDER BY sp_s_19, spdata_0, DATE(created_at)
        '''.format(product)
        df = self.get_df(query)
        # df.to_csv('{}.csv'.format(file_name))
        df.to_pickle('{}.pkl'.format(file_name))
        sys.stdout.write('[SUCC]Successfully get product: {}\n'.format(product))
        return df
    
    def get_sample_time(self):
        file_name = 'data/sample_time'
        if os.path.exists('{}.pkl'.format(file_name)):
            df = pd.read_pickle('{}.pkl'.format(file_name))
            sys.stdout.write('[INFO] Get local cache: {}.pkl, len: {}\n'.format(file_name, len(df)))
            return df
        sys.stdout.write('[INFO] Begin get sample_time.\n')
        query = '''
            SELECT
                b.spot_no,
                b.sample_no,
                MAX(c.start_time) AS start_time
            FROM dw_sp_receive_records_clean AS b
            LEFT JOIN dw_item_process_records_clean AS c
            ON b.sample_no = c.sample_no
            GROUP BY b.spot_no, b.sample_no
        '''
        df = self.get_process_df(query)
        df.to_csv('{}.csv'.format(file_name))
        df.to_pickle('{}.pkl'.format(file_name))
        sys.stdout.write('[SUCC] Successfully get sample_time.\n')
        return df
    
    # sp_bsbs_data表的sp_data_16行关联dw_sp_receive_records_clean的spot_no行，
    # dw_sp_receive_records_clean的sample_no行关联dw_item_process_records_clean的sample_no行
    def get_data_joined(self, product = '葡萄酒'):
        file_name = 'data/{}'.format(product)
        if os.path.exists('{}_join.pkl'.format(file_name)): # 请求过
            df = pd.read_pickle('{}_join.pkl'.format(file_name))
            sys.stdout.write('[INFO] Get local cache: {}_join.pkl, len: {}\n'.format(file_name, len(df)))
            return df
        if os.path.exists('{}.pkl'.format(file_name)): # 获取未join的df
            df = pd.read_pickle('{}.pkl'.format(file_name))
            sys.stdout.write('[INFO] Get local cache: {}.pkl, len: {}\n'.format(file_name, len(df)))
        else:
            sys.stdout.write('[INFO] Begin get product: {}.\n'.format(product))
            query = '''
                SELECT a.sp_s_19 AS product,
                    b.name_new AS test,
                    a.sp_s_16 AS spot_no,
                    CAST(a.spdata_1 AS DECIMAL(10,6)) AS value
                FROM sp_bsbs_data as a
                LEFT JOIN detection_name_new AS b
                ON a.spdata_0 = b.spdata_0
                WHERE a.sp_s_19=\'{}\'
                    AND CAST(a.spdata_1 AS DECIMAL(10,2)) > 0
                    AND a.spdata_2='合格项'
            '''.format(product)
            df = self.get_df(query)
            df.to_csv('{}.csv'.format(file_name))
            df.to_pickle('{}.pkl'.format(file_name))
            sys.stdout.write('[SUCC] Successfully get product: {}, len: {}.\n'.format(product, len(df)))
        df2 = self.get_sample_time()
        df = pd.merge(df, df2, on='spot_no', how='left')
        df = df.dropna() # 丢弃NaN（未匹配到检测时间）
        df.reset_index(drop = True)
        df.to_csv('{}_join.csv'.format(file_name))
        df.to_pickle('{}_join.pkl'.format(file_name))
        sys.stdout.write('[SUCC] Successfully merge data: {}, len: {}.\n'.format(product, len(df)))
        return df

    # get date avg of a single product from database
    def get_date_avg(self, product = '葡萄酒'):
        file_name = 'data/{}_date_avg.csv'.format(product)
        if os.path.exists(file_name):
            df = pd.read_csv('{}'.format(file_name))
            sys.stdout.write('Get local cache: {}\n'.format(file_name))
            return df
        query = '''
            SELECT 
                sp_s_19 AS product,
                spdata_0 AS test,
                COUNT(*) AS data_count,
                AVG(spdata_1) AS avg_value,
                DATE(created_at) AS date,
            FROM sp_bsbs_data 
            WHERE sp_s_19=\'{}\'
                AND CAST(spdata_1 AS DECIMAL(10,2)) > 0
            GROUP BY sp_s_19, spdata_0, DATE(created_at)
        '''.format(product)
        df = self.get_df(query)
        df.to_csv(file_name)
        return df
    
    # def filter_line(df):

# calculate category num
def category_num(queryer):
    sql = '''
        SELECT 
            sp_s_19 AS product,
            spdata_0 AS test,
            COUNT(*) AS record_num
        FROM sp_bsbs_data
        GROUP BY sp_s_19, spdata_0
    '''
    df = queryer.get_df(sql)
    df.to_csv('data/cate_num.csv')
# category_num(Queryer())

def plot_df(df, x = 'date', y = 'avg_value'):
    df.plot(kind = 'point', x = 'date', y = 'avg_value')
    plt.show()
    # plt.save_fig

def plot_scatter(df, x = 'date', y = 'value', fig_name = None):
    try:
        df[y] = df[y].astype(float)
    except:
        return
    if fig_name is None:
        fig_name = 'test'
    plt.figure()
    plt.plot(df[x], df[y], '.')
    plt.xlabel(x)
    plt.ylabel(y)
    # plt.title(fig_name)
    plt.savefig('./result/image/{}.png'.format(fig_name))
    # plt.show()
    plt.close()

def get_product_list():
    with open('data/cate_3.txt', 'r') as f:
        data = list(map(lambda x: x.strip(), f.readlines()))
    return data

def plot_all_data_dist(): # not joined data
    queryer = Queryer()
    product_list = get_product_list()
    for product in product_list:
        df = queryer.get_data(product)
        test_list = df['test'].unique()
        for test in test_list:
            test_df = df.loc[df['test'] == test]
            if len(test_df) < 10: 
                print('test_df num < 10: {}-{} len: {}'.format(product, test ,len(test_df)))
                continue
            plot_scatter(test_df, fig_name = '{}-{}'.format(product, test))

def plot_all_joined_data(): # joined data
    queryer = Queryer()
    product_list = get_product_list()
    for product in product_list:
        try:
            file_name = 'data/{}'.format(product)
            if os.path.exists('{}_join.pkl'.format(file_name)):
                continue
            df = queryer.get_data_joined(product)
            test_list = df['test'].unique()
            for test in test_list:
                try:
                    test_df = df.loc[df['test'] == test]
                    if len(test_df) < 10: 
                        continue
                    plot_scatter(test_df, x = 'start_time', y = 'value', fig_name = '{}-{}'.format(product, test))
                except Exception as e:
                    sys.stdout.write('{}\n'.format(e))
        except Exception as e:
            sys.stdout.write('{}\n'.format(e))

if __name__ == '__main__':
    plot_all_joined_data()
    # queryer = Queryer()
    # queryer.get_sample_time()
    # queryer.get_data_joined()
    # df = queryer.get_data_joined('叶菜类蔬菜')
    # df = df.loc[df['test'] == '阿维菌素']
    # plot_scatter(df, x = 'start_time', y = 'value', fig_name = '{}-{}'.format('叶菜类蔬菜', '阿维菌素'))
    # mx_value = df['value'].max()
    # mn_value = df['value'].min()
    # print(mx_value, mn_value)
    # plot_scatter(df)
    # df.set_index('date')
    # 
    # 
    pass
    



# 一个dataframe中的数据点有多个test属性，根据test属性分不同series画图，y轴为avg_value

# 下一行
# row1 = cursor.fetchone()

# group by time
def group_by_time(df):
    df['time'] = pd.to_datetime(df['time'])
    df[['date', 'time']] = df['time'].str.split(' ', n=1, expand=True)

    # 将 date 列和 time 列转换为 datetime64[ns] 类型
    df['date'] = pd.to_datetime(df['date'])
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')

    df.set_index('date', inplace=True)
    # 对 DataFrame 按日期进行排序
    df_sorted = df.sort_index()

    # 按日期聚合数据点，计算均值
    df_mean = df_sorted.resample('D').mean() # 按日期聚合





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
dw_item_process_records_clean	上报-实验室检测过程记录 -》 检测开始时间
dw_rp_process_records_clean	上报-报告编制记录数据
dw_sp_receive_records_clean	上报-样品接收记录

'''

# alias python=/Users/huang/anaconda3/envs/torch/bin/python
