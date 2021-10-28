#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd

# 文件读取
# read_row_num是已读行数，filepath_usage是要读的文件
def read_container(read_row_num, filepath_usage):
    id = pd.read_csv(filepath_usage, skiprows = read_row_num, nrows = 1, header = None)[0].values # 读取未读数据的第一行
    id = id[0] # 读取该行的第一个值，即获取本次需处理的container_id
    
    while pd.isna(id): # 如果当前id为nan就跳过，直到读到的id不为nan
        read_row_num += 1
        id = pd.read_csv(filepath_usage, skiprows = read_row_num, nrows = 1, header = None)[0].values # 读取下一行
        id = id[0]

    # 需要的是[container_id, time_stamp, cpu_util_percent]这几列
    reader_usage = pd.read_csv(filepath_usage, skiprows = read_row_num, header = None, usecols = [0, 2, 3], iterator = True)

    # 分块读取
    loop = True
    chunkSize = 1 # 每次读一行
    chunks = []
    while loop:
        try:
            chunk = reader_usage.get_chunk(chunkSize)
            
            id_now = chunk[0].values
            id_now = id_now[0]
            
            if pd.isna(id_now): # id为空则跳过
                read_row_num += 1
                continue
            elif id_now == id: # 该行的id与本次需处理的id相同则添加该行
                chunks.append(chunk)
                read_row_num += 1
            else:
                loop = False
        except StopIteration:
            loop = False
            print("Iteration is stopped.")

    # 拼接成一个dataframe
    df_usage = pd.concat(chunks, ignore_index=True)

    return df_usage, id, read_row_num # 返回当前处理的container的id号、整个文件的已读行数、以及读出来的该id的dataframe


# 插值
def interpolation(df):
    # 初始时间和结束时间
    minTimeStamp = 86400
    maxTimeStamp = 777600 # 8天
    res = pd.DataFrame(columns = ['cpu_util_96']) # 新建空dataframe，用来存储插值之后的数据
    
    try:
        time = minTimeStamp
        dflen = len(df)
        stamp = int(df['time_stamp'][0])
        
        # 第一个值之前的都填充0
        while (time <= stamp):
            # cpu_util_96 = float(df['cpu_util_96'][0])
            if time == stamp:
                cpu_util_96 = float(df['cpu_util_96'][0])
            else:
                cpu_util_96 = 0
            res = res.append([{'cpu_util_96': cpu_util_96}], ignore_index = True)
            time += 300 # 每隔5min插值一次
            
        # 在有值的时间范围内线性插值
        for j in range(1, dflen):
            while time <= int(df['time_stamp'][j]):
                # 线性插值
                x1 = int(df['time_stamp'][j-1])
                x2 = int(df['time_stamp'][j])
                y1 = float(df['cpu_util_96'][j-1])
                y2 = float(df['cpu_util_96'][j])
                a = (y1 - y2) / (x1 - x2)
                b = y1 - a * x1
                cpu_util_96 = a * time + b
                
                res = res.append([{'cpu_util_96': cpu_util_96}], ignore_index = True)
                time += 300
        
        # 最后一个值之后的都填充0
        while (time <= maxTimeStamp):
            # cpu_util_96 = float(df['cpu_util_96'][dflen-1])
            cpu_util_96 = 0
            res = res.append([{'cpu_util_96': cpu_util_96}], ignore_index = True)
            time += 300
        
        return res
    except FileNotFoundError:
        print('Error in interpolation！') 


# 主函数
if __name__ == '__main__':
    # 读container_meta
    df_meta = pd.read_csv('./container_meta.csv', header = None, usecols=[0, 1, 5], names = ['container_id', 'machine_id', 'cpu_request'])
    df_meta = df_meta.drop_duplicates(subset = ['container_id'], keep='first') # 去掉重复行

    # 读conatiner_usage
    filepath_usage = './container_usage.csv'
    read_row_num = 0 # 已读行数

    df_only_machine = pd.DataFrame(columns = ['machine_id']) # 用于存储container的初始放置machine_id
    index = 0

    # while read_row_num < len_usage: # 当表没读完，len_usage = 4015763787
    # while read_row_num < len_usage and index < 5: # 读取前index+1个
    while read_row_num < 4015763787:
        # 每次读取一个container_id的所有行
        df_usage, id, read_row_num = read_container(read_row_num, filepath_usage)
        print('Successfully read: id =', id, ', read_row_number =', read_row_num)
    
        # 计算实际资源量
        df_usage.columns = ['container_id', 'time_stamp', 'cpu_util_percent']
        df_meta_id = df_meta[(df_meta['container_id'] == id)] # 找到相同id的container_meta中的对应的那行
        df_meta_id = df_meta_id.reset_index(drop = True) # 重置行索引
        plan_cpu = df_meta_id.loc[:1, ['cpu_request']].values[0][0] # 获取该id对应的plan_cpu
        # 每个id有好几行，取第一行的相应值（不过其实前面读container_meta的时候以及去掉重复行了，问题不大）

        df_usage['cpu_util_96'] = df_usage['cpu_util_percent'] * plan_cpu / 96 # 计算当前使用量占整个机器的比值（实际使用量/机器配置核数）
    
        df_usage_2 = df_usage.loc[:, ['time_stamp', 'cpu_util_96']] # 去掉列
        df_usage_2 = df_usage_2.dropna()
        df_usage_2 = df_usage_2.reset_index(drop = True) # 重置索引
    
        # 插值
        df_usage_complete = interpolation(df_usage_2)
        df_usage_complete['cpu_util_96'] = df_usage_complete['cpu_util_96'].astype(int)
    
        # 保存
        df_usage_complete.to_csv('./alibaba2018/workload/instanceid_{}.csv'.format(id), index = None, header = None)
    
        # 获取配置信息
        machine_id = df_meta_id.loc[:1, ['machine_id']].values[0][0] # 获取该id对应的machine_id
        machine_id = "".join(list(filter(str.isdigit, machine_id))) # 获取数字部分
        machine_id = int(machine_id) # 修改id为int型
        df_only_machine.loc[index] = "".join(list(filter(str.isdigit, machine_id))) # 添加到dataframe中
    
        index += 1
        print('number of container =', index)
    
    df_only_machine.to_csv('./alibaba2018/instance_machine_id.csv', header = None) # 所有container对应的machine_id的dataframe，保存