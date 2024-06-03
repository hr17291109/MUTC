import glob
import os
import ntpath
import pandas as pd
import tqdm
import numpy as np
import datetime

"""
ファイルを読むときに同じのを二度読まないように出来る
引数
concatのオプション
⇒FLAGSの列の型の不一致を直す
⇒列が勝手に動く原因
"""
# def read_file():
#     '''
#     return concat data
    
#     '''
#     # a = glob.glob('twins_model_left/*.csv',recursive = True)
#     # path_list = glob.glob("twins_model_left/*.csv")

#     a = glob.glob('*.csv',recursive = True)
#     path_list = glob.glob("*.csv")

#     name_list = []
#     for n in range(250):
#         file = os.path.basename(path_list[n+250])
#         name, ext = os.path.splitext(file)
#         name_list.append(file)
#     _data=pd.DataFrame()
#     for file,name in zip(tqdm.tqdm(path_list[250:500]),name_list):
#         tmp=pd.read_csv(file,dtype = {'BMY_AIR_TEMP_OPERATIONAL_FLAGS' : object, 'BPY_AIR_TEMP_OPERATIONAL_FLAGS': object, 'WS_OPERATIONAL_FLAGS' : object, 'LTST' : object },usecols=lambda x: x not in ["SCLK", "AOBT"])
#         _data=pd.concat([_data,tmp],sort=True)
#     return _data

def read_file():
    '''
    return concat data
    
    '''
    # a = glob.glob('twins_model_left/*.csv',recursive = True)
    # path_list = glob.glob("twins_model_left/*.csv")

    a = glob.glob('*.csv',recursive = True)
    path_list = glob.glob("*.csv")

    name_list = []
    for n in range(10):
        file = os.path.basename(path_list[n+10])
        name, ext = os.path.splitext(file)
        name_list.append(file)
    _data=pd.DataFrame()
    for file,name in zip(tqdm.tqdm(path_list[0:10]),name_list):
        tmp=pd.read_csv(file, usecols=lambda x: x not in ["SCLK", "AOBT"])
        _data=pd.concat([_data,tmp],sort=True)
    return _data

def choice_data(df):
    df1 = df[df['BMY_AIR_TEMP_OPERATIONAL_FLAGS'].str.startswith('1101',na = False) |
             df['BMY_AIR_TEMP_OPERATIONAL_FLAGS'].str.startswith('1110',na = False) | 
             df['BMY_AIR_TEMP_OPERATIONAL_FLAGS'].str.startswith('1111',na = False) |
             df['BPY_AIR_TEMP_OPERATIONAL_FLAGS'].str.startswith('1101',na = False) |
             df['BPY_AIR_TEMP_OPERATIONAL_FLAGS'].str.startswith('1110',na = False) |
             df['BPY_AIR_TEMP_OPERATIONAL_FLAGS'].str.startswith('1111',na = False) |
             df['WS_OPERATIONAL_FLAGS'].str.startswith('111111',na = False) |
             df['WS_OPERATIONAL_FLAGS'].str.startswith('111101',na = False) |
             df['WS_OPERATIONAL_FLAGS'].str.startswith('111110',na = False)]
    return df1

def make_csv(df, filename):
    df.to_csv(filename)

def make_data(df,filename):
    df = choice_data(df)
    make_csv(df, filename)

def load_csv(df, filename, time=10):
    '''
    input filedata, output_filename
    '''


    LTST = df["LTST"].str.split(" ",expand=True)
    df['utcdate'] =  pd.to_datetime(df['UTC'], format="%Y-%jT%H:%M:%S.%fZ")

    t = pd.to_datetime(LTST[1])
    d = LTST[0].astype(int)
    h = pd.to_timedelta(t.dt.hour,unit='hours')
    m = pd.to_timedelta(t.dt.minute,unit='m')
    S = pd.to_timedelta(t.dt.second,unit='S')
    D = pd.to_timedelta(d,unit='D')
    df['td'] =  np.nan
    df.loc[:,'td'] = h + m + S + D
    org = datetime.datetime(year=2018, month=10, day=31, minute=0, second=0)
    df['MUTC'] = org + df.td

    wdrad = np.deg2rad(df.WIND_DIRECTION)
    df['u'] = u(df.HORIZONTAL_WIND_SPEED, wdrad)
    df['v'] = v(df.HORIZONTAL_WIND_SPEED, wdrad)

    dff = df.copy()
    dff.set_index('td',inplace=True)
    dff = dff.resample('10min').mean()
    td = dff.index



    df.set_index('MUTC',inplace=True)
    if (time==1):
        df1 = df.resample('1min').mean()
    else:
        df1 = df.resample('10min').mean()
    
    utcdate = []
    for MUTC in tqdm.tqdm(df1.index):
        if (MUTC == '2019-09-18 00:10:00'):
            utcdate += ['2019-10-23 01:30:00']
        elif (str([df.utcdate[df.index == MUTC]]) == '[Series([], Name: utcdate, dtype: datetime64[ns])]'):
            utcdate += [np.nan]
        else:
            utcdate += [df.utcdate[df.index == MUTC][0]]

    df1['td'] = td
    df1['utcdate'] = utcdate

    df1.to_csv(filename)

def u(ws, wd):
    return -ws * np.cos(wd)

def v(ws, wd):
    return -ws * np.sin(wd) 

