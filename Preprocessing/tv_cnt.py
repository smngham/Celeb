import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import pymysql
from sqlalchemy import create_engine
from tqdm import tqdm
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
import time
from datetime import datetime
import datetime
import numpy as np
import os

### DB table 설명 ###
# maimovie_kr.tv_cast - 방영된 방송 데이터(출현인물, 날짜 등)
# star_ko.star_ko_data - 인물 데이터(인물 고유번호, 이름 등)

def db_connection(host_name='ds'):
    host_url = ""
    user_nm = ""
    passwd = ""
    port_num = 
    db_name = "maimovie_kr"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           db=db_name, cursorclass=pymysql.cursors.DictCursor)
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    return conn, engine
  
conn, engine = db_connection()
cursor = conn.cursor(pymysql.cursors.DictCursor)
# 방송일자 2021년 01월 01일 이후로 설정
# [regist_date] = 방송일자
qry = "SELECT sub_cd_idx FROM tv_cast WHERE regist_date >= '2021-01-01'"
ex = pd.read_sql(qry, conn)
conn.close()

df = pd.DataFrame(ex)
# [sub_cd_idx] = 방송 출현 인물의 PK , 고유 key
# value_counts() 사용해서 sub_cd_idx 별 횟수 구하기
df_tv = df['sub_cd_idx'].value_counts()
df_tv = pd.DataFrame(df_tv)
df_tv.reset_index(inplace=True)
df_tv.rename(columns={'index':'series_id',
                    'sub_cd_idx':'cnt'}, inplace = True)

# star_ko 테이블의 ['series_id']와 tv_cast 테이블 이용해 생성한 df_tv 데이터프레임의 ['series_id'] 컬럼을 기준으로 병합할 것임
def db_connection(host_name='ds'):
    host_url = ""
    user_nm = ""
    passwd = ""
    port_num = 
    db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           db=db_name, cursorclass=pymysql.cursors.DictCursor)
    # cursorclass=pymysql.cursors.DictCursor 추가 -> 데이터프레임으로 다루기 쉽게 딕셔너리 형태로 결과 반환해줌, cursor는 튜플형태
    # db=db,
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    return conn, engine

conn, engine = db_connection()
cursor = conn.cursor(pymysql.cursors.DictCursor)
qry = "SELECT cd_name, series_id FROM star_ko_data"
ex = pd.read_sql(qry, conn)
conn.close()
ex = pd.DataFrame(ex)
# 'series_id'칼럼 기준으로 merge
fin = df_tv.merge(ex, on ='series_id', how='left')
fin.to_excel('result/방송카운트.xlsx')

