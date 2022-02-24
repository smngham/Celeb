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
import numpy as np
from selenium.webdriver.common.keys import Keys

# count 할 crawling data 불러오기
data = pd.read_excel('result/youtube_수집0218_1158.xlsx')
# likes, comments, views 합계
df = data.groupby(['star_id', 'subs']).agg({'likes': np.sum, 'comments': np.sum, 'views':np.sum})
