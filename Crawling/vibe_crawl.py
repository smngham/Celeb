
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




def db_connection(host_name='ds'):
    host_url = ""
    user_nm = ""
    passwd = ""
    port_num = 
    db_name = ""
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           db=db_name, cursorclass=pymysql.cursors.DictCursor)
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    return conn, engine
  

empty_frame = pd.DataFrame(columns=('star_id', 'star_name', 'album_title', 'album_date'))


now = datetime.datetime.now()
date = now.strftime('%Y-%m-%d')
conn, engine = db_connection()
cursor = conn.cursor(pymysql.cursors.DictCursor)
#DB에 존재하는 인물들 중 네이버 바이브 코드 값이 있는 row만 추출
qry = "SELECT * FROM star_album_genre WHERE vibeCd is NOT NULL and vibeCd != -1"
ex = pd.read_sql(qry, conn)

print("[INFO]", len(ex), "개 개체의 vibe 수집을 시작합니다 ... ")

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

for i, row in tqdm(ex.iterrows(), total=ex.shape[0] < 10):
    series_id = row['series_id']
    vibeCd = row['vibeCd']
    name = row['name']
    print("\n")
    print(series_id, "-", name, "crawling ... ")
    qry = f"SELECT * from star_album_genre WHERE series_id={series_id}"
    result = pd.read_sql(qry, conn)

    try :
        vibe_url = 'https://vibe.naver.com/artist/' + str(int(vibeCd)) + '/albums'
        driver.get(vibe_url)
        time.sleep(1)
        try:
            driver.find_element_by_xpath('//*[@id="app"]/div[2]/div/div/a[2]').click()  # 팝업창 끄기
        except:
            pass

        while True:
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  # 스크롤 끝까지 내리기
                time.sleep(1)
                driver.find_element_by_xpath('//*[@id="content"]/div/div[3]/div/a').click()  # 더보기 클릭
                time.sleep(1)
            except:
                break

        vibe_html = driver.page_source
        vibe_soup = BeautifulSoup(vibe_html, 'html.parser')

        album_list = vibe_soup.select('a[class=title]')

        album_url = []
        album_title = []
        album_date =[]
        star_id = []
        star_name = []

        date1 = "01,1,2021"
        date1_f = datetime.datetime.strptime(date1, "%d,%m,%Y")

        if album_list == []:
            print("[INFO]" "전체 앨범 없음 ")
        else:
            for a in album_list:
                href = a.get('href')
                href = 'https://vibe.naver.com' + href
                album_url.append(href)
            j=0
            while True :
                newURL = album_url[j]

                driver.get(newURL)
                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')

                # 앨범명
                title = soup.select('span[class=title]')[0].text
                title = re.sub("\'", "", title)  # 특수문자 제거
                # 발매 날짜
                release_date = soup.select('span[class=item]')[0].text
                release_date = release_date.replace(' ', '')
                # 발매 날짜가 2021년 01월 01일 부터인 앨범만 수집
                if pd.to_datetime(release_date) < date1_f :
                    break

                j += 1

                df_new = pd.DataFrame({"star_id": [series_id],
                                       "star_name": [name],
                                       "album_title": [title],
                                       "album_date": [release_date]})
                print(df_new)
                empty_frame = pd.concat([empty_frame, df_new], axis=0)
                print(empty_frame)



    except Exception as e:
        print("error ! ", e)


empty_frame.to_excel('album_craw' + date + '.xlsx')



