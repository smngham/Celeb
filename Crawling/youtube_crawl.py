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
from selenium.webdriver.common.keys import Keys

### DB 연결 def ###
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
  

### 크롤링한 데이터 저장 할 데이터프레임 생성 ### 
empty_frame = pd.DataFrame(columns=('star_id', 'star_name', 'date', 'subscribers','like','comments','views'))

timelabel = datetime.strftime(datetime.now(), format='%m%d_%H%M')
now = datetime.now()
date = now.strftime('%Y-%m-%d')

### DB 연결 ###
conn, engine = db_connection()
cursor = conn.cursor(pymysql.cursors.DictCursor)

### DB 컬럼 설명 ###
# [series_id] : PK, 인물에게 부여된 고유번호
# [cd_name] : 인물 이름

### DB QUERY ###
# star 테이블의 모든 컬럼에 해당하는 데이터를 가져오는 쿼리문 
qry = "SELECT * FROM star"
### DB 에서 가져온 데이터를 변수에 저장 ###
ex = pd.read_sql(qry, conn)
conn.close()
print("[INFO]", len(ex), "개 개체의 youtube 수집을 시작합니다 ... ")

### 홈페이지 접속 ###
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

### 로그인 ### 
pid = ''
ppw = ''
driver.get(f'http://{pid}:{ppw}@')
alert = driver.switch_to.alert
alert.accept()
time.sleep(2.0)
driver.find_element_by_css_selector('#adminId').send_keys('')
driver.find_element_by_css_selector('#adminPw').send_keys('')
btn = driver.find_element_by_css_selector('#loginForm > button')
btn.click()


### 크롤링 순서 ###
# 1. 쿼리문으로 가져온 모든 컬럼 중 ['series_id'] 와 ['cd_name'] 사용 #
# 2. 홈페이지 URL/{series_id} : 해당 인물의 프로필 정보, 프로필 정보 중 SNS 링크를 사용할 것임 #
# 3. SNS 링크 중 youtube 링크가 있을 경우 접속 #
# 3-1. youtube [동영상] 탭으로 이동 #
# 3-2. 2021년 01월 01일 이후 게시된 영상 정보 수집할 것임 #
# 3-3. 2021년 01월 01일 이전 게시된 영상은 pass #
# 4. 구독자 수, 좋아요 수, 댓글 수, 조회 수 수집 #
 # 5. 1~4 반복 #

for i, row in tqdm(ex.iterrows(), total=ex.shape[0]):
    series_id = row['series_id']
    name = row['cd_name']
    print("\n")
    print(series_id, "-", name, "crawling ... ")
    
    url1 = '' + str(series_id)
    driver.get(url1)
    try :
        alert = driver.switch_to.alert
        alert.accept()
        time.sleep(2.0)
    except :
        pass
    try : 
        driver.find_element_by_css_selector('#adminId').send_keys('')
        driver.find_element_by_css_selector('#adminPw').send_keys('')
        btn = driver.find_element_by_css_selector('#loginForm > button')
        btn.click()
    except :
        pass

    print('\n')
    print(name, "-", series_id)

    donut_html = driver.page_source
    donut_soup = BeautifulSoup(donut_html, 'html.parser')
    site_list = donut_soup.select('div[id=site_list]')
    # youtube 링크 접속 에러 발생하면 pass
    try:
        site_href = site_list[0].find_all("a", {"target": "_blank"})
    except :
        pass
      
# SNS 링크 중 youtube 링크 있으면 접속
    for link in site_href:
        if "www.youtube.com" in str(link):
            youtube_link = link.get("href")
            chrome_options = webdriver.ChromeOptions()
            driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
            driver.get(youtube_link)
            time.sleep(1)
            try :
                driver.find_element_by_xpath('//*[@id="tabsContent"]/tp-yt-paper-tab[2]/div').click()  # 동영상 탭 클릭
                time.sleep(1)
                
                ### 동영상 탭 페이지 스크롤 다운 ###
                # 스크롤 다운한 범위까지 게시된 동영상 리스트 정보 수집 #
                elem = driver.find_element_by_tag_name("body")
                last_height = driver.execute_script("return document.body.scrollHeight")
                while True:
                    scroll_down = 0
                    while scroll_down < 60:
                        elem.send_keys(Keys.PAGE_DOWN)
                        time.sleep(0.2)
                        scroll_down += 1
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                    
                youtube_html = driver.page_source
                youtube_soup = BeautifulSoup(youtube_html, 'html.parser')
                # 영상목록 video_list 변수에 저장 
                video_list = youtube_soup.select('div[class="style-scope ytd-grid-renderer"]')[0]

                video_url = []
                star_id = []
                star_name = []
                video_subs = []
                video_date = []
                video_likes = []
                video_comments = []
                video_views = []
                # 영상 게시일 2021년 01월 01일 이후로 설정 
                date1 = "01,01,2021"
                date1_f = datetime.strptime(date1, "%d,%m,%Y")
                # 게시된 영상이 없을 경우
                if video_list == []:
                    print("[INFO]" "video 없음 ")
                else:
                    j = 0
                    while True:
                        # video_list 에 저장된 동영상의 주소 가져오기 
                        href = video_list.find_all("a", {"class": "yt-simple-endpoint inline-block style-scope ytd-thumbnail"})[
                            j].get('href')
                        newURL = 'https://www.youtube.com/' + str(href)
                        driver.get(newURL)
                        time.sleep(1)

                        # youtube 팝업 끄기
                        try:
                            driver.find_element_by_css_selector("#dismiss-button > a").click()
                        except:
                            pass
                        time.sleep(1)
                        # 댓글 정보 수집할 수 있도록 Scroll down
                        e = driver.find_element_by_tag_name('body')
                        for i in range(5):
                            e.send_keys(Keys.PAGE_DOWN)
                            time.sleep(1)

                        source = driver.page_source
                        soup = BeautifulSoup(source, 'lxml')
                        # 조회수 수집 - 조회수 비공개인 경우 pass
                        try :
                            views = soup.find_all("span", {"class": "view-count style-scope ytd-video-view-count-renderer"})[
                                0].text
                            views = views.replace('조회수', '').replace('회', '').replace(' ', '').replace(',', '').strip()
                        except :
                            views = 0
                        # 좋아요 수 수집 - 좋아요수 비공개인 경우 pass
                        try : 
                            likes = soup.find_all("yt-formatted-string",
                                                  {"class": "style-scope ytd-toggle-button-renderer style-text"})[0].get(
                                'aria-label')
                            likes = likes.replace('좋아요', '').replace('만','').replace('개', '').replace(' ', '').replace(',', '').strip()
                        except :
                            likes = 0
                        # 구독자 수 - 구독자 비공개인 경우 pass 
                        try :
                            subs = soup.find_all("yt-formatted-string", {"class": "style-scope ytd-video-owner-renderer"})[
                                0].get('aria-label')
                            subs = subs.replace('구독자', '').replace('만','0000').replace('명', '').replace('.','').replace(' ', '').replace(',', '').strip()
                        except : 
                            subs = 0
                        # 댓글 수 수집 - 댓글창 없는 경우 pass
                        try : 
                            comments = soup.select('h2[id="count"]')[0].text
                            comments = comments.replace('\n', '').replace('댓글', '').replace('개', '').replace(',', '').strip()
                        except : 
                            comments = 0
                        # 게시일 수집
                        date = \
                        soup.find_all({"div": "info-strings"}, {"class": "style-scope ytd-video-primary-info-renderer"})[0]
                        date = \
                        date.find_all("yt-formatted-string", {"class": "style-scope ytd-video-primary-info-renderer"})[
                            1].text
                        date = date.replace(' ', '').replace('최초공개:','').replace('실시간스트리밍시작일:','').strip()
                        # 설정한 날짜보다 이전의 게시일인 경우 pass
                        if pd.to_datetime(date) < date1_f:
                            break
                        # 다음 순서 인물 크롤링할 것임
                        j += 1
                        # 수집한 데이터 concat
                        df_new = pd.DataFrame({"star_id": [series_id], "star_name": [name],
                                               "views": [views], "likes": [likes], "subs": [subs], "comments": [comments]})
                        empty_frame = pd.concat([empty_frame, df_new], axis=0)
                        print(empty_frame)
                print(name,'수집 완료')
            except:
                pass
        # youtube 링크 없으면 pass
        else:
            pass

empty_frame.to_excel('youtube_수집'+ timelabel +'.xlsx')
