### 전처리 과정 ###
# 1. maimovie_kr.movie_data_update 테이블과 star_ko.star_ko_data 테이블 매핑작업
# 2. 
# 3. 
# 4.
# 5.
# 6.


def db_connection(host_name='ds'):
    host_url = ""
    user_nm = ""
    passwd = ""
    port_num = 
    db_name = "star_ko"
    conn = pymysql.connect(host=host_url, user=user_nm, passwd=passwd, port = port_num, charset='utf8',
                           db=db_name, cursorclass=pymysql.cursors.DictCursor)
    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    return conn, engine
  
conn, engine = db_connection()
cursor = conn.cursor(pymysql.cursors.DictCursor)
qry = "SELECT cd_name, series_id, naver_movie_people_id FROM star_ko_data""
ex = pd.read_sql(qry, conn)
conn.close()



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
# movie_data 에서 naver_cast_code 가져오기
qry = "SELECT * FROM movie_data_update where md_date >= '2021-01-01' and naver_cast_code IS NOT NULL"
ac = pd.read_sql(qry, conn)
conn.close()


naver_cd = ac['naver_cast_code']
df = pd.DataFrame(columns= ('series_id','count'))
for i, row in tqdm(ac.iterrows(), total=ac.shape[0]):
    cd = naver_cd[i].split(',')
    cd = pd.DataFrame(cd, columns = ['series_id'])
    cd = cd[cd.series_id != '-']
    df_main = pd.concat([df,cd], axis = 0)
    df = df_main

    
# 매핑 전 공백제거    
df['series_id']=df['series_id'].str.strip()    
# series_id 별 개수 세기
df_naver = df['series_id'].value_counts()
df_naver = pd.DataFrame(df_naver)
df_naver.reset_index(inplace=True)
df_naver.rename(columns={'index': 'n_actor_moviecode',
                 'series_id' : 'cnt'}, inplace=True)
# NUll값 제거
df_naver['naver_movie_people_id']=df_naver['naver_movie_people_id'].dropna()


ex['naver_movie_people_id']=ex['naver_movie_people_id'].dropna()
ex['naver_movie_people_id']=ex['naver_movie_people_id'].fillna(0).astype(int)
ex['naver_movie_people_id']=ex['naver_movie_people_id'].replace(' ','')
# df_naver 와 ex 데이터프레임 merge
mg = ex.merge(df_naver, on='naver_movie_people_id', how='left')
mg.to_excel('result/영화카운트.xlsx')

