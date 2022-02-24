album_cd = pd.read_excel("result/album_craw_minus2022-02-08.xlsx")
album_mi = pd.read_excel("result/album_craw2022-01-28.xlsx")
# concat 2 files
album_tot = pd.concat([album_cd,album_mi])
album_tot.to_excel("/Users/mycelebs_104/Desktop/스타인기도/result/album_tot.xlsx")

# connect star_ko_data
conn, engine = db_connection()
cursor = conn.cursor(pymysql.cursors.DictCursor)
qry = "SELECT * FROM star_ko_data"
ex = pd.read_sql(qry, conn)
star_name = ex['cd_name']
star_id = ex['series_id']


empty_frame = pd.DataFrame(columns=('star_id', 'star_name', 'cnt'))
album_tot_new = album_tot.groupby(["star_id","star_name"]).size().reset_index(name='cnt')
star_cnt = album_tot_new['cnt']

for i, row in (album_tot_new.iterrows(), total=album_tot_new.shape[0])
df_new = pd.DataFrame({"series_id":[star_id],
                       "cnt":[star_cnt],
                       "name":[star_name]
    
})


# 확인
album_tot.head()

album_tot_new.shape[0]

album_tot_new.to_excel('result/album_cnt.xlsx')
