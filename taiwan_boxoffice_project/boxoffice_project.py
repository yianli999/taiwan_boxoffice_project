import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import sqlite3
from sklearn.linear_model import LinearRegression
import math
from sklearn.feature_selection import f_regression
import matplotlib.pyplot as plt 

url='https://www.tfai.org.tw/boxOffice/weekly'
headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/120.0.0.0 Safari/537.36'}

res=requests.get(url,headers=headers)
res.encoding='utf8'
html_body=res.text
#以Beautifulsoup找到涵蓋全部href的ul:download-list
soup=BeautifulSoup(html_body,'lxml')
download = soup.find_all('ul','download-list')


download_titles=[]
for i in download:
    download_names=i.find_all('span','title')
    time.sleep(5)
    for n in download_names:
        text=n.get_text().split('全國電影票房')[1].split('-')[0].strip()
        text=text.replace('年','/')
        download_titles.append(text)

download_links=[]
for i in download:
    download_xls = i.find_all('a','xls')
    time.sleep(5)
    for a in download_xls:
        download_links.append('https://www.tfai.org.tw'+a.get('href'))
        
#2020/8/3 169 '20200803.xlsx'
#2018/10/22 262 '20181022.xlsx'     
        
x=0
for i in download_links:
    response = requests.get(i,headers=headers)    
    if response.status_code == 200:
        file_content = response.content
    # 將link內容寫入download_{x}.xlsx
        with open(f'download_{x}.xlsx', 'wb') as file:
            file.write(file_content)
            print('文件載入成功')
            x+=1
    else:
        print('文件載入失敗')     

fn=[f'download_{i}.xlsx' for i in range(316)]        
fn.insert(169, '20200803.xlsx')
fn.insert(262, '20181022.xlsx')
fn.pop(175)
fn.insert(175, '20200629.xlsx')  
fn.pop(223)
fn.insert(223, '20190722.xlsx')


#%%
#讀取download的execel並轉成DataFrame      
df={}
x=0
for i in range(len(download_titles)):
    df[download_titles[i]]=pd.read_excel(fn[i],
                                         engine='openpyxl',
                                         usecols=['中文片名','國別地區','累計銷售金額','銷售金額','累計銷售票數',
                                                  '銷售票數','上映院數','上映日期','申請人'],
                                         parse_dates=['上映日期'],
                                         dtype={0:str,1:str,3:str})
    print(x)
    x+=1
#將object改成int
L=['累計銷售金額','銷售金額','累計銷售票數','銷售票數']
for i in range(len(download_titles)):
    for l in L:
        df[download_titles[i]][l] = df[download_titles[i]][l].astype(str).str.replace(',', '')  # Removing commas if present
        df[download_titles[i]][l] = pd.to_numeric(df[download_titles[i]][l], errors='coerce').fillna(0).astype('int64') #convert the column to 'int64'
#%%

#把df.value 合併成一個 DataFrame
movie_df=pd.concat([i for i in df.values()],axis=0,ignore_index=True)
#建立欄位計算週數
movie_df['週數'] = movie_df.groupby('中文片名')['中文片名'].transform('count')
#建立一個欄位['is_max_sale']  其值為bool值 ，判斷是否為groupby('中文片名') 累計銷售金額最大值
movie_df['is_max_sale'] = movie_df.groupby('中文片名')['累計銷售金額'].transform(lambda x: x == x.max())
#把判斷為False則刪除
movie_df = movie_df[movie_df['is_max_sale'] != False]
#刪除欄位['is_max_sale']
movie_df.drop(columns='is_max_sale', inplace=True)
#將['上映日期'] 改成日期型態
movie_df['上映日期'] = pd.to_datetime(movie_df['上映日期'],errors='coerce')
#找出日期型態有缺失值
time_NAT=movie_df[movie_df['上映日期'].isna()]
#個別找出正確的值
t=[pd.to_datetime('2023-12-29'),pd.to_datetime('2023-11-24'),pd.to_datetime('2023-12-01'),pd.to_datetime('2023-09-15'),pd.to_datetime('NaT'),
   pd.to_datetime('2023-10-27'),pd.to_datetime('2023-09-22'),pd.to_datetime('2023-07-26'),pd.to_datetime('NaT'),pd.to_datetime('2023-09-08'),
   pd.to_datetime('2023-09-08'),pd.to_datetime('2023-12-12'),pd.to_datetime('NaT'),pd.to_datetime('NaT')]

k=[int(i) for i in time_NAT.index]
dic=dict(zip(k,t))
#將正確的值替換掉NaT
for key,value in dic.items():
    movie_df.loc[key, '上映日期'] = value

movie_df.info()

print(movie_df[movie_df['國別地區'].isna()])
print(movie_df[movie_df['申請人'].isna()])

movie_df.loc[416, '國別地區'] = '澳洲'
movie_df.loc[10672, '國別地區'] = '義大利'

movie_df.dropna(inplace=True)
movie_df.info()

#%%
#取得電影名稱及外文名稱
L=['111年電影名稱.csv','110年電影名稱.csv','109年電影名稱.csv','108年電影名稱.csv']
k=['m_name_'+str(i) for i in range(111,107,-1)]

df_name={}
for i in range(4):
    df_name[k[i]]=pd.read_csv(L[i],encoding='utf8')

#結合name_df一個DataFrame
name_df=pd.concat([i for i in df_name.values()],axis=0,ignore_index=True)

# 使用 duplicated 方法檢查 '中文片名' 是否有重複值
name_df['is_duplicate'] = name_df['中文片名'].duplicated()

# 刪除具有重複值的行
name_df=name_df[name_df['is_duplicate']==False]

name_df.drop(columns='is_duplicate',inplace=True)
print('name_df建立完成')

#%%
#建立movie_total 將movie_df加上name_df的['外文名稱']
movie_total=pd.merge(movie_df, name_df,how='left',on='中文片名')

name_NaN=movie_total[movie_total['外文片名'].isna()]
name_NaN.drop(columns='外文片名',inplace=True)
name_value=movie_total[movie_total['外文片名'].notna()]
name_complete=pd.read_csv('補齊缺失外文名.csv')
name_NaN=pd.merge(name_NaN, name_complete, how='left', on='中文片名')
name_NaN.drop(columns='外文片名_x')

name_value_part01=name_value.iloc[:500,:]
name_value_part02=name_value.iloc[500:1000,:]
name_value_part03=name_value.iloc[1000:1500,:]
name_value_part04=name_value.iloc[1500:2000,:]
name_value_part05=name_value.iloc[2000:,:]
#取得OMDB_URL內容
OMDB_URL_01 = 'http://www.omdbapi.com/?apikey=a6d4d22c'
OMDB_URL_02 = 'http://www.omdbapi.com/?apikey=81c00a15'
OMDB_URL_03 = 'http://www.omdbapi.com/?apikey=d874f65a'
OMDB_URL_04 = 'http://www.omdbapi.com/?apikey=d8a0f02f'
OMDB_URL_05 = 'http://www.omdbapi.com/?apikey=571af43c'
OMDB_URL_06 = 'http://www.omdbapi.com/?apikey=a309db95'

#'http://www.omdbapi.com/?apikey=81c00a15'
#'http://www.omdbapi.com/?apikey=d874f65a'
#'http://www.omdbapi.com/?apikey=d8a0f02f'
#'http://www.omdbapi.com/?apikey=571af43c'
#'http://www.omdbapi.com/?apikey=a309db95'
'http://www.omdbapi.com/?apikey=a6d4d22c'
'http://www.omdbapi.com/?apikey=c06a8e7a'#16
def get_data(url):
    data = requests.get(url).json()
    return data if data['Response'] == 'True' else None
#以關鍵字搜尋imdbID
def search_ids_by_keyword(keyword,OMDB_URL):
    query = '+'.join(keyword.split())
    url = OMDB_URL + '&t=' + query
    data = get_data(url)

    if data:
        # 取得第一筆電影 id
        movie_ids=data['imdbID']
    else:
        movie_ids=None
            
    return movie_ids
#imdbID搜尋movie詳細資料
def search_by_id(movie_id,OMDB_URL):
    if movie_id != None:
        url = OMDB_URL + '&i=' + movie_id
        data = get_data(url)
        return data if data else None
m_ids=list()
m_ids_01=list()
m_ids_02=list()
m_ids_03=list()
m_ids_04=list()
m_ids_05=list()
for name in name_value_part01['外文片名']:
    m_ids_01.append(search_ids_by_keyword(name,OMDB_URL_01))
for name in name_value_part02['外文片名']:
    m_ids_02.append(search_ids_by_keyword(name,OMDB_URL_02))
for name in name_value_part03['外文片名']:
    m_ids_03.append(search_ids_by_keyword(name,OMDB_URL_03))
for name in name_value_part04['外文片名']:
    m_ids_04.append(search_ids_by_keyword(name,OMDB_URL_04))
for name in name_value_part05['外文片名']:
    m_ids_05.append(search_ids_by_keyword(name,OMDB_URL_05))

m_ids=m_ids_01+m_ids_02+m_ids_03+m_ids_04+m_ids_05
name_value['ID']=m_ids
#%%
#將movie_total寫入sqlite 
movie_total.drop(columns='外文片名',inplace=True)
conn = sqlite3.connect('Movie_boxoffice.db')

movie_total.to_sql('movie_boxoffice', conn, index=False, if_exists='replace')

conn.close()
name_NaN=name_NaN[['中文片名','外文片名','ID']]
name_value=name_value[['中文片名','外文片名','ID']]
movie_id=pd.concat([name_NaN,name_value],axis=0,ignore_index=True)
movie_id.dropna(inplace=True,ignore_index=True)

#將movie_id寫入sqlite
conn = sqlite3.connect('Movie_boxoffice.db')

movie_id.to_sql('movie_id', conn, index=False, if_exists='replace')

conn.close()
#%%
#以ID取得movie_info
movies_info=list()
movies_info_01 = list()
movies_info_02 = list()
movies_info_03 = list()
movies_info_04 = list()
movies_info_05 = list()
movies_info_06 = list()

for m_id in movie_id["ID"][:500]:
    movies_info_01.append(search_by_id(m_id,OMDB_URL_01))
for m_id in movie_id["ID"][500:1000]:
    movies_info_02.append(search_by_id(m_id,OMDB_URL_02))
for m_id in movie_id["ID"][1000:1500]:
    movies_info_03.append(search_by_id(m_id,OMDB_URL_03))
for m_id in movie_id["ID"][1500:2000]:
    movies_info_04.append(search_by_id(m_id,OMDB_URL_04))
for m_id in movie_id["ID"][2000:2500]:
    movies_info_05.append(search_by_id(m_id,OMDB_URL_05))
for m_id in movie_id["ID"][2500:]:
    movies_info_06.append(search_by_id(m_id,OMDB_URL_06))

movies_info=movies_info_01+movies_info_02+movies_info_03+movies_info_04+movies_info_05+movies_info_06
movies_info=[x for x in movies_info if x is not None]
#%%
#取得movies_info裡的資料
movies_information=pd.DataFrame(columns=['Title','ID','Country','Genre','Awards','Director','Metascore','Tomatos','imdbRating','imdbVotes','Runtime','Poster','Plot'],index=list(range(len(movies_info))))
for i in range(len(movies_info)):
    movies_information.iloc[i,0]=movies_info[i]['Title']
    movies_information.iloc[i,1]=movies_info[i]['imdbID']
    movies_information.iloc[i,2]=movies_info[i]['Country']
    movies_information.iloc[i,3]=movies_info[i]['Genre']
    movies_information.iloc[i,4]=movies_info[i]['Awards']
    movies_information.iloc[i,5]=movies_info[i]['Director']
    movies_information.iloc[i,6]=movies_info[i]['Metascore']
    rotten_tomatoes_rating = next((x['Value'] for x in movies_info[i]['Ratings'] if x['Source'] == 'Rotten Tomatoes'), 'N/A')
    movies_information.iloc[i,7]=rotten_tomatoes_rating.strip('%')
    movies_information.iloc[i,8]=movies_info[i]['imdbRating']
    movies_information.iloc[i,9]=movies_info[i]['imdbVotes']
    movies_information.iloc[i,10]=movies_info[i]['Runtime'].strip('min')
    movies_information.iloc[i,11]=movies_info[i]['Poster']
    movies_information.iloc[i,12]=movies_info[i]['Plot']


movies_information[['Genre1', 'Genre2', 'Genre3','Genre4']] = movies_information['Genre'].str.split(', ', expand=True)
Genre1=[x for x in movies_information['Genre1']]
Genre2=[x for x in movies_information['Genre2']]
Genre3=[x for x in movies_information['Genre3']]
Genre4=[x for x in movies_information['Genre4']]
Genre=[x for x in set(Genre1+Genre2+Genre3+Genre4)]

for i in Genre:
    movies_information[i]=0
movies_information.info()

for i in range(len(Genre)):
    for x in range(2526):
        if Genre[i]==movies_information.iloc[x,13]:
            movies_information.iloc[x,i+17]=1
        elif Genre[i]==movies_information.iloc[x,14]:
            movies_information.iloc[x,i+17]=1
        elif Genre[i]==movies_information.iloc[x,15]:
            movies_information.iloc[x,i+17]=1
        elif Genre[i]==movies_information.iloc[x,16]:
            movies_information.iloc[x,i+17]=1
        else:
            movies_information.iloc[x,i+17]=0

movies_information[['wins','nominations']]=movies_information['Awards'].str.split('&',expand=True)
movies_information['wins'] = movies_information['wins'].where(~movies_information['wins'].str.contains('nomination'))
movies_information['wins'] = movies_information['wins'].str.extract('(\d+)').astype(float)
movies_information['wins'].fillna(0,inplace=True)
movies_information.drop(columns=['nominations'],inplace=True)

#%%
#將DataFrame內文字改成數值
movies_information['Metascore']=pd.to_numeric(movies_information['Metascore'],errors='coerce').fillna(0).astype(float)
movies_information['Metascore']=movies_information['Metascore']/10
#movies_information['Metascore']=movies_information['Metascore'].replace(0,None)

movies_information['Tomatos']=pd.to_numeric(movies_information['Tomatos'],errors='coerce').fillna(0).astype(float)
movies_information['Tomatos']=movies_information['Tomatos']/10
#movies_information['Tomatos']=movies_information['Tomatos'].replace(0,None)

movies_information['imdbRating']=pd.to_numeric(movies_information['imdbRating'],errors='coerce').fillna(0).astype(float)
#movies_information['imdbRating']=movies_information['imdbRating'].replace(0,None)

movies_information['imdbVotes'] = movies_information['imdbVotes'].astype(str).str.replace(',', '')  # Removing commas if present
movies_information['imdbVotes'] = pd.to_numeric(movies_information['imdbVotes'], errors='coerce').fillna(0).astype('int64')
#movies_information['imdbVotes']=movies_information['imdbVotes'].replace(0,None)

movies_information['Runtime']=pd.to_numeric(movies_information['Runtime'],errors='coerce').fillna(0).astype('int64')
#movies_information['Runtime']=movies_information['Runtime'].replace(0,None)

movies_information.drop(columns=[None,'N/A'],inplace=True)

movies_information.info()


#寫入資料庫
conn = sqlite3.connect('Movie_boxoffice.db')

movies_information.to_sql('movie_information', conn, index=False, if_exists='replace')

conn.close()

#%%
#建立國別類別變數
country= pd.DataFrame(columns = ['US','JP','TW','KOR','FR','Other_country'], index=list(range(4555)),dtype=str)

# Set values based on conditions
country['US'] = np.where(movie_total['國別地區'] == '美國', 1, 0)
country['JP'] = np.where(movie_total['國別地區'] == '日本', 1, 0)
country['TW'] = np.where(movie_total['國別地區'] == '中華民國', 1, 0)
country['KOR'] = np.where(movie_total['國別地區'] == '韓國', 1, 0)
country['KOR'] = np.where(movie_total['國別地區'] == '南韓', 1, 0)
country['FR'] = np.where(movie_total['國別地區'] == '法國', 1, 0)

# Set 'Other_country' column using negation of previous conditions
condition = (
    (movie_total['國別地區'] != '美國') &
    (movie_total['國別地區'] != '日本') &
    (movie_total['國別地區'] != '中華民國') &
    (movie_total['國別地區'] != '韓國') &
    (movie_total['國別地區'] != '南韓') &
    (movie_total['國別地區'] != '法國')
)
country['Other_country'] = np.where(condition, 1, 0)
movies_country=pd.concat([movie_total['中文片名'],country],axis=1)

movies_lm=pd.merge(movie_total,movies_country,how='left', on='中文片名')
movies_lm['is_duplicate'] = movies_lm['中文片名'].duplicated()
movies_lm=movies_lm[movies_lm['is_duplicate']==False]

movies_lm.drop(columns='is_duplicate',inplace=True)

movies_lm=pd.merge(movies_lm,movie_id,how='left', on='中文片名')
movies_lm.dropna(inplace=True)

movies_lm['is_duplicate'] = movies_lm['ID'].duplicated()
movies_lm=movies_lm[movies_lm['is_duplicate']==False]
movies_lm.drop(columns='is_duplicate',inplace=True)

movies_lm=pd.merge(movies_lm,movies_information,how='left', on='ID')
movies_lm.drop(1,inplace=True)
movies_lm.reset_index(drop=True,inplace=True)
movies_lm.info()
movies_lm_sorted_desc = movies_lm.sort_values(by='上映日期', ascending=False)
movies_lm_sorted_desc.reset_index(drop=True,inplace=True)
movies_lm_sorted_desc = movies_lm_sorted_desc.drop(movies_lm_sorted_desc.tail(106).index)
movies_lm_sorted_desc.to_csv('movies_lm_sorted_desc.csv',index=None,encoding='utf-8-sig')
movies_lm_sorted_desc.drop(476,inplace=True)

lm=LinearRegression()

X=movies_lm_sorted_desc[['上映院數','週數','US','JP','TW','KOR','FR','Other_country','imdbRating','imdbVotes','Drama','Romance','Biography','Family','Talk-Show','War','Adult','Horror','Film-Noir','Sport','Documentary','Music','Sci-Fi','Western','Short','News','History','Fantasy','Mystery','Thriller','Comedy','Action','Musical','Adventure','Crime','Animation','wins']]
y=np.array([math.log(x) for x in movies_lm_sorted_desc['累計銷售票數']])

lm_m=lm.fit(X, y)

print('迴歸係數:',lm_m.coef_)
print('截距:',lm_m.intercept_)
print('R-squared:',lm_m.score(X, y))

f_stats, p_values = f_regression(X, y)
for i in range(len(f_stats)):
    print(f"{X.columns[i]}: F統計量={f_stats[i]}, p值={p_values[i]}")

