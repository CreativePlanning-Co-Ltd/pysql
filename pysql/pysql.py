from pydantic import BaseModel
from typing import List,Literal,TypeVar
import os
import chardet
import pandas as pd
import sqlite3
import subprocess
from IPython.display import display


DataFrame = TypeVar("DataFrame") #データフレーム型
class Data_Source(BaseModel):
    data_source_name:str = None #データソース名称
    enc_type:str = None #エンコーディングの方式
    data_source:DataFrame = None
    modified_data:DataFrame = None
    table_name:str= 'table_xxx' #テーブル名（仮)
    file_name:str = None

    #データソース一覧表示
    def SELECT_ALL(self):
        display(self.exec_sql(self.getDefaultQuery()))
    
    #SQLクエリのテンプレート表示
    def getDefaultQuery(self) -> str:
        header = list(self.data_source.columns)
        col =  f''',\n    '''.join(header)
        default_query = f'''SELECT \n    {col}\nFROM\n    {self.table_name}\nWHERE\n    1 = 1'''
        return default_query
    #データソースにSQLを発行
    def exec_sql(self,query:str):
        conn = sqlite3.connect(':memory:')
        self.data_source.to_sql(self.table_name,conn,if_exists='replace',index=False)
        result = pd.read_sql(f'''{query}''',conn)
        return result
    
    #文字コードの判定
    def getEncType(self):
        with open(self.file_name,'rb') as f:
            c = f.read()
            encode_type = chardet.detect(c)['encoding']
            if encode_type is None:
                encode_type = subprocess.run(['nkf',"-guess",self.file_name],capture_output=True).stdout.decode('utf-8').replace("¥n","")
            if encode_type == "MacRoman":
                encode_type = 'Shift-jis'
            return encode_type

    def find(self,txt:str,EXACT_MATCH=0):
        if EXACT_MATCH == 0:
            return self.data_source[self.data_source.apply(lambda row: any(txt.upper() in str(cell).upper() for cell in row), axis=1)]
        elif EXACT_MATCH == 1:
            return self.data_source[self.data_source.apply(lambda row: any(txt in str(cell) for cell in row), axis=1)]
        else:
            raise ValueError("Invaild EXACT_MATCH_FLG has been detected")
    
#ローカルのCSVファイル
class PyTable_LocalCsv(Data_Source):
    def __init__(self,path): #インスタンス作成時の起動処理
        super().__init__(path=path)
        self.file_name = os.path.basename(path)

        #文字コードの判定・取得
        self.enc_type = self.getEncType()

        #CSVデータをデータフレーム化
        self.data_source = pd.read_csv(path,encoding=self.enc_type)

#Webから取得したCSV
class PyTable_WebCsv(Data_Source):
    def __init__(self,uri): #インスタンス作成時の起動処理
        super().__init__(uri=uri)
        try:
            before_file = os.listdir() # ls -aコマンド
            os.system(f''' curl -OL "{uri}"''')
            after_file = os.listdir() # ls -aコマンド
            self.file_name = list(set(after_file) -set(before_file))[0]
        
            #文字コードの判定・取得
            self.enc_type = self.getEncType()

            #CSVをデータフレーム化
            self.data_source = pd.read_csv(uri,encoding=self.enc_type)

        #ダウンロードしたファイルを削除
        finally:
            if os.path.exists("./"+self.file_name):
                os.remove("./"+self.file_name)

#一時テーブル
class PyWorkTable(Data_Source):
    def __init__(self,df):#インスタンス作成時の起動処理
        super().__init__(df=df)
        self.data_source = df
