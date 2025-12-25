# -*- coding: utf-8 -*-
import os, json, argparse, sqlite3
import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io

# 載入自定義模組
from core_engine import AlphaCoreEngine
from market_rules import MarketRuleRouter

load_dotenv()

class AlphaPipeline:
    def __init__(self, db_name):
        self.db_name = f"{db_name}.db"
        self.parent_id = os.getenv("PARENT_FOLDER_ID")
        self.service = self._get_drive_service()
        self.market_abbr = db_name.split('_')[0].upper()

    def _get_drive_service(self):
        info = json.loads(os.getenv("GDRIVE_SERVICE_ACCOUNT"))
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive'])
        return build('drive', 'v3', credentials=creds)

    def download(self):
        query = f"name = '{self.db_name}' and '{self.parent_id}' in parents"
        res = self.service.files().list(q=query).execute()
        fid = res['files'][0]['id']
        request = self.service.files().get_media(fileId=fid)
        fh = io.FileIO(self.db_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        print(f"📥 {self.db_name} 下載成功")
        return fid

    def upload(self, file_id):
        media = MediaFileUpload(self.db_name, mimetype='application/x-sqlite3')
        self.service.files().update(fileId=file_id, media_body=media).execute()
        print(f"📤 {self.db_name} 更新至雲端成功")

    def run_process(self):
        # 1. 建立連線
        conn = sqlite3.connect(self.db_name)
        
        # 2. 獲取該市場規則
        rules = MarketRuleRouter.get_rules(self.market_abbr)
        
        # 3. 呼叫核心引擎進行「清洗 + 暴漲分析 + 漲停標記」
        engine = AlphaCoreEngine(conn, rules, self.market_abbr)
        summary = engine.execute()
        
        conn.close()
        return summary

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="例如: tw_stock_warehouse")
    args = parser.parse_args()

    pipeline = AlphaPipeline(args.db)
    f_id = pipeline.download()
    summary_msg = pipeline.run_process()
    pipeline.upload(f_id)
    
    # 將摘要寫入暫存檔，供最後集體報告使用
    with open(f"summary_{args.db}.txt", "w") as f:
        f.write(summary_msg)