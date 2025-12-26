import os
import sqlite3
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.service_account import Credentials
import io
import json

# 導入自定義模組
from market_rules import MarketRuleRouter
from core_engine import AlphaCoreEngine

class AlphaDataPipeline:
    def __init__(self, market_abbr):
        self.market_abbr = market_abbr.upper()
        # 自動生成的檔名：例如 tw_stock_warehouse.db
        self.db_name = f"{self.market_abbr.lower()}_stock_warehouse.db"
        self.creds = self._load_credentials()
        self.service = build('drive', 'v3', credentials=self.creds)

    def _load_credentials(self):
        creds_json = os.environ.get("GDRIVE_SERVICE_ACCOUNT")
        if not creds_json:
            raise ValueError("❌ 找不到環境變數: GDRIVE_SERVICE_ACCOUNT")
        return Credentials.from_service_account_info(json.loads(creds_json))

    def find_file_id_by_name(self, filename):
        """
        🚀 恢復自動化：透過檔名在 Google Drive 搜尋檔案 ID
        """
        query = f"name = '{filename}' and trashed = false"
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if not files:
            raise ValueError(f"❌ 在雲端找不到檔案: {filename}")
        return files[0]['id']

    def download_db(self):
        # 自動找 ID
        file_id = self.find_file_id_by_name(self.db_name)
        
        print(f"📥 偵測到雲端檔案 ID: {file_id}，開始下載...")
        request = self.service.files().get_media(fileId=file_id)
        fh = io.FileIO(self.db_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        print(f"✅ {self.db_name} 下載成功")

    def upload_db(self):
        # 自動找 ID
        file_id = self.find_file_id_by_name(self.db_name)
        
        # 🚀 保留解決美國大檔案的 Resumable 技術
        media = MediaFileUpload(self.db_name, mimetype='application/octet-stream', resumable=True)
        request = self.service.files().update(fileId=file_id, media_body=media)
        
        print(f"📤 正在同步回雲端 (可續傳模式)...")
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"   > 進度: {int(status.progress() * 100)}%")
        print(f"✅ {self.market_abbr} 雲端同步成功")

    def run_process(self):
        self.download_db()
        conn = sqlite3.connect(self.db_name)
        try:
            rules = MarketRuleRouter.get_rules(self.market_abbr)
            engine = AlphaCoreEngine(conn, rules, self.market_abbr)
            summary_msg = engine.execute()
            conn.close()
            
            self.upload_db()
            
            # 生成摘要供報告使用
            summary_file = f"summary_{self.market_abbr.lower()}_stock_warehouse.txt"
            with open(summary_file, "w", encoding="utf-8") as f:
                f.write(str(summary_msg))
            return summary_msg
        except Exception as e:
            if conn: conn.close()
            raise e

if __name__ == "__main__":
    target_market = os.environ.get("MARKET_TYPE")
    if not target_market:
        # 如果沒有設定變數，嘗試從 matrix 指令抓取（這對應你的 YAML 改動）
        print("❌ 錯誤：未設定 MARKET_TYPE")
        exit(1)
    
    pipeline = AlphaDataPipeline(target_market)
    pipeline.run_process()
