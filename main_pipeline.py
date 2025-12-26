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
        # 符合 YAML 規範的檔名格式
        self.db_name = f"{self.market_abbr.lower()}_stock_warehouse.db"
        self.creds = self._load_credentials()
        self.service = build('drive', 'v3', credentials=self.creds)
        
        self.file_id_map = {
            "TW": os.environ.get("TW_DB_ID"),
            "US": os.environ.get("US_DB_ID"),
            "JP": os.environ.get("JP_DB_ID"),
            "HK": os.environ.get("HK_DB_ID"),
            "KR": os.environ.get("KR_DB_ID"),
            "CN": os.environ.get("CN_DB_ID"),
        }

    def _load_credentials(self):
        creds_json = os.environ.get("GDRIVE_SERVICE_ACCOUNT")
        if not creds_json:
            raise ValueError("❌ 找不到環境變數: GDRIVE_SERVICE_ACCOUNT")
        return Credentials.from_service_account_info(json.loads(creds_json))

    def download_db(self):
        file_id = self.file_id_map.get(self.market_abbr)
        if not file_id:
            raise ValueError(f"❌ 找不到市場 {self.market_abbr} 的 File ID。請檢查 Secrets 設定。")
            
        print(f"📥 正在下載 {self.market_abbr} 資料庫...")
        request = self.service.files().get_media(fileId=file_id)
        fh = io.FileIO(self.db_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        print(f"✅ {self.db_name} 下載完成")

    def upload_db(self):
        file_id = self.file_id_map.get(self.market_abbr)
        media = MediaFileUpload(self.db_name, mimetype='application/octet-stream', resumable=True)
        request = self.service.files().update(fileId=file_id, media_body=media)
        
        print(f"📤 正在上傳 {self.market_abbr} (Resumable)...")
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
            
            # 💡 重要：產出符合 YAML Artifact 規範的檔名
            summary_file = f"summary_{self.market_abbr.lower()}_stock_warehouse.txt"
            with open(summary_file, "w", encoding="utf-8") as f:
                f.write(str(summary_msg))
            print(f"📄 摘要已存至 {summary_file}")
            
            return summary_msg
        except Exception as e:
            if conn: conn.close()
            raise e

if __name__ == "__main__":
    target_market = os.environ.get("MARKET_TYPE")
    if not target_market:
        print("❌ 錯誤：未設定 MARKET_TYPE")
        exit(1)
    
    pipeline = AlphaDataPipeline(target_market)
    pipeline.run_process()
