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
        🚀 透過檔名在 Google Drive 搜尋檔案 ID
        """
        query = f"name = '{filename}' and trashed = false"
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if not files:
            raise ValueError(f"❌ 在雲端找不到檔案: {filename}")
        return files[0]['id']

    def download_db(self):
        file_id = self.find_file_id_by_name(self.db_name)
        print(f"📥 偵測到雲端檔案 ID: {file_id}，開始下載...")
        request = self.service.files().get_media(fileId=file_id)
        fh = io.FileIO(self.db_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        print(f"✅ {self.db_name} 下載成功")

    def _ensure_schema_upgraded(self, conn):
        """
        🚀 確保資料庫 Schema 包含炸板分析所需的欄位 (Ret_High)
        """
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(cleaned_daily_base)")
        columns = [column[1] for column in cursor.fetchall()]
        
        # 如果沒有 Ret_High 欄位，則新增
        if 'Ret_High' not in columns:
            print(f"🛠️  正在為 {self.market_abbr} 資料庫新增 Ret_High 欄位...")
            try:
                # SQLite 增加欄位語法
                cursor.execute("ALTER TABLE cleaned_daily_base ADD COLUMN Ret_High REAL")
                conn.commit()
                print("✅ Ret_High 欄位新增成功")
            except Exception as e:
                print(f"⚠️ 欄位新增異常 (可能已存在或表格鎖定): {e}")

    def upload_db(self):
        file_id = self.find_file_id_by_name(self.db_name)
        # 🚀 使用 Resumable 技術處理大檔案上傳
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
        # 1. 下載
        self.download_db()
        
        conn = sqlite3.connect(self.db_name)
        try:
            # 2. 自動升級資料庫結構
            self._ensure_schema_upgraded(conn)

            # 3. 執行核心運算引擎 (計算指標並填入 Ret_High)
            rules = MarketRuleRouter.get_rules(self.market_abbr)
            engine = AlphaCoreEngine(conn, rules, self.market_abbr)
            summary_msg = engine.execute()
            
            # 確保寫入並關閉連線，避免上傳時檔案被佔用
            conn.close()
            
            # 4. 上傳回雲端
            self.upload_db()
            
            # 5. 生成報告摘要
            summary_file = f"summary_{self.market_abbr.lower()}_stock_warehouse.txt"
            with open(summary_file, "w", encoding="utf-8") as f:
                f.write(str(summary_msg))
            
            return summary_msg

        except Exception as e:
            if conn:
                conn.close()
            print(f"❌ 流程執行失敗: {e}")
            raise e

if __name__ == "__main__":
    target_market = os.environ.get("MARKET_TYPE")
    if not target_market:
        print("❌ 錯誤：未設定 MARKET_TYPE 環境變數")
        exit(1)
    
    pipeline = AlphaDataPipeline(target_market)
    pipeline.run_process()
