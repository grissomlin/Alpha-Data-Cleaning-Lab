import os
import sqlite3
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.service_account import Credentials
import io

# 導入自定義模組
from market_rules import MarketRuleRouter
from core_engine import AlphaCoreEngine

class AlphaDataPipeline:
    def __init__(self, market_abbr):
        self.market_abbr = market_abbr
        self.db_name = f"{market_abbr.lower()}_stock_warehouse.db"
        self.creds = self._load_credentials()
        self.service = build('drive', 'v3', credentials=self.creds)
        # Google Drive 上的檔案 ID 映射 (請確保 secret 內有這些 ID)
        self.file_id_map = {
            "TW": os.environ.get("TW_DB_ID"),
            "US": os.environ.get("US_DB_ID"),
            "JP": os.environ.get("JP_DB_ID"),
            "HK": os.environ.get("HK_DB_ID"),
            "KR": os.environ.get("KR_DB_ID"),
            "CN": os.environ.get("CN_DB_ID"),
        }

    def _load_credentials(self):
        # 從 GitHub Secrets 讀取服務帳號金鑰
        import json
        info = json.loads(os.environ.get("GDRIVE_SERVICE_ACCOUNT"))
        return Credentials.from_service_account_info(info)

    def download_db(self):
        file_id = self.file_id_map.get(self.market_abbr)
        if not file_id:
            raise ValueError(f"找不到市場 {self.market_abbr} 的 File ID")
            
        request = self.service.files().get_media(fileId=file_id)
        fh = io.FileIO(self.db_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        print(f"📥 {self.db_name} 下載成功")

    def upload_db(self):
        """
        🚀 核心改進：使用 Resumable Upload 處理大檔案上傳 (解決 US SSL 錯誤)
        """
        file_id = self.file_id_map.get(self.market_abbr)
        
        # 使用 MediaFileUpload 並啟用 resumable 功能
        # chunksize 設為 5MB 提高大檔案上傳穩定度
        media = MediaFileUpload(
            self.db_name, 
            mimetype='application/octet-stream',
            resumable=True,
            chunksize=5 * 1024 * 1024 
        )
        
        request = self.service.files().update(
            fileId=file_id,
            media_body=media
        )
        
        print(f"📤 正在上傳 {self.db_name} (支援可續傳模式)...")
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"   > 上傳進度: {int(status.progress() * 100)}%")
        
        print(f"✅ {self.db_name} 更新至雲端成功")

    def run_process(self):
        """
        執行整個精煉流程
        """
        self.download_db()
        
        # 建立資料庫連線
        conn = sqlite3.connect(self.db_name)
        
        try:
            # 1. 獲取市場規則路由
            rules = MarketRuleRouter.get_rules(self.market_abbr)
            
            # 2. 初始化核心引擎 (傳入連線、規則、市場標籤)
            engine = AlphaCoreEngine(conn, rules, self.market_abbr)
            
            # 3. 執行精煉並獲取摘要訊息 (已在 core_engine 內完成 execute)
            summary_msg = engine.execute()
            
            # 關閉連線以解鎖檔案，準備上傳
            conn.close()
            
            # 4. 上傳更新後的資料庫
            self.upload_db()
            
            # 將結果寫入檔案供 GitHub Action 後續讀取 (例如發送 Telegram)
            with open("summary.txt", "w", encoding="utf-8") as f:
                f.write(str(summary_msg))
                
            return summary_msg
            
        except Exception as e:
            if conn: conn.close()
            print(f"❌ 流程中斷: {e}")
            raise e

if __name__ == "__main__":
    # 從環境變數獲取目前矩陣跑的是哪個市場
    target_market = os.environ.get("MARKET_TYPE", "TW")
    pipeline = AlphaDataPipeline(target_market)
    pipeline.run_process()
