# -*- coding: utf-8 -*-
import os
import requests
from dotenv import load_dotenv

load_dotenv()

def send_final_summary():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    # 搜尋所有機器產出的 summary_*.txt
    summary_files = [f for f in os.listdir('.') if f.startswith('summary_') and f.endswith('.txt')]
    
    if not summary_files:
        print("沒有偵測到任何處理摘要。")
        return

    report_content = "📊 **Alpha-Data-Refinery-6 執行報告**\n"
    report_content += "--------------------------------------\n"
    
    for file in sorted(summary_files):
        with open(file, 'r') as f:
            report_content += f.read() + "\n"
            
    report_content += "--------------------------------------\n"
    report_content += "✅ 六國歷史數據 (2020-2025) 精煉完成。"

    # 發送至 Telegram
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": report_content, "parse_mode": "Markdown"}
    
    try:
        requests.post(url, json=payload, timeout=10)
        print("✨ 總結報告已成功發送至 Telegram")
    except Exception as e:
        print(f"❌ Telegram 發送失敗: {e}")

if __name__ == "__main__":
    send_final_summary()