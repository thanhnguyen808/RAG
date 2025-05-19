import requests
from bs4 import BeautifulSoup
import re
import os
import time
import logging

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_2.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clean_text(text):
    """Làm sạch văn bản: loại bỏ khoảng trắng thừa và ký tự đặc biệt"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)  # Thay nhiều khoảng trắng bằng một
    text = re.sub(r'\<.*?\>', '', text)  # Xóa thẻ HTML còn sót
    return text.strip()

def extract_table(table):
    """Trích xuất dữ liệu từ bảng HTML và định dạng thành văn bản"""
    if not table:
        return ""
    table_data = []
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all(['td', 'th'])
        row_text = [clean_text(cell.text) for cell in cells]
        table_data.append(" | ".join(row_text))
    return "\n".join(table_data)

def scrape_website(url, file):
    """
    Hàm crawl dữ liệu từ website, chỉ trích xuất bảng <table id="myTable">, lưu vào file txt.
    Args:
        url (str): URL của trang web
        file (file object): File txt để ghi dữ liệu
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        }
        logger.info(f"Đang kết nối tới {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'utf-8'  # Đảm bảo mã hóa tiếng Việt
        if response.status_code == 200:
            logger.info(f"Truy cập thành công (Status Code: {response.status_code})")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ghi phân tách và thông tin URL
            file.write(f"\n{'='*80}\n")
            file.write(f"URL: {url}\n")
            
            # Trích xuất bảng <table id="myTable">
            file.write("===== BẢNG DỮ LIỆU =====\n")
            table = soup.find('table', id='myTable')
            if table:
                table_text = extract_table(table)
                if table_text:
                    file.write(f"{table_text}\n\n")
                else:
                    file.write("Bảng trống hoặc không thể trích xuất.\n\n")
            else:
                logger.warning("Không tìm thấy bảng với id='myTable'")
                file.write("Không tìm thấy bảng dữ liệu.\n\n")
            
            logger.info(f"Dữ liệu từ {url} đã được lưu.")
        else:
            logger.error(f"Không thể truy cập. Status Code: {response.status_code}")
            file.write(f"LỖI: Không thể truy cập {url} (Status Code: {response.status_code})\n")
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi trong quá trình gửi yêu cầu: {e}")
        file.write(f"LỖI: Không thể kết nối tới {url} ({str(e)})\n")
    except Exception as e:
        logger.error(f"Lỗi bất ngờ: {e}", exc_info=True)
        file.write(f"LỖI: Đã xảy ra lỗi khi xử lý {url} ({str(e)})\n")

if __name__ == "__main__":
    try:
        logger.info("Bắt đầu crawl dữ liệu từ danh sách URL")
        # Đọc danh sách URL từ file link_2.txt
        with open('link_2.txt', 'r', encoding='utf-8') as f:
            links = [line.strip() for line in f if line.strip()]
        
        # Mở file để ghi dữ liệu
        with open('data_output_2.txt', 'w', encoding='utf-8') as file:
            for idx, url in enumerate(links, 1):
                logger.info(f"Đang crawl trang {idx}/{len(links)}: {url}")
                scrape_website(url, file)
                time.sleep(2)  # Nghỉ 2 giây giữa các yêu cầu để tránh bị chặn
        logger.info("Hoàn tất crawl dữ liệu")
    except FileNotFoundError:
        logger.error("Không tìm thấy file link_2.txt")
        print("Vui lòng tạo file 'link_2.txt' chứa danh sách URL cần crawl.")
    except Exception as e:
        logger.error(f"Lỗi chính: {e}", exc_info=True)