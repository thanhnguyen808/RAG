import requests
import pdfplumber
import re
import os
import time
import logging
from io import BytesIO
import ssl

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pdf_scraper_filtered.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clean_text(text):
    """Làm sạch văn bản: loại bỏ khoảng trắng thừa và ký tự đặc biệt"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)  # Thay nhiều khoảng trắng bằng một
    text = text.strip()
    return text

def should_exclude_content(text):
    """Kiểm tra xem nội dung có chứa từ khóa cần loại bỏ không"""
    exclude_keywords = ['Thông tin liên quan']  # Có thể thêm các từ khóa khác
    return any(keyword.lower() in text.lower() for keyword in exclude_keywords)

def extract_pdf_content(pdf_url):
    """
    Tải PDF từ URL và trích xuất văn bản, loại bỏ nội dung liên quan đến 'Thông tin liên quan'.
    Args:
        pdf_url (str): URL của file PDF
    Returns:
        str: Văn bản trích xuất từ PDF
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }
        logger.info(f"Đang tải PDF từ {pdf_url}")
        # Bỏ qua kiểm tra SSL (cảnh báo: không an toàn cho production)
        logger.warning("Đã bỏ qua kiểm tra SSL (verify=False) cho URL này. Điều này không an toàn trong môi trường sản xuất.")
        response = requests.get(pdf_url, headers=headers, timeout=30, verify=False)
        
        if response.status_code == 200:
            logger.info(f"Tải PDF thành công (Status Code: {response.status_code})")
            pdf_content = ""
            
            # Sử dụng BytesIO để đọc PDF từ bộ nhớ
            with pdfplumber.open(BytesIO(response.content)) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    # Trích xuất văn bản
                    text = page.extract_text()
                    if text:
                        cleaned_text = clean_text(text)
                        if cleaned_text and not should_exclude_content(cleaned_text):
                            pdf_content += f"Trang {page_num}:\n{cleaned_text}\n\n"
                        else:
                            logger.info(f"Bỏ qua nội dung trang {page_num} do chứa từ khóa không mong muốn")
                    
                    # Trích xuất bảng (nếu có)
                    tables = page.extract_tables()
                    if tables:
                        for table_num, table in enumerate(tables, 1):
                            table_content = []
                            for row in table:
                                cleaned_row = [clean_text(cell) if cell else "" for cell in row]
                                row_text = " | ".join(cleaned_row)
                                if row_text and not should_exclude_content(row_text):
                                    table_content.append(row_text)
                                else:
                                    logger.info(f"Bỏ qua dòng bảng {table_num} (Trang {page_num}) do chứa từ khóa không mong muốn")
                            if table_content:
                                pdf_content += f"Bảng {table_num} (Trang {page_num}):\n" + "\n".join(table_content) + "\n\n"
            
            return pdf_content if pdf_content else "Không trích xuất được nội dung từ PDF."
        else:
            logger.error(f"Không thể tải PDF. Status Code: {response.status_code}")
            return f"LỖI: Không thể tải PDF từ {pdf_url} (Status Code: {response.status_code})"
    except requests.exceptions.SSLError as e:
        logger.error(f"Lỗi SSL khi tải PDF: {e}")
        return f"LỖI: Lỗi SSL khi kết nối tới {pdf_url} ({str(e)})"
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi trong quá trình gửi yêu cầu: {e}")
        return f"LỖI: Không thể kết nối tới {pdf_url} ({str(e)})"
    except Exception as e:
        logger.error(f"Lỗi khi xử lý PDF: {e}", exc_info=True)
        return f"LỖI: Đã xảy ra lỗi khi xử lý {pdf_url} ({str(e)})"

def scrape_pdf_links(pdf_links_file, output_file):
    """
    Hàm crawl dữ liệu từ danh sách các link PDF và lưu vào file txt.
    Args:
        pdf_links_file (str): File chứa danh sách URL PDF
        output_file (str): File txt để lưu dữ liệu
    """
    try:
        logger.info("Bắt đầu crawl dữ liệu từ danh sách PDF")
        # Đọc danh sách URL từ file
        with open(pdf_links_file, 'r', encoding='utf-8') as f:
            links = [line.strip() for line in f if line.strip()]
        
        # Mở file để ghi dữ liệu
        with open(output_file, 'w', encoding='utf-8') as file:
            for idx, url in enumerate(links, 1):
                logger.info(f"Đang crawl PDF {idx}/{len(links)}: {url}")
                file.write(f"\n{'='*80}\n")
                file.write(f"PDF URL: {url}\n")
                file.write("===== NỘI DUNG PDF =====\n")
                
                # Trích xuất nội dung PDF
                content = extract_pdf_content(url)
                file.write(content + "\n")
                
                time.sleep(2)  # Nghỉ 2 giây giữa các yêu cầu để tránh bị chặn
        logger.info("Hoàn tất crawl dữ liệu PDF")
    except FileNotFoundError:
        logger.error(f"Không tìm thấy file {pdf_links_file}")
        print(f"Vui lòng tạo file '{pdf_links_file}' chứa danh sách URL PDF cần crawl.")
    except Exception as e:
        logger.error(f"Lỗi chính: {e}", exc_info=True)

if __name__ == "__main__":
    scrape_pdf_links('pdf_links.txt', 'pdf_data_output.txt')