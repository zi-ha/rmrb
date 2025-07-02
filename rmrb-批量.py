import requests
from bs4 import BeautifulSoup
import os
import time
from PyPDF2 import PdfMerger
import pandas as pd
from urllib.parse import urljoin

# 设置请求头，模拟浏览器访问
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0'
}

# 创建必要的目录
os.makedirs('download', exist_ok=True)
os.makedirs('merged', exist_ok=True)

def get_pdf_urls(date_str):
    """获取指定日期人民日报所有版面的PDF下载链接"""
    base_url = f"http://paper.people.com.cn/rmrb/pc/layout/{date_str[:4]}{date_str[5:7]}/{date_str[8:10]}/"
    index_url = urljoin(base_url, "node_01.html")
    
    try:
        response = requests.get(index_url, headers=headers)
        response.raise_for_status()  # 检查请求是否成功
        soup = BeautifulSoup(response.content, 'lxml')
        
        # 获取所有版面链接
        pages = soup.find_all(class_='swiper-slide')
        page_count = len(pages)
        print(f"{date_str} 找到 {page_count} 个版面")
        
        pdf_urls = []
        for i in range(1, page_count + 1):
            page_url = urljoin(base_url, f"node_{i:02d}.html")
            page_response = requests.get(page_url, headers=headers)
            page_response.raise_for_status()
            
            page_soup = BeautifulSoup(page_response.content, 'lxml')
            pdf_link = page_soup.find(class_='right btn').a['href']
            
            # 关键修改：正确处理相对路径，避免rmrb/pc重复
            # 1. 移除相对路径中的多余部分
            if pdf_link.startswith("../../../"):
                pdf_link = pdf_link[9:]  # 移除../../../
                
            # 2. 构建正确的基础URL
            base_pdf_url = "https://paper.people.com.cn/rmrb/pc/"
            
            # 3. 拼接最终URL
            pdf_url = urljoin(base_pdf_url, pdf_link)
            
            pdf_urls.append(pdf_url)
            time.sleep(0.5)  # 控制爬取频率，避免被封IP
        
        return pdf_urls
    
    except Exception as e:
        print(f"获取{date_str}版面链接时出错: {e}")
        return []

def download_and_merge_pdfs(date_str):
    """下载指定日期的所有PDF并合并为一个文件"""
    pdf_urls = get_pdf_urls(date_str)
    if not pdf_urls:
        print(f"{date_str} 没有找到PDF链接，跳过")
        return
    
    # 记录下载信息
    with open('download_log.txt', 'a', encoding='utf-8') as log_file:
        log_file.write(f"{date_str}:\n")
        for url in pdf_urls:
            log_file.write(f"{url}\n")
    
    # 准备合并器
    merger = PdfMerger()
    success_files = []
    
    # 下载并添加到合并器
    for i, url in enumerate(pdf_urls, 1):
        filename = f"download/rmrb{date_str.replace('-', '')}{i:02d}.pdf"
        
        # 检查文件是否已存在
        if os.path.exists(filename):
            print(f"{filename} 已存在，跳过下载")
        else:
            try:
                print(f"正在下载 {url} 到 {filename}")
                response = requests.get(url, headers=headers, stream=True)
                response.raise_for_status()
                
                with open(filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                print(f"{filename} 下载完成")
                time.sleep(0.5)  # 避免请求过于频繁
            except Exception as e:
                print(f"下载 {url} 时出错: {e}")
                continue
        
        # 尝试添加到合并器
        try:
            merger.append(filename)
            success_files.append(filename)
        except Exception as e:
            print(f"无法合并 {filename}: {e}")
    
    # 合并所有成功下载的PDF
    if success_files:
        merged_filename = f"merged/rmrb{date_str.replace('-', '')}.pdf"
        try:
            merger.write(merged_filename)
            merger.close()
            print(f"已合并 {len(success_files)} 个文件到 {merged_filename}")
            
            # 新增：合并成功后删除源文件
            for file in success_files:
                try:
                    os.remove(file)
                    print(f"已删除源文件: {file}")
                except Exception as e:
                    print(f"删除文件 {file} 失败: {e}")
                    
        except Exception as e:
            print(f"合并PDF时出错: {e}")
    else:
        print(f"{date_str} 没有成功下载任何PDF文件")

if __name__ == "__main__":
    # 生成日期列表
    date_list = pd.date_range("20250703", "20250703                                                                                                                                                                  ").strftime("%Y-%m-%d").tolist()
    
    # 处理每个日期
    for date_str in date_list:
        print(f"\n开始处理日期: {date_str}")
        download_and_merge_pdfs(date_str)
        time.sleep(1)  # 日期之间稍作停顿