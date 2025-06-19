# -*- coding: utf-8 -*-

import json
from random import randrange
import requests
from lxml import html
from loguru import logger
from lxml import etree
import pandas as pd
import re
from unidecode import unidecode
import csv
import time
import os
import math
from typing import List
import logging
from libs.user_agent import USER_AGENT
import database
import pattern

useragent = USER_AGENT[randrange(len(USER_AGENT)-1)]
# useragent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
# useragent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
# const_headers = {'User-Agent': useragent}
const_headers = {
    'User-Agent': useragent,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'identity',  # Không cho phép compression
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Upgrade-Insecure-Requests': '1'
}
const_proxies = False

ignore_text = [
    'Bị ẩn theo yêu cầu người dùng'
]

session = requests.Session()
def get_request(path_url, headers={}, proxies=False):
    url = f'{pattern.BASE_URL}{path_url}'
    # final_headers = default_headers
    logger.info(f'Send GET request:\n- URL: {url}\n- Header: {const_headers}\n- Proxy: {proxies}')
    response = requests.get(url, headers=const_headers, proxies=proxies)
    
    # Set encoding đúng cho tiếng Việt
    response.encoding = 'iso-8859-1'

    # Parse HTML với encoding đã fix
    # tree = etree.HTML(response.text)

    # Debug: In ra content đã decode đúng
    print(f"Correctly decoded content (first 300 chars):")
    print(response.text[:300])

    if response.status_code == 200:
        tree = html.fromstring(response.content)
        logger.info('Send GET request successfully')
        return tree, response
    else:
        logger.error(f'Failed to retrieve content. Status code: {response.status_code}')
        return False


def is_more_data(handler_page, active_page, els):
    return handler_page == active_page and els


def crawl_data_province(url, headers=const_headers, proxies=False):
    tree = get_request(url, headers, proxies)
    if not tree:
        logger.error('Failed to get data provinces')
        return


    conn = database.get_db_connection()
    cur = conn.cursor()

    # Sử dụng XPath để tìm phần tử
    province_els = tree.xpath('//table//tr')
    data_insert = []
    for province_el in province_els:
        province_info = province_el.xpath(".//a")[0]
        province_name = province_info.text_content().strip()
        province_link = province_info.get('href')
        data_insert.append({
            "name": province_name,
            "slug": province_link
        })
        logger.info(f'- {province_name} | {province_link}')
        sql = """
            INSERT INTO mst_province (name, slug)
            VALUES (%s, %s)
        """
        cur.execute(sql, (province_name, province_link))

    database.end_db_connection(cur, conn)

    logger.info(f'Get provinces data successfully. Total: {len(province_els)}')
    return


def crawl_data_district(headers=const_headers, proxies=False):
    conn = database.get_db_connection()
    cur = conn.cursor()
    sql = """SELECT id, name, slug FROM mst_province;"""
    cur.execute(sql)
    provinces = cur.fetchall()
    for province in provinces:
        crawl_data_district_by_province(province, cur, conn, headers, proxies)
    database.end_db_connection(cur, conn)


def crawl_data_district_by_province(province_data, cur, conn, headers=const_headers, proxies=False):
    # province_data = (1, 'Hà Nội', '/tra-cuu-ma-so-thue-theo-tinh/ha-noi-7') | id - name - slug
    logger.info(f'Get Province in {province_data[1]} by link {province_data[2]}')
    tree = get_request(province_data[2], const_headers, proxies)
    if not tree:
        logger.error('Failed to get data districts')
        return

    # Sử dụng XPath để tìm phần tử
    district_els = tree.xpath('//div[@id="sidebar"]//li')
    for district_el in district_els:
        district_info = district_el.xpath(".//a")[0]
        district_name = district_info.text_content().strip()
        district_link = district_info.get('href')
        logger.info(f'- {district_name} | {district_link}')
        sql = """
            INSERT INTO mst_district (name, slug, province_id, province_name)
            VALUES (%s, %s, %s, %s)
        """
        cur.execute(sql, (district_name, district_link, province_data[0], province_data[1]))
    logger.info(f'Get districts data successfully. Total: {len(district_els)}')
    return


def crawl_data_district_by_url(url, headers=const_headers, proxies=False):
    tree = get_request(url, headers, proxies)
    if not tree:
        logger.error('Failed to get data districts')
        return

    # Sử dụng XPath để tìm phần tử
    district_els = tree.xpath('//div[@id="sidebar"]//li')
    for district_el in district_els:
        district_info = district_el.xpath(".//a")[0]
        district_name = district_info.text_content().strip()
        district_link = district_info.get('href')
        print(f'- {district_name} | {district_link}')

    logger.info(f'Get districts data successfully. Total: {len(district_els)}')
    return


def crawl_data_career(url, headers=const_headers, proxies=False):
    conn = database.get_db_connection()
    cur = conn.cursor()


    is_more = True
    handle_page = 1
    try_count = 0
    total_crawl = 0
    while is_more:
        handle_url = f'{url}?page={handle_page}'
        try:
            tree = get_request(handle_url, headers, proxies)
            if not tree:
                logger.error('Failed to get data career')
                return
        except requests.exceptions.RequestException as err:
            logger.error(err.response.json())
            if try_count > 3:
                logger.info('Not have more data need crawl.')
                is_more = False
                return err.response.json()
            else:
                try_count += 1
                logger.warning('Try crawl more data!')

        # Sử dụng XPath để tìm phần tử
        career_els = tree.xpath('//tbody//tr')
        for career_el in career_els:
            career_code_info = career_el.xpath(".//td[1]//a")[0]
            career_code = career_code_info.text_content().strip()
            career_name = career_el.xpath(".//td[2]//a")[0].text_content().strip()
            career_link = career_code_info.get('href')
            print(f'- {career_code} | {career_name} | {career_link}')
            sql = """
                        INSERT INTO mst_career (code, name, slug)
                        VALUES (%s, %s, %s)
                    """
            cur.execute(sql, (str(career_code), career_name, career_link))

        total_crawl += len(career_els)

        active_pages = tree.xpath('//span[@class="page-numbers current"]')
        active_page = int(active_pages[0].text_content().strip())
        if is_more_data(handle_page, active_page, career_els):
            handle_page += 1
            try_count = 0
            logger.info('Have more data need crawl')
        else:
            if try_count > 3:
                logger.info('Not have more data need crawl.')
                is_more = False
            else:
                try_count += 1
                logger.warning('Try crawl more data!')
        logger.info(f'Get districts data successfully. Total: {total_crawl}')

    database.end_db_connection(cur, conn)
    return

def crawl_data_company(crawl_by='district', headers=const_headers, proxies=False):
    logger.info(f'Get companies data by {crawl_by}')
    conn = database.get_db_connection()
    cur = conn.cursor()
    sql_by_mode = {
        'province': 'SELECT * FROM mst_province;',
        'district': 'SELECT * FROM mst_district;',
        'career': 'SELECT * FROM mst_career;',
    }
    cur.execute(sql_by_mode.get(crawl_by))
    datas = cur.fetchall()
    for data in datas:
        # district_data = (1, 'Hà Nội', '/tra-cuu-ma-so-thue-theo-tinh/ha-noi-11483', 1, 'Hà Nội') | id, name, slug, province_id, province_name
        crawl_data_company_by_data(crawl_by, data, cur, conn, headers, proxies)
    database.end_db_connection(cur, conn)
    logger.info(f'Get companies data by {crawl_by} successfully!')


def crawl_data_company_by_data(crawl_by, data, cur, conn, headers=const_headers, proxies=False):
    logger.info(f'crawl_data_company_by_data {crawl_by}.')
    # district_data = (1, 'Hà Nội', '/tra-cuu-ma-so-thue-theo-tinh/ha-noi-11483', 1, 'Hà Nội') | id, name, slug, province_id, province_name
    province_id = None
    district_id = None
    career_id = None
    url = ''
    if crawl_by == 'province':
        province_id = data[0]
        url = data[2]
    elif crawl_by == 'district':
        province_id = data[3]
        district_id = data[0]
        url = data[2]
    else:
        career_id = data[0]
        url = data[3]
    logger.info(f'crawl_data_company_by_data {crawl_by}:\n- province_id: {province_id}\n- district_id: {district_id}\n- career_id: {career_id}\n- url: {url}')

    is_more = True
    handle_page = 1
    try_count = 0
    total_crawl = 0
    while is_more:
        handle_url = f'{url}?page={handle_page}'
        try:
            tree = get_request(handle_url, headers, proxies)
            if not tree:
                logger.error('Failed to get data career')
                return
        except requests.exceptions.RequestException as err:
            logger.error(err.response.json())
            if try_count > 3:
                logger.info('Not have more data need crawl.')
                is_more = False
                return err.response.json()
            else:
                try_count += 1
                logger.warning('Try crawl more data!')

        # Sử dụng XPath để tìm phần tử
        company_els = tree.xpath('//div[@class="tax-listing"]/div')
        for company_el in company_els:
            company_link_el = company_el.xpath("./h3/a")[0]
            company_link = company_link_el.get('href')
            company_name = company_link_el.text_content().strip()
            crawl_data_company_by_url(company_link, province_id, district_id, career_id, cur, conn, headers, proxies)

        total_crawl += len(company_els)

        active_pages = tree.xpath('//span[@class="page-numbers current"]')
        active_page = int(active_pages[0].text_content().strip())
        if is_more_data(handle_page, active_page, company_els):
            handle_page += 1
            try_count = 0
            logger.info('Have more data need crawl')
        else:
            if try_count > 3:
                logger.info('Not have more data need crawl.')
                is_more = False
            else:
                try_count += 1
                logger.warning('Try crawl more data!')
        logger.info(f'Get company data successfully. Total: {total_crawl}')
    return


def crawl_data_company_by_url(url='', province_id=None, district_id=None, career_id=None, cur=False, conn=False, headers={}, proxies=False):
    try:
        tree, response = get_request(url, headers, proxies)
        with open('debug_response.html', 'w', encoding='utf-8') as f:
            if hasattr(tree, 'tag'):
                # Lấy text từ response đã decode đúng
                if hasattr(response, 'text'):
                    f.write(response.text)
                else:
                    html_content = etree.tostring(tree, encoding='unicode', pretty_print=True)
                    f.write(html_content)
            else:
                f.write(str(tree))
        if tree is None:
            logger.error('Failed to get data company')
            return
    except requests.exceptions.RequestException as err:
        logger.error(err.response.json())
        return err.response.json()

    # Sử dụng XPath để tìm phần tử
    company_els = tree.xpath('//tbody//tr')
    company_name_el = tree.xpath('//thead//span') # Tên công ty
    company_name_globe_el = tree.xpath('//i[@class="fa fa-globe"]/../../td[2]/span') # Tên quốc tế
    company_name_short_el = tree.xpath('//i[@class="fa fa-reorder"]/../../td[2]/span') # Tên viết tắt
    conpany_tax_el = tree.xpath('//i[@class="fa fa-hashtag"]/../../td[2]/span') # Mã số thuế
    conpany_address_el = tree.xpath('//i[@class="fa fa-map-marker"]/../../td[2]/span') # Địa chỉ
    conpany_user_el = tree.xpath('//i[@class="fa fa-user"]/../../td[2]/span/a') # Người đại diện
    conpany_phone_el = tree.xpath('//i[@class="fa fa-phone"]/../../td[2]/span') # Điện thoại
    conpany_active_date_el = tree.xpath('//i[@class="fa fa-calendar"]/../../td[2]/span') # Ngày hoạt động
    conpany_manage_el = tree.xpath('//i[@class="fa fa-users"]/../../td[2]/span') # Quản lý bởi
    conpany_category_el = tree.xpath('//i[@class="fa fa-building"]/../../td[2]/a') # Loại hình doanh nghiệp
    conpany_status_el = tree.xpath('//i[@class="fa fa-info"]/../../td[2]/a') # Tình trạng hoạt động
    conpany_last_update_el = tree.xpath('//td//em') # Cập nhật gần nhất
    company_career_els = tree.xpath('//table[@class="table"]//tbody//tr') # Ngành nghề kinh doanh

    company_name = len(company_name_el) and company_name_el[0].text_content().strip() or None
    company_name_globe = len(company_name_globe_el) and company_name_globe_el[0].text_content().strip() or None
    company_name_short = len(company_name_short_el) and company_name_short_el[0].text_content().strip() or None
    conpany_tax = len(conpany_tax_el) and conpany_tax_el[0].text_content().strip() or None
    conpany_address = len(conpany_address_el) and conpany_address_el[0].text_content().strip() or None
    conpany_user = len(conpany_user_el) and conpany_user_el[0].text_content().strip() or None
    conpany_phone = len(conpany_phone_el) and conpany_phone_el[0].text_content().strip() or None
    conpany_active_date = len(conpany_active_date_el) and conpany_active_date_el[0].text_content().strip() or None
    conpany_manage = len(conpany_manage_el) and conpany_manage_el[0].text_content().strip() or None
    conpany_category = len(conpany_category_el) and conpany_category_el[0].text_content().strip() or None
    conpany_status = len(conpany_status_el) and conpany_status_el[0].text_content().strip() or None
    conpany_last_update = len(conpany_last_update_el) and conpany_last_update_el[0].text_content().strip() or None

    if company_name in ignore_text:
        company_name = None
    if company_name_globe in ignore_text:
        company_name_globe = None
    if company_name_short in ignore_text:
        company_name_short = None
    if conpany_tax in ignore_text:
        company_name = None
    if conpany_address in ignore_text:
        conpany_address = None
    if conpany_user in ignore_text:
        conpany_user = None
    if conpany_phone in ignore_text:
        conpany_phone = None
    if conpany_active_date in ignore_text:
        conpany_active_date = None
    if conpany_manage in ignore_text:
        conpany_manage = None
    if conpany_category in ignore_text:
        conpany_category = None
    if conpany_status in ignore_text:
        conpany_status = None
    if conpany_last_update in ignore_text:
        conpany_last_update = None

    # print('Ngành nghề kinh doanh')
    company_career_code = []
    company_career_name = []
    for company_career in company_career_els:
        career_code_info = company_career.xpath(".//td[1]//a")[0]
        career_code = career_code_info.text_content().strip()
        company_career_code.append(career_code)
        company_career_name.append(company_career.xpath(".//td[2]//a")[0].text_content().strip())
    company_career_codes = ', '.join(company_career_code)
    company_career_names = ', '.join(company_career_name)

    company_data = {
        'name': company_name,
        'name_short': company_name_short,
        'name_global': company_name_globe,
        'tax': conpany_tax,
        'address': conpany_address,
        # 'district_id': district_id,
        # 'province_id': province_id,
        'representative': conpany_user,
        'phone': conpany_phone,
        'active_date': conpany_active_date,
        'manage_by': conpany_manage,
        'category': conpany_category,
        'status': conpany_status,
        'last_update': conpany_last_update,
        'career_code': company_career_codes,
        'career_name': company_career_names,
        'slug': url
    }
    # In thông tin với format đẹp
    print_company_info(company_data)
    
    return company_data

    # print(company_career_codes)
    # print(', '.join(company_career_names))

    # sql = """
    #     INSERT INTO mst_company (name, name_short, name_global, tax, address, district_id, province_id, representative, phone, active_date, manage_by, category, status, last_update, career_code, career_name, slug)
    #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    # """
    # cur.execute(sql, (
    #     company_name, company_name_short, company_name_globe, conpany_tax,
    #     conpany_address, district_id, province_id,
    #     conpany_user, conpany_phone,
    #     conpany_active_date, conpany_manage, conpany_category, conpany_status, conpany_last_update,
    #     company_career_codes, company_career_names, url))
    # conn.commit()

def print_company_info(data):
    """In thông tin công ty với format đẹp"""
    print("\n" + "="*80)
    print("📊 THÔNG TIN CÔNG TY")
    print("="*80)
    
    # Thông tin cơ bản
    print(f"🏢 Tên công ty        : {data.get('name', 'N/A')}")
    print(f"📝 Tên viết tắt       : {data.get('name_short', 'N/A')}")
    print(f"🌐 Tên quốc tế        : {data.get('name_global', 'N/A')}")
    print(f"🔢 Mã số thuế        : {data.get('tax', 'N/A')}")
    
    print("\n" + "-"*50)
    print("📍 ĐỊA CHỈ & LIÊN HỆ")
    print("-"*50)
    print(f"🏠 Địa chỉ           : {data.get('address', 'N/A')}")
    # print(f"🏘️  Quận/Huyện        : {data.get('district_id', 'N/A')}")
    # print(f"🗺️  Tỉnh/Thành phố    : {data.get('province_id', 'N/A')}")
    print(f"👤 Người đại diện     : {data.get('representative', 'N/A')}")
    print(f"📞 Số điện thoại      : {data.get('phone', 'N/A')}")
    
    print("\n" + "-"*50)
    print("📋 THÔNG TIN DOANH NGHIỆP")
    print("-"*50)
    print(f"📅 Ngày hoạt động     : {data.get('active_date', 'N/A')}")
    print(f"🏛️  Cơ quan quản lý    : {data.get('manage_by', 'N/A')}")
    print(f"📂 Loại hình         : {data.get('category', 'N/A')}")
    print(f"✅ Trạng thái        : {data.get('status', 'N/A')}")
    print(f"🔄 Cập nhật cuối     : {data.get('last_update', 'N/A')}")
    
    print("\n" + "-"*50)
    print("💼 NGÀNH NGHỀ")
    print("-"*50)
    print(f"🔖 Mã ngành nghề     : {data.get('career_code', 'N/A')}")
    print(f"📊 Tên ngành nghề    : {data.get('career_name', 'N/A')}")
    
    print("\n" + "-"*50)
    print("🔗 KHÁC")
    print("-"*50)
    print(f"🌐 Slug              : {data.get('slug', 'N/A')}")
    
    print("="*80)
    print("✅ HOÀN THÀNH CRAWL THÔNG TIN CÔNG TY")
    print("="*80 + "\n")

def generate_slug(ten_cty, mst):
    clean_mst = str(mst).strip()
    clean_ten_cty = str(ten_cty).strip()
    slug = unidecode(clean_ten_cty)
    slug = slug.replace("&", "-").replace(".", "-").replace("'", "-")
    slug = re.sub(r"[^a-zA-Z0-9\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")
    slug = slug.lower()

    return f"/{clean_mst}-{slug}"

def read_excel_and_generate_slugs(file_path):
    try:
        # Đọc file Excel
        df = pd.read_excel(file_path)
        
        # Lấy cột D (tên công ty) và cột E (mã số thuế)
        # Pandas sử dụng index từ 0, nên cột D = index 3, cột E = index 4
        ten_cty_col = df.iloc[:, 3]  # Cột D
        mst_col = df.iloc[:, 4]      # Cột E
        
        # Tạo danh sách các slug
        slugs = []
        for i in range(len(df)):
            ten_cty = ten_cty_col.iloc[i]
            mst = mst_col.iloc[i]
            
            # Bỏ qua các dòng có giá trị null
            if pd.notna(ten_cty) and pd.notna(mst):
                slug = generate_slug(ten_cty, mst)
                slugs.append({
                    'ten_cong_ty': ten_cty,
                    'ma_so_thue': mst,
                    'slug': slug
                })
        
        return slugs
    
    except Exception as e:
        print(f"Lỗi khi đọc file: {e}")
        return []
    
def save_to_csv_pandas(results, output_file="company_slugs.csv"):
    try:
        df = pd.DataFrame(results)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Đã lưu {len(results)} dòng dữ liệu vào file {output_file}")
        
    except Exception as e:
        print(f"Lỗi khi lưu file CSV: {e}")

def save_company_data_to_csv_pandas(company_data_list, output_file="company_data.csv"):
    try:
        df = pd.DataFrame(company_data_list)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Đã lưu {len(company_data_list)} công ty vào file {output_file}")
        
    except Exception as e:
        print(f"Lỗi khi lưu file CSV: {e}")


def split_csv_into_batches(input_file: str, batch_size: int = 100, output_dir: str = "batches") -> List[str]:
    """
    Tách file CSV thành các batch nhỏ hơn
    
    Args:
        input_file: Đường dẫn file CSV gốc
        batch_size: Số lượng items trong mỗi batch
        output_dir: Thư mục lưu các file batch
    
    Returns:
        List đường dẫn các file batch đã tạo
    """
    # Tạo thư mục output nếu chưa tồn tại
    os.makedirs(output_dir, exist_ok=True)
    
    # Đọc file CSV gốc
    df = pd.read_csv(input_file)
    total_rows = len(df)
    num_batches = math.ceil(total_rows / batch_size)
    
    batch_files = []
    
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_rows)
        
        # Lấy subset data cho batch hiện tại
        batch_df = df.iloc[start_idx:end_idx]
        
        # Tạo tên file cho batch
        batch_filename = f"company_slugs_batch_{i+1:03d}.csv"
        batch_filepath = os.path.join(output_dir, batch_filename)
        
        # Lưu batch vào file
        batch_df.to_csv(batch_filepath, index=False)
        batch_files.append(batch_filepath)
        
        logging.info(f"Created batch {i+1}/{num_batches}: {batch_filename} ({len(batch_df)} items)")
    
    return batch_files

def process_company_slugs_in_batches(input_csv: str, batch_size: int = 100):
    """
    Xử lý file company_slugs.csv theo từng batch
    
    Args:
        input_csv: Đường dẫn file company_slugs.csv gốc
        batch_size: Số lượng items trong mỗi batch
    """
    logging.info(f"Starting batch processing for {input_csv}")
    
    # Tách file thành các batch
    batch_files = split_csv_into_batches(input_csv, batch_size)
    
    # Tạo thư mục để lưu kết quả
    results_dir = "crawled_results"
    os.makedirs(results_dir, exist_ok=True)
    
    # Xử lý từng batch
    for batch_idx, batch_file in enumerate(batch_files, 1):
        try:
            logging.info(f"Processing batch {batch_idx}/{len(batch_files)}: {batch_file}")
            
            # Đọc batch hiện tại
            batch_df = pd.read_csv(batch_file)
            
            # Crawl data cho batch này
            crawled_data = crawl_batch_data(batch_df)
            
            # Tạo tên file kết quả
            result_filename = f"crawled_batch_{batch_idx:03d}.csv"
            result_filepath = os.path.join(results_dir, result_filename)
            
            # Lưu kết quả crawl
            crawled_data.to_csv(result_filepath, index=False)
            
            logging.info(f"Completed batch {batch_idx}: Saved to {result_filepath}")
            
        except Exception as e:
            logging.error(f"Error processing batch {batch_idx}: {str(e)}")
            continue
    
    logging.info("Batch processing completed")

def crawl_batch_data(batch_df: pd.DataFrame) -> pd.DataFrame:
    """
    Crawl dữ liệu cho một batch companies
    
    Args:
        batch_df: DataFrame chứa company slugs của batch
    
    Returns:
        DataFrame chứa dữ liệu đã crawl
    """
    crawled_results = []
    
    for idx, row in batch_df.iterrows():
        try:
            company_slug = row['slug']  # Giả sử column name là 'slug'
            
            # Gọi hàm crawl cho từng company (thay thế bằng hàm crawl thực tế)
            company_data = crawl_single_company(company_slug)
            
            if company_data:
                crawled_results.append(company_data)
                
            logging.info(f"Crawled data for: {company_slug}")
            
        except Exception as e:
            logging.error(f"Error crawling {company_slug}: {str(e)}")
            continue
    
    return pd.DataFrame(crawled_results)

def crawl_single_company(company_slug: str) -> dict:
    """
    Crawl dữ liệu cho một company cụ thể
    
    Args:
        company_slug: Slug của company
    
    Returns:
        Dictionary chứa dữ liệu company
    """
    # Ví dụ:
    return crawl_data_company_by_url(company_slug, const_headers)
    # return crawl_company_details(company_slug)
    

def merge_batch_results(results_dir: str = "crawled_results", output_file: str = "final_crawled_data.csv"):
    """
    Gộp tất cả kết quả từ các batch thành một file cuối cùng
    
    Args:
        results_dir: Thư mục chứa các file kết quả batch
        output_file: Tên file kết quả cuối cùng
    """
    batch_files = [f for f in os.listdir(results_dir) if f.startswith('crawled_batch_')]
    batch_files.sort()
    
    all_data = []
    
    for batch_file in batch_files:
        batch_path = os.path.join(results_dir, batch_file)
        batch_df = pd.read_csv(batch_path)
        all_data.append(batch_df)
        logging.info(f"Loaded {len(batch_df)} records from {batch_file}")
    
    # Gộp tất cả data
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv(output_file, index=False)
    
    logging.info(f"Merged {len(final_df)} total records into {output_file}")




# Lấy thông tin về tỉnh/thành phố
# crawl_data_province(pattern.URL_PATH_BY_PROVINCE, const_headers) # =============================================== Job
# Lấy thông tin quận huyện theo URL
# crawl_data_district() # ========================================================================================== Job
# crawl_data_district_by_url('/tra-cuu-ma-so-thue-theo-tinh/ha-noi-7', const_headers) # Test


# Lấy thông tin ngành nghề
# crawl_data_career(pattern.URL_PATH_BY_CAREER, const_headers) # =================================================== Job

# Đọc file Excel và tạo slug
# file_path = "/Users/taidang/Desktop/hanoi_FULL.xlsx"  # Thay đổi đường dẫn file
# results = read_excel_and_generate_slugs(file_path)

# In kết quả
# for item in results:
#     print(f"Công ty: {item['ten_cong_ty']}")
#     print(f"MST: {item['ma_so_thue']}")
#     print(f"Slug: {item['slug']}")
#     print("-" * 50)

# Lưu company slug vào file CSV
# save_to_csv_pandas(results, "company_slugs.csv")

# Lấy thông tin mã số thuế công ty từ file CSV và crawl dữ liệu
# process_company_slugs("company_slugs.csv", "company_data.csv")



# Lấy thông tin công ty
# crawl_data_company() # =========================================================================================== Job
# crawl_data_company_by_url('/2100689933-cong-ty-tnhh-mtv-vang-bac-kim-hue', const_headers)
# crawl_data_company_by_url('/2100689059-cong-ty-tnhh-xang-dau-tra-vinh-petro', const_headers)
# crawl_data_company_by_url('/0315739605-001-van-phong-dai-dien-cong-ty-tnhh-chint-vietnam-holding-tai-ha-noi', const_headers)
process_company_slugs_in_batches("company_slugs.csv", batch_size=100)
# def main():
#     """Hàm main để chạy batch processing"""
    
#     # Thay vì xử lý toàn bộ file một lúc
#     # process_company_slugs("company_slugs.csv")
    
#     # Sử dụng batch processing
#     # from batch_processor import process_company_slugs_in_batches
    
#     process_company_slugs_in_batches("company_slugs.csv", batch_size=100)
