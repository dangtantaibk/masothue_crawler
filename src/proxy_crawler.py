import random
import time
from loguru import logger
from proxy_manager import ProxyManager
from premium_proxy_manager import PremiumProxyManager
import requests
from lxml import html
import pandas as pd
import os
import logging
from mst_crawler import print_company_info
from libs.user_agent import USER_AGENT

import ssl
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ProxyCrawler:
    def __init__(self, use_premium=True):
        self.use_premium = use_premium
        if use_premium:
            self.proxy_manager = PremiumProxyManager()
            self.current_service = 'brightdata'  # or 'brightdata', 'proxyrotator'
        else:
            self.proxy_manager = ProxyManager()
            self.proxy_manager.find_working_proxies(max_proxies=3)
        
        self.request_count = 0
        self.proxy_rotation_interval = 5  # Change proxy every 5 requests
        self.base_url = "https://masothue.com"
                # Create a session with SSL configuration
        self.session = requests.Session()
        
        # Configure SSL adapter
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 504]
            )
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    
    def get_request_with_proxy(self, url, max_retries=100):
        """Make request with proxy rotation"""
        for attempt in range(max_retries):
            try:
                # Debug proxy manager state
                logger.info(f"Attempt {attempt + 1}/{max_retries}")
                
                proxy = None
                
                # Only log working proxies info for free proxy manager
                if hasattr(self.proxy_manager, 'working_proxies'):
                    logger.info(f"Working proxies count: {len(self.proxy_manager.working_proxies)}")
                    logger.info(f"Current proxy index: {self.proxy_manager.current_proxy_index}")
                else:
                    logger.info("Using premium proxy manager")
                # Get proxy config
                if self.use_premium:
                    proxy_config = self.proxy_manager.get_proxy_config(self.current_service)
                else:
                    try:
                        proxy = self.proxy_manager.get_next_proxy()
                        print(f"Using proxy: {proxy}")
                        if not proxy:
                            logger.error("No working proxies available, using direct connection")
                            # proxy_config = None
                            raise Exception("No working proxies available - stopping execution")
                        else:
                            proxy_config = {
                                'http': f'http://{proxy}',
                                'https': f'http://{proxy}'
                            }
                    except IndexError as e:
                        logger.error(f"IndexError when getting proxy: {e}")
                        logger.error(f"Working proxies: {self.proxy_manager.working_proxies}")
                        logger.error(f"Current index: {self.proxy_manager.current_proxy_index}")
                        raise e
                
                # Rotate proxy if needed
                if self.request_count % self.proxy_rotation_interval == 0 and not self.use_premium:
                    if proxy:
                        proxy = self.proxy_manager.get_next_proxy()
                        if proxy:
                            proxy_config = {
                                'http': f'http://{proxy}',
                                'https': f'http://{proxy}'
                            }

                #  Kiểm tra danh sách proxy trước khi truy cập
                # if not self.working_proxies:
                #     print("Proxy list is empty")
                #     return None
                    
                # # Kiểm tra index trước khi truy cập
                # if len(self.working_proxies) <= self:
                #     print(f"Proxy index {proxy_index} out of range. List length: {len(proxies_list)}")
                #     return None
                
                headers = self.get_random_headers()
                print("========", proxy_config)
                if hasattr(self.proxy_manager, 'working_proxies'):
                    
                    print("proxy_manager ========", self.proxy_manager)
                    print("get_free_proxies ========", self.proxy_manager.get_free_proxies())
                    print("get_next_proxy========", self.proxy_manager.get_next_proxy())
                # Make request
                full_url = f"{self.base_url}{url}" if url.startswith('/') else url
                response = requests.get(
                    full_url,
                    # proxies=proxy_config,
                    headers=headers,
                    timeout=30,
                )
                

                # Get proxy and make request
                # proxy = self.proxy_manager.get_proxy()
                # if not proxy:
                #     logger.error("No proxy available")
                #     continue
                    
                # Make request with proxy and disabled SSL verification
                # response = self.session.get(
                #     full_url, 
                #     proxies=proxy_config, 
                #     timeout=10,
                #     # verify=False,  # Disable SSL verification
                #     headers=headers
                # )
                
                self.request_count += 1
                
                if response.status_code == 200:
                    # Set encoding for Vietnamese content
                    response.encoding = 'iso-8859-1'
                    logger.info(f"Request successful with proxy: {proxy if proxy_config else 'direct'}")
                    return response
                elif response.status_code == 403:
                    logger.warning(f"Got 403 with proxy, trying next proxy...")
                    if not self.use_premium and proxy:
                        self.proxy_manager.mark_proxy_failed(proxy)
                    continue
                else:
                    logger.warning(f"Got status {response.status_code}")
                    
            except Exception as e:
                logger.error(f"Request failed: {e}")
                logger.error(f"Exception type: {type(e).__name__}")
                
                # If it's an IndexError, log more details
                if isinstance(e, IndexError):
                    logger.error(f"IndexError details - working_proxies length: {len(self.proxy_manager.working_proxies)}")
                    logger.error(f"Current index: {self.proxy_manager.current_proxy_index}")
            
                # print(f"List length: {len(proxies_list)}")
                # print(f"Trying to access index: {proxy_index}")
                if not self.use_premium and 'proxy' in locals() and proxy:
                    self.proxy_manager.mark_proxy_failed(proxy)
            
            # Wait before retry
            time.sleep(random.uniform(2, 5))
        
        return None
    
    def get_random_headers(self):
        """Get random headers with Vietnamese locale"""
        user_agents = USER_AGENT
        
        return {
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
    
    def crawl_single_company_with_proxy(self, company_slug: str) -> dict:
        """
        Crawl dữ liệu cho một company cụ thể sử dụng proxy
        
        Args:
            company_slug: Slug của company
        
        Returns:
            Dictionary chứa dữ liệu company
        """
        try:
            response = self.get_request_with_proxy(company_slug)
            if not response:
                logger.error(f'Failed to get data for company: {company_slug}')
                return None
            response.encoding = 'iso-8859-1'
            # Debug: In ra content đã decode đúng
            print(f"Correctly decoded content (first 300 chars):")
            print(response.text[:300])
            
            tree = html.fromstring(response.content)
            
            # Parse company data similar to crawl_data_company_by_url
            company_data = self.parse_company_data(tree, response, company_slug)
            
            if company_data:
                logger.info(f"Successfully crawled: {company_slug}")
                # Print company info
                print_company_info(company_data)
                
                # Add delay between requests
                time.sleep(random.uniform(1, 3))
                
            return company_data
            
        except Exception as e:
            logger.error(f"Error crawling {company_slug}: {str(e)}")
            return None
    
    def parse_company_data(self, tree, response, url):
        """Parse company data from HTML tree"""
        ignore_text = ['Bị ẩn theo yêu cầu người dùng']

        try:
            # tree, response = get_request(url, headers, proxies)
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
        
        try:

            # Extract company information using XPath
            company_name_el = tree.xpath('//thead//span')
            company_name_globe_el = tree.xpath('//i[@class="fa fa-globe"]/../../td[2]/span')
            company_name_short_el = tree.xpath('//i[@class="fa fa-reorder"]/../../td[2]/span')
            conpany_tax_el = tree.xpath('//i[@class="fa fa-hashtag"]/../../td[2]/span')
            conpany_address_el = tree.xpath('//i[@class="fa fa-map-marker"]/../../td[2]/span')
            conpany_user_el = tree.xpath('//i[@class="fa fa-user"]/../../td[2]/span/a')
            conpany_phone_el = tree.xpath('//i[@class="fa fa-phone"]/../../td[2]/span')
            conpany_active_date_el = tree.xpath('//i[@class="fa fa-calendar"]/../../td[2]/span')
            conpany_manage_el = tree.xpath('//i[@class="fa fa-users"]/../../td[2]/span')
            conpany_category_el = tree.xpath('//i[@class="fa fa-building"]/../../td[2]/a')
            conpany_status_el = tree.xpath('//i[@class="fa fa-info"]/../../td[2]/a')
            conpany_briefcase_el = tree.xpath('//i[@class="fa fa-briefcase"]/../../td[2]/a') # Ngành nghề chính
            conpany_last_update_el = tree.xpath('//td//em')
            company_career_els = tree.xpath('//table[@class="table"]//tbody//tr')

            # Extract text content safely
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
            conpany_briefcase = len(conpany_briefcase_el) and conpany_briefcase_el[0].text_content().strip() or None
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
            if conpany_briefcase in ignore_text:
                conpany_briefcase = None
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
                'conpany_briefcase': conpany_briefcase,
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
            return company_data
            
        except Exception as e:
            logger.error(f"Error parsing company data: {e}")
            return None

    def crawl_batch_data_with_proxy(self, batch_df: pd.DataFrame) -> pd.DataFrame:
        """
        Crawl dữ liệu cho một batch companies sử dụng proxy
        
        Args:
            batch_df: DataFrame chứa company slugs của batch
        
        Returns:
            DataFrame chứa dữ liệu đã crawl
        """
        crawled_results = []
        
        for idx, row in batch_df.iterrows():
            try:
                company_slug = row['slug']
                
                # Crawl company data với proxy
                company_data = self.crawl_single_company_with_proxy(company_slug)
                
                if company_data:
                    crawled_results.append(company_data)
                    
                logger.info(f"Progress: {idx + 1}/{len(batch_df)} - Crawled: {company_slug}")
                
            except Exception as e:
                logger.error(f"Error crawling {company_slug}: {str(e)}")
                continue
        
        return pd.DataFrame(crawled_results)

    def process_company_slugs_in_batches_with_proxy(self, input_csv: str, batch_size: int = 100):
        """
        Xử lý file company_slugs.csv theo từng batch sử dụng proxy
        
        Args:
            input_csv: Đường dẫn file company_slugs.csv gốc
            batch_size: Số lượng items trong mỗi batch
        """
        from mst_crawler import split_csv_into_batches
        
        logger.info(f"Starting proxy batch processing for {input_csv}")
        
        # Tách file thành các batch
        batch_files = split_csv_into_batches(input_csv, batch_size)
        
        # Tạo thư mục để lưu kết quả
        results_dir = "crawled_results"
        os.makedirs(results_dir, exist_ok=True)
        
        # Xử lý từng batch
        for batch_idx, batch_file in enumerate(batch_files, 1):
            try:
                logger.info(f"Processing batch {batch_idx}/{len(batch_files)}: {batch_file}")
                
                # Đọc batch hiện tại
                batch_df = pd.read_csv(batch_file)
                
                # Crawl data cho batch này với proxy
                crawled_data = self.crawl_batch_data_with_proxy(batch_df)
                
                # Tạo tên file kết quả
                result_filename = f"crawled_batch_{batch_idx:03d}.csv"
                result_filepath = os.path.join(results_dir, result_filename)
                
                # Lưu kết quả crawl
                if not crawled_data.empty:
                    crawled_data.to_csv(result_filepath, index=False, encoding='utf-8')
                    logger.info(f"Completed batch {batch_idx}: Saved {len(crawled_data)} records to {result_filepath}")
                else:
                    logger.warning(f"Batch {batch_idx}: No data crawled successfully")
                
                # Nghỉ giữa các batch để tránh bị block
                if batch_idx < len(batch_files):
                    wait_time = random.uniform(5, 10)
                    logger.info(f"Waiting {wait_time:.1f} seconds before next batch...")
                    time.sleep(wait_time)
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_idx}: {str(e)}")
                continue
        
        logger.info("Proxy batch processing completed")
        
        # Merge all results into final file
        self.merge_batch_results(results_dir)

    def merge_batch_results(self, results_dir: str = "crawled_results", output_file: str = "final_crawled_data.csv"):
        """
        Gộp tất cả kết quả từ các batch thành một file cuối cùng
        """
        batch_files = [f for f in os.listdir(results_dir) if f.startswith('crawled_batch_') and f.endswith('.csv')]
        batch_files.sort()
        
        if not batch_files:
            logger.warning("No batch files found to merge")
            return
        
        all_data = []
        
        for batch_file in batch_files:
            batch_path = os.path.join(results_dir, batch_file)
            try:
                batch_df = pd.read_csv(batch_path)
                all_data.append(batch_df)
                logger.info(f"Loaded {len(batch_df)} records from {batch_file}")
            except Exception as e:
                logger.error(f"Error reading {batch_file}: {e}")
                continue
        
        if all_data:
            # Gộp tất cả data
            final_df = pd.concat(all_data, ignore_index=True)
            final_df.to_csv(output_file, index=False, encoding='utf-8')
            
            logger.info(f"Merged {len(final_df)} total records into {output_file}")
        else:
            logger.warning("No valid data found to merge")

# Usage example
def main():
    """Main function to run proxy batch processing"""
    
    # Option 1: Use free proxies
    crawler = ProxyCrawler(use_premium=True)
    
    # Option 2: Use premium proxies (recommended for production)
    # crawler = ProxyCrawler(use_premium=True)
    
    # Process company slugs with proxy
    # Xử lý miền trung
    crawler.process_company_slugs_in_batches_with_proxy("company_slugs.csv", batch_size=50)

if __name__ == "__main__":
    main()