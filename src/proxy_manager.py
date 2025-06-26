import requests
import random
import time
from loguru import logger

class ProxyManager:
    def __init__(self):
        self.working_proxies = []
        self.failed_proxies = set()
        self.current_proxy_index = 0
    
    def get_free_proxies(self):
        """Get free proxies from various sources"""
        import json
        import os
        
        working_proxies_file = "working_proxies.json"
        
        try:
            if os.path.exists(working_proxies_file):
                with open(working_proxies_file, 'r') as f:
                    data = json.load(f)
                    proxies = data.get('proxies', [])
                    
                    # Extract just the proxy addresses
                    proxy_list = []
                    for proxy_info in proxies:
                        proxy_address = proxy_info.get('proxy', '')
                        if proxy_address:
                            # Ensure format is ip:port
                            if not proxy_address.startswith('http://'):
                                proxy_list.append(proxy_address)
                            else:
                                # Remove http:// prefix
                                proxy_list.append(proxy_address.replace('http://', ''))
                    
                    print(f"Loaded {len(proxy_list)} working proxies from file")
                    return proxy_list
            else:
                print(f"Working proxies file not found: {working_proxies_file}")
                print("Please run proxy_tester.py first to generate working proxies")
                return []
                
        except Exception as e:
            print(f"Error loading working proxies: {str(e)}")
            return []
        # proxy_urls = [
        #     "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        #     "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/proxy.txt",
        #     "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
        #     "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
        #     "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        #     "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
        #     "https://raw.githubusercontent.com/UserR00T/proxy-list/main/online/http.txt",
        #     "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
        #     "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all&format=textplain",
        #     "https://www.proxy-list.download/api/v1/get?type=http",
        #     "https://raw.githubusercontent.com/almroot/proxylist/master/list.txt"
        # ]
        
        all_proxies = []
        for url in proxy_urls:
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    proxies = response.text.strip().split('\n')
                    all_proxies.extend([p.strip() for p in proxies if p.strip()])
                    logger.info(f"Got {len(proxies)} proxies from {url}")
            except Exception as e:
                logger.error(f"Failed to get proxies from {url}: {e}")
        
        return list(set(all_proxies))  # Remove duplicates
    
    def test_proxy(self, proxy, test_url="https://masothue.com"):
        """Test if a proxy works"""
        try:
            proxies = {
                'http': f'http://{proxy}',
                'https': f'http://{proxy}'
            }
            
            response = requests.get(
                test_url, 
                proxies=proxies, 
                timeout=15,
                headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
            )
            
            if response.status_code == 200:
                logger.success(f"✅ Proxy {proxy} works")
                return True
            else:
                logger.warning(f"❌ Proxy {proxy} returned status {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Proxy {proxy} failed: {e}")
            return False
    
    def find_working_proxies(self, max_proxies=10):
        """Find working proxies"""
        logger.info("Getting proxy list...")
        all_proxies = self.get_free_proxies()
        random.shuffle(all_proxies)
        
        logger.info(f"Testing {len(all_proxies)} proxies...")
        working_count = 0
        # max_proxies = 3;

        for proxy in all_proxies:
            print(f"working_count: {working_count}")
            if working_count >= max_proxies:
                break
                
            if self.test_proxy(proxy):
                self.working_proxies.append(proxy)
                working_count += 1
            
            time.sleep(0.5)  # Small delay between tests
        
        logger.info(f"Found {len(self.working_proxies)} working proxies")
        return self.working_proxies
    
    def get_next_proxy(self):
        """Get next working proxy in rotation"""
        if not self.working_proxies:
            return None
        
        proxy = self.working_proxies[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.working_proxies)
        return proxy
    
    def mark_proxy_failed(self, proxy):
        """Mark proxy as failed and remove from working list"""
        if proxy in self.working_proxies:
            self.working_proxies.remove(proxy)
            self.failed_proxies.add(proxy)
            logger.warning(f"Removed failed proxy: {proxy}")