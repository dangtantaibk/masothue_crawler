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
        proxy_urls = [
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"
        ]
        
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
        max_proxies = 3;

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