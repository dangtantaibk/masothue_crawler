import random
import time
from loguru import logger
from proxy_manager import ProxyManager
from premium_proxy_manager import PremiumProxyManager
import requests

class ProxyCrawler:
    def __init__(self, use_premium=False):
        self.use_premium = use_premium
        if use_premium:
            self.proxy_manager = PremiumProxyManager()
            self.current_service = 'smartproxy'  # or 'brightdata', 'proxyrotator'
        else:
            self.proxy_manager = ProxyManager()
            self.proxy_manager.find_working_proxies(max_proxies=20)
        
        self.request_count = 0
        self.proxy_rotation_interval = 10  # Change proxy every 10 requests
    
    def get_request_with_proxy(self, url, max_retries=3):
        """Make request with proxy rotation"""
        for attempt in range(max_retries):
            try:
                # Get proxy config
                if self.use_premium:
                    proxy_config = self.proxy_manager.get_proxy_config(self.current_service)
                else:
                    proxy = self.proxy_manager.get_next_proxy()
                    if not proxy:
                        logger.error("No working proxies available")
                        return None
                    proxy_config = {
                        'http': f'http://{proxy}',
                        'https': f'http://{proxy}'
                    }
                
                # Rotate proxy if needed
                if self.request_count % self.proxy_rotation_interval == 0:
                    if not self.use_premium:
                        proxy = self.proxy_manager.get_next_proxy()
                        proxy_config = {
                            'http': f'http://{proxy}',
                            'https': f'http://{proxy}'
                        }
                
                headers = self.get_random_headers()
                response = requests.get(
                    url,
                    proxies=proxy_config,
                    headers=headers,
                    timeout=30
                )
                
                self.request_count += 1
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 403:
                    logger.warning(f"Got 403 with proxy, trying next proxy...")
                    if not self.use_premium and proxy:
                        self.proxy_manager.mark_proxy_failed(proxy)
                    continue
                else:
                    logger.warning(f"Got status {response.status_code}")
                    
            except Exception as e:
                logger.error(f"Request failed with proxy: {e}")
                if not self.use_premium and 'proxy' in locals():
                    self.proxy_manager.mark_proxy_failed(proxy)
            
            # Wait before retry
            time.sleep(random.uniform(3, 7))
        
        return None
    
    def get_random_headers(self):
        """Get random headers with Vietnamese locale"""
        user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]
        
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
            'Sec-Fetch-Site': 'none'
        }

# Usage
def main():
    # Option 1: Use free proxies
    crawler = ProxyCrawler(use_premium=False)
    
    # Option 2: Use premium proxies (recommended for production)
    # crawler = ProxyCrawler(use_premium=True)
    
    response = crawler.get_request_with_proxy("https://masothue.com/2100689933-cong-ty-tnhh-mtv-vang-bac-kim-hue")
    if response:
        print("Request successful!")
        print(response.text[:500])  # Print first 500 characters of the response
    else:
        print("Request failed after retries.")
if __name__ == "__main__":
    main()