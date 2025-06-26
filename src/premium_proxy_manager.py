import requests
import random
from itertools import cycle
from loguru import logger

class PremiumProxyManager:
    def __init__(self):
        # Các service proxy tốt cho Việt Nam
        self.proxy_services = {
            'brightdata': {
                'username': 'brd-customer-hl_700e76f1-zone-datacenter_proxy1',
                'password': 'kzonwlnmpe46',
                'endpoint': 'brd.superproxy.io',
                'port': 33335
            },
        }
        
    
    def get_proxy_config(self, service='brightdata'):
        """Get proxy configuration"""
        config = self.proxy_services.get(service)
        if not config:
            return None
        
        return {
            'http': f"http://{config['username']}:{config['password']}@{config['endpoint']}:{config['port']}",
            'https': f"http://{config['username']}:{config['password']}@{config['endpoint']}:{config['port']}",
        }
    
    def test_premium_proxy(self, service='brightdata'):
        """Test premium proxy service"""
        proxy_config = self.get_proxy_config(service)
        if not proxy_config:
            return False
        
        try:
            response = requests.get(
                'https://masothue.com',
                proxies=proxy_config,
                timeout=30,
                headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
            )
            
            if response.status_code == 200:
                logger.success(f"✅ Premium proxy {service} works")
                return True
            else:
                logger.warning(f"❌ Premium proxy {service} returned {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Premium proxy {service} failed: {e}")
            return False