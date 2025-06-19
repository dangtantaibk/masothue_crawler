import requests
import random
from itertools import cycle

class PremiumProxyManager:
    def __init__(self):
        # Các service proxy tốt cho Việt Nam
        self.proxy_services = {
            'proxyrotator': {
                'endpoint': 'rotating-residential.proxymesh.com:31280',
                'username': 'your_username',
                'password': 'your_password'
            },
            'brightdata': {
                'endpoint': 'zproxy.lum-superproxy.io:22225',
                'username': 'your_username',
                'password': 'your_password'
            },
            'smartproxy': {
                'endpoint': 'gate.smartproxy.com:10001',
                'username': 'your_username', 
                'password': 'your_password'
            }
        }
        
        # Vietnamese proxy endpoints (if available)
        self.vn_proxies = [
            'vn-pr.oxylabs.io:10000',
            'gate.smartproxy.com:10001',  # Has VN endpoints
        ]
    
    def get_proxy_config(self, service='smartproxy'):
        """Get proxy configuration"""
        config = self.proxy_services.get(service)
        if not config:
            return None
        
        return {
            'http': f'http://{config["username"]}:{config["password"]}@{config["endpoint"]}',
            'https': f'http://{config["username"]}:{config["password"]}@{config["endpoint"]}'
        }
    
    def test_premium_proxy(self, service='smartproxy'):
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