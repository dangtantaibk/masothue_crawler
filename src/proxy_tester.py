import requests
import concurrent.futures
import time
from datetime import datetime
import json
import os

class ProxyTester:
    def __init__(self):
        self.proxy_urls = [
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
            "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/proxy.txt",
            "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
            "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
            "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
            "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
            "https://raw.githubusercontent.com/UserR00T/proxy-list/main/online/http.txt",
            "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
            "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all&format=textplain",
            "https://www.proxy-list.download/api/v1/get?type=http",
            "https://raw.githubusercontent.com/almroot/proxylist/master/list.txt"
        ]
        self.working_proxies_file = "working_proxies.json"
        self.test_urls = [
            "https://masothue.com",
        ]
        
    def fetch_proxies_from_url(self, url):
        """Lấy danh sách proxy từ một URL"""
        try:
            print(f"Fetching proxies from: {url}")
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                proxies = []
                for line in response.text.strip().split('\n'):
                    line = line.strip()
                    if line and ':' in line:
                        # Loại bỏ các ký tự không mong muốn
                        if line.count(':') >= 2:
                            # Nếu có nhiều hơn 2 dấu :, lấy 2 phần đầu
                            parts = line.split(':')
                            line = f"{parts[0]}:{parts[1]}"
                        proxies.append(line)
                print(f"Found {len(proxies)} proxies from {url}")
                return proxies
            else:
                print(f"Failed to fetch from {url}: {response.status_code}")
                return []
        except Exception as e:
            print(f"Error fetching from {url}: {str(e)}")
            return []
    
    def get_all_proxies(self):
        """Lấy tất cả proxy từ các nguồn"""
        all_proxies = set()
        
        for url in self.proxy_urls:
            proxies = self.fetch_proxies_from_url(url)
            all_proxies.update(proxies)
            time.sleep(1)  # Tránh spam requests
        
        print(f"Total unique proxies collected: {len(all_proxies)}")
        return list(all_proxies)
    
    def test_single_proxy(self, proxy):
        """Test một proxy duy nhất"""
        try:
            # Format proxy
            if not proxy.startswith('http://'):
                proxy_url = f"http://{proxy}"
            else:
                proxy_url = proxy
            
            proxy_dict = {
                "http": proxy_url,
                "https": proxy_url
            }
            
            # Test với multiple URLs
            success_count = 0
            total_time = 0
            
            for test_url in self.test_urls:
                try:
                    start_time = time.time()
                    response = requests.get(
                        test_url, 
                        proxies=proxy_dict, 
                        timeout=10,
                        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                    )
                    end_time = time.time()
                    
                    if response.status_code == 200:
                        success_count += 1
                        total_time += (end_time - start_time)
                        
                except:
                    continue
            
            # Nếu ít nhất 1 test thành công
            if success_count > 0:
                avg_response_time = total_time / success_count
                return {
                    "proxy": proxy,
                    "success_rate": success_count / len(self.test_urls),
                    "avg_response_time": round(avg_response_time, 2),
                    "tested_at": datetime.now().isoformat()
                }
            else:
                return None
                
        except Exception as e:
            return None
    
    def test_proxies_batch(self, proxies, max_workers=100):
        """Test proxies với threading"""
        working_proxies = []
        total = len(proxies)
        
        print(f"Testing {total} proxies with {max_workers} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_proxy = {executor.submit(self.test_single_proxy, proxy): proxy for proxy in proxies}
            
            # Process completed tasks
            completed = 0
            for future in concurrent.futures.as_completed(future_to_proxy):
                completed += 1
                if completed % 50 == 0:
                    print(f"Progress: {completed}/{total} ({completed/total*100:.1f}%)")
                
                result = future.result()
                if result:
                    working_proxies.append(result)
                    print(f"✓ Working proxy found: {result['proxy']} (Response time: {result['avg_response_time']}s)")
        
        return working_proxies
    
    def save_working_proxies(self, working_proxies):
        """Lưu proxy hoạt động vào file"""
        data = {
            "last_updated": datetime.now().isoformat(),
            "total_working": len(working_proxies),
            "proxies": working_proxies
        }
        
        with open(self.working_proxies_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Saved {len(working_proxies)} working proxies to {self.working_proxies_file}")
    
    def load_working_proxies(self):
        """Load proxy đã lưu từ file"""
        if os.path.exists(self.working_proxies_file):
            try:
                with open(self.working_proxies_file, 'r') as f:
                    data = json.load(f)
                return data.get('proxies', [])
            except:
                return []
        return []
    
    def get_best_proxies(self, limit=10):
        """Lấy các proxy tốt nhất (response time thấp nhất)"""
        working_proxies = self.load_working_proxies()
        if not working_proxies:
            return []
        
        # Sort by response time và success rate
        sorted_proxies = sorted(
            working_proxies, 
            key=lambda x: (x.get('avg_response_time', 999), -x.get('success_rate', 0))
        )
        
        return sorted_proxies[:limit]
    
    def run_full_test(self):
        """Chạy test đầy đủ"""
        print("=== PROXY TESTER STARTED ===")
        
        # Lấy tất cả proxies
        print("\n1. Collecting proxies from sources...")
        all_proxies = self.get_all_proxies()
        
        if not all_proxies:
            print("No proxies found!")
            return
        
        # Test proxies
        print(f"\n2. Testing {len(all_proxies)} proxies...")
        working_proxies = self.test_proxies_batch(all_proxies)
        
        # Lưu kết quả
        print(f"\n3. Saving results...")
        self.save_working_proxies(working_proxies)
        
        print(f"\n=== COMPLETED ===")
        print(f"Total proxies tested: {len(all_proxies)}")
        print(f"Working proxies found: {len(working_proxies)}")
        
        if working_proxies:
            print(f"\nTop 5 fastest proxies:")
            best_proxies = self.get_best_proxies(5)
            for i, proxy in enumerate(best_proxies, 1):
                print(f"{i}. {proxy['proxy']} - {proxy['avg_response_time']}s")

def main():
    tester = ProxyTester()
    
    print("Choose an option:")
    print("1. Run full test (collect + test all proxies)")
    print("2. Show saved working proxies")
    print("3. Get best proxies")
    print("4. Test specific proxy")
    
    choice = input("Enter choice (1-4): ").strip()
    
    if choice == "1":
        tester.run_full_test()
    elif choice == "2":
        proxies = tester.load_working_proxies()
        if proxies:
            print(f"\nFound {len(proxies)} working proxies:")
            for proxy in proxies:
                print(f"- {proxy['proxy']} (Response: {proxy['avg_response_time']}s, Success: {proxy['success_rate']*100:.1f}%)")
        else:
            print("No working proxies found. Run full test first.")
    elif choice == "3":
        best = tester.get_best_proxies(10)
        if best:
            print(f"\nTop 10 best proxies:")
            for i, proxy in enumerate(best, 1):
                print(f"{i}. {proxy['proxy']} - {proxy['avg_response_time']}s")
        else:
            print("No working proxies found. Run full test first.")
    elif choice == "4":
        proxy = input("Enter proxy (ip:port): ").strip()
        result = tester.test_single_proxy(proxy)
        if result:
            print(f"✓ Proxy works: {result}")
        else:
            print("✗ Proxy doesn't work")
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()