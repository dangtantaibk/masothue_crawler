import urllib.request
import ssl

# BrightData proxy configuration
BRIGHTDATA_PROXY = {
    # 'username': 'brd-customer-hl_700e76f1-zone-residential_proxy1',
    # 'password': '5nmllyvpntt7',
    # 'endpoint': 'brd.superproxy.io',
    # 'port': 33335

    'username': 'package-295135-country-vn',
    'password': '8lPYXZ9lVkIRmshQ',
    'endpoint': 'proxy.soax.com',
    'port': 5000
}

ignore_text = [
    'Bị ẩn theo yêu cầu người dùng'
]

def create_proxy_opener():
    """Create urllib opener with BrightData proxy configuration"""
    proxy_url = f"http://{BRIGHTDATA_PROXY['username']}:{BRIGHTDATA_PROXY['password']}@{BRIGHTDATA_PROXY['endpoint']}:{BRIGHTDATA_PROXY['port']}"
    
    # Create SSL context that doesn't verify certificates
    ssl_context = ssl._create_unverified_context()
    
    # Build opener with proxy and SSL handler
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({
            'http': proxy_url,
            'https': proxy_url
        }),
        urllib.request.HTTPSHandler(context=ssl_context)
    )
    
    # Add user agent header
    opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')]
    
    return opener

def test_proxy_connection():
    """Test the proxy connection"""
    test_url = 'https://masothue.com/3200737004-001-chi-nhanh-cong-ty-tnhh-dich-vu-cong-nghe-va-xay-dung-one-touch'
    
    try:
        opener = create_proxy_opener()
        response = opener.open(test_url, timeout=30)
        result = response.read().decode('utf-8')
        print("Proxy connection successful!")
        print(f"Response: {result}")
        from lxml import html

        # Giả sử html_content là nội dung HTML đầy đủ
        tree = html.fromstring(result)

        company_els = tree.xpath('//tbody//tr')
        company_name_el = tree.xpath('//thead//span')  # Tên công ty
        company_name_globe_el = tree.xpath('//i[@class="fa fa-globe"]/../../td[2]/span')  # Tên quốc tế
        company_name_short_el = tree.xpath('//i[@class="fa fa-reorder"]/../../td[2]/span')  # Tên viết tắt
        conpany_tax_el = tree.xpath('//i[@class="fa fa-hashtag"]/../../td[2]/span')  # Mã số thuế
        conpany_address_el = tree.xpath('//i[@class="fa fa-map-marker"]/../../td[2]/span')  # Địa chỉ
        conpany_user_el = tree.xpath('//i[@class="fa fa-user"]/../../td[2]/span/a')  # Người đại diện
        conpany_phone_el = tree.xpath('//i[@class="fa fa-phone"]/../../td[2]/span')  # Điện thoại
        conpany_active_date_el = tree.xpath('//i[@class="fa fa-calendar"]/../../td[2]/span')  # Ngày hoạt động
        conpany_manage_el = tree.xpath('//i[@class="fa fa-users"]/../../td[2]/span')  # Quản lý bởi
        conpany_category_el = tree.xpath('//i[@class="fa fa-building"]/../../td[2]/a')  # Loại hình doanh nghiệp
        conpany_status_el = tree.xpath('//i[@class="fa fa-info"]/../../td[2]/a')  # Tình trạng hoạt động
        conpany_briefcase_el = tree.xpath('//i[@class="fa fa-briefcase"]/../../td[2]/a')  # Ngành nghề chính
        conpany_last_update_el = tree.xpath('//td//em')  # Cập nhật gần nhất
        company_career_els = tree.xpath('//table[@class="table"]//tbody//tr')  # Ngành nghề kinh doanh
        
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
            # 'mien': mien,
            # 'khu_vuc': khu_vuc,
            # 'tinh': tinh,
            'name': company_name,
            'name_short': company_name_short,
            'name_global': company_name_globe,
            'tax': conpany_tax,
            'address': conpany_address,
            'representative': conpany_user,
            'phone': conpany_phone,
            'active_date': conpany_active_date,
            'manage_by': conpany_manage,
            'category': conpany_category,
            'status': conpany_status,
            'last_update': conpany_last_update,
            'conpany_briefcase': conpany_briefcase,
            'career_code': company_career_codes,
            'career_name': company_career_names,
            'slug': test_url
        }
        from mst_crawler import print_company_info
        print_company_info(company_data)
        
        
        
        return True
    except Exception as e:
        print(f"Proxy connection failed: {e}")
        return False

def make_request_with_proxy(url):
    """Make a request using the configured proxy"""
    try:
        opener = create_proxy_opener()
        response = opener.open(url, timeout=30)
        return response.read().decode('utf-8')
    except Exception as e:
        print(f"Request failed: {e}")
        return None
    

# Test the proxy configuration
if __name__ == "__main__":
    print("Testing BrightData proxy configuration...")
    test_proxy_connection()
    
    # Example usage for other URLs
    # result = make_request_with_proxy('https://httpbin.org/ip')
    # print(f"IP check result: {result}")