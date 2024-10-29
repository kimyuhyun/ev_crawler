from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from time import sleep

def scrape_page():
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1280,720')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # 메모리 관련 옵션
        chrome_options.add_argument('--aggressive-cache-discard')
        chrome_options.add_argument('--disable-application-cache')
        chrome_options.add_argument('--disable-cache')
        chrome_options.add_argument('--disable-offline-load-stale-cache')
        chrome_options.add_argument('--disk-cache-size=0')
        chrome_options.add_argument('--media-cache-size=0')
        
        driver = webdriver.Chrome(options=chrome_options)
        
        url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
        print(f"Start loading page: {url}")
        
        driver.get(url)
        sleep(5)  # 페이지 로딩 대기
        
        content = driver.page_source
        print("Successfully retrieved page content")
        
        driver.quit()
        return content

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

def main():
    try:
        for attempt in range(3):
            try:
                content = scrape_page()
                print(content)
                break
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"Attempt {attempt + 1} failed, retrying...")
                sleep(5)
                
    except Exception as e:
        print(f"Main function error: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Program interrupted by user")
    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")