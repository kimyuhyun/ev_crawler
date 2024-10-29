import asyncio
from pyppeteer import launch
from bs4 import BeautifulSoup
import time
from datetime import datetime
import os

async def setup_browser():
   browser_args = [
       '--no-sandbox',
       '--disable-setuid-sandbox',
       '--disable-dev-shm-usage',
       '--disable-gpu',
       '--disable-extensions',
       '--disable-software-rasterizer',
       '--disable-plugins', 
       '--disable-default-apps',
       '--disable-translate',
       '--disable-features=TranslateUI',
       '--disable-features=IsolateOrigins,site-per-process',
       '--disable-blink-features=AutomationControlled',
       '--disable-application-cache',
       '--disable-sync',
       '--no-first-run',
       '--no-default-browser-check',
       '--single-process',
       '--memory-pressure-off',
       '--window-size=1280,720',
       '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
   ]
   
   return await launch(
       headless=True,
       args=browser_args,
       ignoreHTTPSErrors=True,
       handleSIGINT=False,
       handleSIGTERM=False,
       handleSIGHUP=False,
       executablePath='/usr/bin/chromium-browser',
       options={
           'protocolTimeout': 240000,
       }
   )

async def scrape_page(retry_count=3):
   browser = None
   for attempt in range(retry_count):
       print(f"Attempt {attempt + 1}/{retry_count}")
       
       browser = await setup_browser()
       page = await browser.newPage()
       
       await page.setRequestInterception(True)
       
       async def intercept(request):
           if request.resourceType in ['image', 'stylesheet', 'font']:
               await request.abort()
           else:
               await request.continue_()
       
       page.on('request', lambda req: asyncio.ensure_future(intercept(req)))
       
       url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
       print(f"Loading page: {url}")
       
       await page.goto(url, {
           'waitUntil': 'networkidle0',
           'timeout': 30000
       })
       
       content = await page.content()
       soup = BeautifulSoup(content, 'html.parser')
       
       timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
       filename = f'ev_content_{timestamp}.html'
       
       with open(filename, 'w', encoding='utf-8') as f:
           f.write(str(soup.prettify()))
       
       print(f"Content saved to: {filename}")
       print(soup.prettify())
       
       if browser:
           await browser.close()
           
       return True

       if attempt < retry_count - 1:
           print(f"Retrying in 5 seconds...")
           await asyncio.sleep(5)

async def main():
   success = await scrape_page()
   
   if success:
       print("Scraping completed successfully")
   else:
       print("All attempts failed")

if __name__ == "__main__":
   os.system('sync; echo 1 > /proc/sys/vm/drop_caches')
   
   loop = asyncio.get_event_loop()
   loop.run_until_complete(main())
   loop.close()