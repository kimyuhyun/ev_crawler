import asyncio
from playwright.async_api import async_playwright

async def scrape_page():
   try:
       async with async_playwright() as p:
           browser = await p.webkit.launch(
               headless=True,
               args=['--no-startup-window']
           )
           
           context = await browser.new_context(
               user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
               viewport={'width': 1280, 'height': 720}
           )
           
           page = await context.new_page()
           
           url = "https://ev.or.kr/nportal/buySupprt/initSubsidyPaymentCheckAction.do"
           print(f"Start loading page: {url}")
           
           await page.goto(url, wait_until="domcontentloaded", timeout=60000)
           
           try:
               await page.wait_for_load_state('networkidle', timeout=60000)
           except Exception as e:
               print(f"Network idle wait timeout: {str(e)}")
           
           await asyncio.sleep(5)
           
           content = await page.content()
           print("Successfully retrieved page content")
           
           await browser.close()
           
           return content

   except Exception as e:
       print(f"Error occurred: {str(e)}")
       raise

async def main():
   try:
       for attempt in range(3):
           try:
               content = await scrape_page()
               print(content)
               break
           except Exception as e:
               if attempt == 2:
                   raise
               print(f"Attempt {attempt + 1} failed, retrying...")
               await asyncio.sleep(5)
               
   except Exception as e:
       print(f"Main function error: {str(e)}")
       raise

if __name__ == "__main__":
   try:
       asyncio.run(main())
   except KeyboardInterrupt:
       print("Program interrupted by user")
   except Exception as e:
       print(f"Unexpected error occurred: {str(e)}")