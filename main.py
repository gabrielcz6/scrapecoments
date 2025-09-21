from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import hashlib
import time
import random
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import concurrent.futures
from threading import Lock
import uvicorn
import os

app = FastAPI(title="TikTok Anti-Captcha Scraper", version="3.0.0")

class VideoRequest(BaseModel):
    url: str

class TikTokCommentScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.screenshot_counter = 0
        self.extraction_lock = Lock()
        self.setup_driver()

    def setup_driver(self):
        """Driver optimizado para evitar captcha"""
        chrome_options = Options()
        
        # Configuración anti-detección agresiva
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        # Anti-captcha específico
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-sync")
        chrome_options.add_argument("--disable-translate")
        chrome_options.add_argument("--hide-scrollbars")
        chrome_options.add_argument("--metrics-recording-only")
        chrome_options.add_argument("--mute-audio")
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--safebrowsing-disable-auto-update")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        # Headers realistas
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Para Docker
        if self.headless:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-gpu")

        # Prefs anti-captcha
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.images": 2,  # Bloquear imágenes
            "profile.default_content_setting_values.plugins": 2,
            "profile.managed_default_content_settings.plugins": 2,
            "profile.managed_default_content_settings.popups": 2,
            "profile.managed_default_content_settings.geolocation": 2,
            "profile.managed_default_content_settings.media_stream": 2,
        }
        chrome_options.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Chrome(options=chrome_options)
        
        # Configuración de timeouts
        self.driver.set_page_load_timeout(15)
        self.driver.implicitly_wait(3)
        
        # Script anti-detección
        self.driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            window.chrome = {runtime: {}};
        """)

    def take_screenshot(self, name):
        """Screenshot silencioso"""
        try:
            self.screenshot_counter += 1
            os.makedirs("/app/screenshots", exist_ok=True)
            path = f"/app/screenshots/{self.screenshot_counter:02d}_{name}.png"
            self.driver.save_screenshot(path)
            return path
        except:
            return None

    def detect_and_handle_captcha(self):
        """Detectar y manejar captcha"""
        try:
            page_source = self.driver.page_source.lower()
            
            # Detectar captcha
            captcha_indicators = [
                "captcha", "verify", "puzzle", "slider", "arrastra",
                "human verification", "security check"
            ]
            
            has_captcha = any(indicator in page_source for indicator in captcha_indicators)
            
            if has_captcha:
                self.take_screenshot("captcha_detected")
                
                # Intentar cerrar modal de captcha
                close_selectors = [
                    "button[aria-label='Close']",
                    "button.close",
                    "[data-testid='close']",
                    ".close-button",
                    "div[role='button'][aria-label='Close']"
                ]
                
                for selector in close_selectors:
                    try:
                        close_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if close_btn.is_displayed():
                            self.driver.execute_script("arguments[0].click();", close_btn)
                            time.sleep(1)
                            break
                    except:
                        continue
                
                return True
            return False
            
        except:
            return False

    def rapid_extraction(self):
        """Extracción inmediata sin scrolling"""
        comments = []
        
        # Buscar elementos inmediatamente
        selectors = [
            "[data-e2e^='comment-level-']",
            "[data-e2e='comment-item']"
        ]
        
        best_elements = []
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if len(elements) > len(best_elements):
                    best_elements = elements
                    if len(elements) > 0:
                        break
            except:
                continue
        
        if not best_elements:
            return []
        
        # Extracción paralela rápida
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            for i, element in enumerate(best_elements[:50]):  # Máximo 50
                future = executor.submit(self.extract_single_comment, element, i)
                futures.append(future)
            
            for future in concurrent.futures.as_completed(futures, timeout=5):
                try:
                    comment_data = future.result(timeout=1)
                    if comment_data:
                        comments.append(comment_data)
                except:
                    continue
        
        return comments

    def extract_single_comment(self, element, index):
        """Extracción individual rápida"""
        try:
            with self.extraction_lock:
                try:
                    parent = element.find_element(By.XPATH, "..")
                    full_text = parent.text
                except:
                    full_text = element.text
                
                if not full_text or len(full_text) < 10:
                    return None
                
                lines = full_text.replace('\n· Creator', '').split("\n")
                if len(lines) < 3:
                    return None
                
                return {
                    "index": index,
                    "username": lines[0] if len(lines) > 0 else "",
                    "text": lines[1] if len(lines) > 1 else "",
                    "date": lines[2] if len(lines) > 2 else "",
                    "likes": lines[4] if len(lines) > 4 else "0",
                    "id": hashlib.md5(full_text.encode("utf-8")).hexdigest()[:12],
                }
        except:
            return None

    def get_video_info(self):
        """Metadata básica"""
        info = {}
        selectors = {
            "title": "[data-e2e='browse-video-desc']",
            "author": "[data-e2e='browse-username']",
        }
        
        for field, selector in selectors.items():
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, selector)
                info[field] = element.text.strip()
            except:
                info[field] = ""
        
        return info

    def extract_comments(self, video_url):
        """Método principal ultra-rápido anti-captcha"""
        start_time = time.time()
        
        try:
            # Cargar página rápido
            self.driver.get(video_url)
            self.take_screenshot("01_loaded")
            
            # Espera mínima
            time.sleep(1.5)
            
            # Detectar captcha inmediatamente
            if self.detect_and_handle_captcha():
                self.take_screenshot("02_captcha_handled")
                time.sleep(1)
            
            # Metadata rápida
            video_data = self.get_video_info()
            
            # Extracción inmediata
            comments = self.rapid_extraction()
            
            # Screenshot final
            self.take_screenshot("03_completed")
            
            # Eliminar duplicados
            seen_ids = set()
            unique_comments = []
            for comment in comments:
                comment_id = comment.get('id')
                if comment_id and comment_id not in seen_ids:
                    seen_ids.add(comment_id)
                    unique_comments.append(comment)
            
            return {
                "url": video_url,
                "scraped_at": datetime.now().isoformat(),
                "video_metadata": video_data,
                "comments": unique_comments,
                "total_comments_scraped": len(unique_comments),
                "extraction_time_seconds": round(time.time() - start_time, 2),
                "screenshots_path": "/app/screenshots/",
                "success": True
            }
            
        except Exception as e:
            self.take_screenshot("error")
            return {
                "url": video_url,
                "error": str(e),
                "comments": [],
                "success": False,
                "screenshots_path": "/app/screenshots/"
            }

    def close(self):
        if hasattr(self, "driver"):
            self.driver.quit()


@app.get("/")
async def root():
    return {"message": "TikTok Anti-Captcha Scraper", "status": "running"}


@app.post("/scrape-comments/")
async def scrape_comments(request: VideoRequest):
    """Endpoint anti-captcha"""
    scraper = None
    try:
        if not request.url or "tiktok.com" not in request.url:
            raise HTTPException(status_code=400, detail="Invalid TikTok URL")
        
        scraper = TikTokCommentScraper(headless=True)
        result = scraper.extract_comments(request.url)
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)