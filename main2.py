from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import hashlib
import time
import random
import asyncio
from datetime import datetime
import nodriver as uc
from typing import List, Dict, Any, Optional
import uvicorn
import os
from contextlib import asynccontextmanager
import aiofiles
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="TikTok NoDriver Anti-Captcha Scraper", version="5.0.0")

class VideoRequest(BaseModel):
    url: str

class BatchVideoRequest(BaseModel):
    urls: List[str]
    max_concurrent: int = 3

class BrowserPool:
    """Pool asíncrono de browsers NoDriver para máximo rendimiento"""
    
    def __init__(self, pool_size: int = 5, headless: bool = False):
        self.pool_size = pool_size
        self.headless = headless
        self.browsers: List[uc.Browser] = []
        self.available_browsers = asyncio.Queue(maxsize=pool_size)
        self.semaphore = asyncio.Semaphore(pool_size)
        self.initialized = False
        
    async def initialize(self):
        """Inicializar pool de browsers"""
        if self.initialized:
            return
         #   
        logger.info(f"Inicializando pool de {self.pool_size} browsers NoDriver...")
        
        for i in range(self.pool_size):
            try:
                browser = await self._create_browser(f"browser_{i}")
                self.browsers.append(browser)
                await self.available_browsers.put(browser)
                logger.info(f"Browser {i+1}/{self.pool_size} inicializado")
            except Exception as e:
                logger.error(f"Error creando browser {i}: {e}")
                
        self.initialized = True
        logger.info("Pool de browsers inicializado exitosamente")
    
    async def _create_browser(self, browser_id: str) -> uc.Browser:
        """Crear browser con configuración anti-detección optimizada"""
        
        # User agents realistas rotados
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        
        # Argumentos de browser optimizados para TikTok
        browser_args = [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            '--disable-background-networking',
            '--disable-extensions',
            '--disable-sync',
            '--disable-translate',
            '--hide-scrollbars',
            '--mute-audio',
            '--no-first-run',
            '--disable-ipc-flooding-protection',
            f'--user-agent={random.choice(user_agents)}',
            '--window-size=1920,1080'
        ]
        
        if self.headless:
            browser_args.extend(['--headless=new', '--disable-gpu'])
        
        # Crear browser con argumentos pasados directamente
        browser = await uc.start(
            headless=self.headless,
            browser_args=browser_args,
            sandbox=False
        )
        
        # Scripts anti-detección adicionales
        await self._setup_anti_detection(browser)
        
        return browser
    
    async def _setup_anti_detection(self, browser: uc.Browser):
        """Configurar scripts anti-detección en el browser"""
        try:
            # Obtener la pestaña principal
            tab = browser.main_tab
            
            # Script anti-detección avanzado
            anti_detection_script = """
            // Ocultar webdriver
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            
            // Simular plugins realistas
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Idiomas realistas
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en', 'es']
            });
            
            // Chrome runtime
            window.chrome = {
                runtime: {},
                csi: function() {},
                loadTimes: function() {}
            };
            
            // Eliminar rastros de automatización
            delete window.__webdriver_evaluate;
            delete window.__selenium_evaluate;
            delete window.__webdriver_script_function;
            delete window.__webdriver_script_func;
            delete window.__webdriver_script_fn;
            delete window.__fxdriver_evaluate;
            delete window.__driver_unwrapped;
            delete window.__webdriver_unwrapped;
            delete window.__driver_evaluate;
            delete window.__selenium_unwrapped;
            delete window.__fxdriver_unwrapped;
            
            // Permisos simulados
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // Viewport y screen consistency
            Object.defineProperty(window, 'outerHeight', {
                get: () => window.innerHeight
            });
            Object.defineProperty(window, 'outerWidth', {
                get: () => window.innerWidth
            });
            """
            
            await tab.evaluate(anti_detection_script)
            
        except Exception as e:
            logger.warning(f"Error configurando anti-detección: {e}")
    
    @asynccontextmanager
    async def get_browser(self):
        """Context manager para obtener y devolver browser del pool"""
        await self.semaphore.acquire()
        browser = None
        try:
            # Esperar hasta 10 segundos por un browser disponible
            browser = await asyncio.wait_for(
                self.available_browsers.get(), 
                timeout=10.0
            )
            yield browser
        except asyncio.TimeoutError:
            # Si no hay browsers disponibles, crear uno temporal
            logger.warning("Pool agotado, creando browser temporal")
            browser = await self._create_browser("temp_browser")
            yield browser
        finally:
            if browser:
                try:
                    # Limpiar estado del browser
                    await self._clean_browser_state(browser)
                    
                    # Solo devolver al pool si no es temporal
                    if browser in self.browsers:
                        await self.available_browsers.put(browser)
                    else:
                        # Cerrar browser temporal
                        await browser.stop()
                except Exception as e:
                    logger.error(f"Error limpiando browser: {e}")
            
            self.semaphore.release()
    
    async def _clean_browser_state(self, browser: uc.Browser):
        """Limpiar estado del browser para reutilización"""
        try:
            tab = browser.main_tab
            
            # Limpiar storage
            await tab.evaluate("""
                try {
                    localStorage.clear();
                    sessionStorage.clear();
                } catch(e) {
                    console.log('Error limpiando storage:', e);
                }
            """)
            
            # Cerrar pestañas extras (mantener solo la principal)
            for tab_obj in browser.tabs:
                if tab_obj != browser.main_tab:
                    try:
                        await tab_obj.close()
                    except:
                        pass
                        
        except Exception as e:
            logger.warning(f"Error limpiando estado: {e}")
    
    async def cleanup(self):
        """Limpiar todos los browsers del pool"""
        logger.info("Cerrando pool de browsers...")
        for browser in self.browsers:
            try:
                await browser.stop()
            except Exception as e:
                logger.error(f"Error cerrando browser: {e}")
        self.browsers.clear()

# Pool global de browsers
browser_pool = BrowserPool(pool_size=5, headless=True)

class NoDriverTikTokScraper:
    """Scraper de TikTok usando NoDriver con máxima eficiencia"""
    
    def __init__(self):
        self.screenshot_counter = 0
        
    async def take_screenshot(self, tab, name: str, request_id: str = ""):
        """Screenshot asíncrono thread-safe"""
        try:
            self.screenshot_counter += 1
            os.makedirs("/app/screenshots", exist_ok=True)
            filename = f"/app/screenshots/{request_id}_{self.screenshot_counter:02d}_{name}.png"
            await tab.save_screenshot(filename)
            return filename
        except Exception as e:
            logger.warning(f"Error guardando screenshot: {e}")
            return None
    
    async def detect_and_handle_captcha(self, tab) -> bool:
        """Detectar y manejar captcha de forma asíncrona"""
        try:
            # Obtener contenido de la página
            page_content = await tab.get_content()
            page_text = page_content.lower()
            
            # Indicadores de captcha
            captcha_indicators = [
                "captcha", "verify", "puzzle", "slider", "arrastra",
                "human verification", "security check", "prove you're human",
                "verify that you are human", "complete the security check"
            ]
            
            has_captcha = any(indicator in page_text for indicator in captcha_indicators)
            
            if has_captcha:
                logger.info("Captcha detectado, intentando cerrar modal")
                
                # Selectores para cerrar modal de captcha
                close_selectors = [
                    "button[aria-label='Close']",
                    "button[aria-label='Cerrar']", 
                    "button.close",
                    "[data-testid='close']",
                    ".close-button",
                    "div[role='button'][aria-label='Close']",
                    ".modal-close",
                    "[data-e2e='modal-close-btn']"
                ]
                
                for selector in close_selectors:
                    try:
                        elements = await tab.select_all(selector)
                        if elements:
                            for element in elements:
                                try:
                                    await element.click()
                                    await asyncio.sleep(1)
                                    logger.info(f"Modal cerrado con selector: {selector}")
                                    return True
                                except:
                                    continue
                    except:
                        continue
                
                # Si no se pudo cerrar, intentar presionar ESC
                try:
                    await tab.evaluate("""
                        document.dispatchEvent(new KeyboardEvent('keydown', {
                            key: 'Escape',
                            code: 'Escape',
                            keyCode: 27
                        }));
                    """)
                    await asyncio.sleep(0.5)
                    logger.info("ESC enviado para cerrar captcha")
                except:
                    pass
                    
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error detectando captcha: {e}")
            return False
    
    async def rapid_extraction(self, tab) -> List[Dict]:
        """Extracción rápida de comentarios con procesamiento paralelo"""
        comments = []
        
        try:
            # Selectores optimizados para comentarios de TikTok
            comment_selectors = [
                "[data-e2e^='comment-level-']",
                "[data-e2e='comment-item']",
                "[data-e2e='comment-container']",
                ".comment-item",
                "[data-testid='comment']"
            ]
            
            best_elements = []
            
            # Buscar el mejor conjunto de elementos
            for selector in comment_selectors:
                try:
                    elements = await tab.select_all(selector)
                    if len(elements) > len(best_elements):
                        best_elements = elements
                        if len(elements) > 0:
                            logger.info(f"Encontrados {len(elements)} comentarios con selector: {selector}")
                            break
                except:
                    continue
            
            if not best_elements:
                logger.warning("No se encontraron comentarios")
                return []
            
            # Procesar comentarios en paralelo usando tasks
            tasks = []
            semaphore = asyncio.Semaphore(10)  # Limitar concurrencia
            
            for i, element in enumerate(best_elements[:100]):  # Límite de 100 comentarios
                task = asyncio.create_task(
                    self._extract_single_comment_async(element, i, semaphore)
                )
                tasks.append(task)
            
            # Esperar todos los tasks con timeout
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=15.0
                )
                
                # Filtrar resultados válidos
                for result in results:
                    if isinstance(result, dict) and result.get('text'):
                        comments.append(result)
                        
            except asyncio.TimeoutError:
                logger.warning("Timeout en extracción de comentarios")
                
        except Exception as e:
            logger.error(f"Error en extracción rápida: {e}")
        
        return comments
    
    async def _extract_single_comment_async(self, element, index: int, semaphore) -> Optional[Dict]:
        """Extraer un comentario individual de forma asíncrona"""
        async with semaphore:
            try:
                # Obtener texto del elemento
                text_content = await element.text
                
                if not text_content or len(text_content.strip()) < 5:
                    return None
                
                # Limpiar y procesar texto
                lines = text_content.replace('\n· Creator', '').strip().split('\n')
                lines = [line.strip() for line in lines if line.strip()]
                
                if len(lines) < 2:
                    return None
                
                # Extraer información del comentario
                username = lines[0] if lines else ""
                comment_text = lines[1] if len(lines) > 1 else ""
                date_info = lines[2] if len(lines) > 2 else ""
                likes_info = ""
                
                # Buscar información de likes en las líneas
                for line in lines[2:]:
                    if any(indicator in line.lower() for indicator in ['like', 'heart', '❤️', '♥️']):
                        likes_info = line
                        break
                
                # Generar ID único
                comment_id = hashlib.md5(
                    f"{username}_{comment_text}_{index}".encode('utf-8')
                ).hexdigest()[:12]
                
                return {
                    "index": index,
                    "username": username,
                    "text": comment_text,
                    "date": date_info,
                    "likes": likes_info,
                    "id": comment_id,
                    "extracted_at": datetime.now().isoformat()
                }
                
            except Exception as e:
                logger.debug(f"Error extrayendo comentario {index}: {e}")
                return None
    
    async def get_video_metadata(self, tab) -> Dict:
        """Obtener metadata del video de forma asíncrona"""
        metadata = {}
        
        # Selectores para metadata de TikTok
        selectors = {
            "title": [
                "[data-e2e='browse-video-desc']",
                "[data-e2e='video-desc']",
                "h1[data-e2e='browse-video-desc']",
                ".video-meta-title"
            ],
            "author": [
                "[data-e2e='browse-username']", 
                "[data-e2e='video-author-uniqueid']",
                ".author-uniqueid",
                "h2[data-e2e='browse-username']"
            ],
            "view_count": [
                "[data-e2e='video-view-count']",
                ".video-count"
            ],
            "like_count": [
                "[data-e2e='like-count']",
                "[data-e2e='browse-like-count']"
            ]
        }
        
        for field, field_selectors in selectors.items():
            for selector in field_selectors:
                try:
                    elements = await tab.select_all(selector)
                    if elements:
                        text = await elements[0].text
                        if text.strip():
                            metadata[field] = text.strip()
                            break
                except:
                    continue
            
            # Valor por defecto si no se encuentra
            if field not in metadata:
                metadata[field] = ""
        
        return metadata
    
    async def extract_comments_single(self, video_url: str, request_id: str = "") -> Dict:
        """Extraer comentarios de un video individual usando NoDriver"""
        start_time = time.time()
        
        async with browser_pool.get_browser() as browser:
            try:
                tab = browser.main_tab
                
                logger.info(f"Procesando video: {video_url}")
                
                # Navegar a la URL
                await tab.get(video_url)
                await self.take_screenshot(tab, "loaded", request_id)
                
                # Espera inteligente con jitter anti-bot
                await asyncio.sleep(random.uniform(1.5, 3.0))
                
                # Detectar y manejar captcha
                captcha_detected = await self.detect_and_handle_captcha(tab)
                if captcha_detected:
                    await self.take_screenshot(tab, "captcha_handled", request_id)
                    await asyncio.sleep(random.uniform(1.0, 2.0))
                
                # Scroll suave para cargar comentarios
                await self._smart_scroll_for_comments(tab)
                
                # Extraer metadata y comentarios en paralelo
                metadata_task = asyncio.create_task(self.get_video_metadata(tab))
                comments_task = asyncio.create_task(self.rapid_extraction(tab))
                
                try:
                    video_metadata, comments = await asyncio.gather(
                        metadata_task, 
                        comments_task,
                        return_exceptions=True
                    )
                    
                    # Manejar excepciones en tasks
                    if isinstance(video_metadata, Exception):
                        logger.warning(f"Error en metadata: {video_metadata}")
                        video_metadata = {}
                    
                    if isinstance(comments, Exception):
                        logger.warning(f"Error en comentarios: {comments}")
                        comments = []
                        
                except Exception as e:
                    logger.error(f"Error en extracción paralela: {e}")
                    video_metadata = {}
                    comments = []
                
                await self.take_screenshot(tab, "completed", request_id)
                
                # Eliminar duplicados
                unique_comments = self._remove_duplicates(comments)
                
                execution_time = round(time.time() - start_time, 2)
                
                result = {
                    "url": video_url,
                    "scraped_at": datetime.now().isoformat(),
                    "video_metadata": video_metadata,
                    "comments": unique_comments,
                    "total_comments_scraped": len(unique_comments),
                    "extraction_time_seconds": execution_time,
                    "request_id": request_id,
                    "success": True,
                    "captcha_detected": captcha_detected
                }
                
                logger.info(f"Extracción exitosa: {len(unique_comments)} comentarios en {execution_time}s")
                return result
                
            except Exception as e:
                logger.error(f"Error procesando {video_url}: {e}")
                await self.take_screenshot(tab, "error", request_id)
                
                return {
                    "url": video_url,
                    "error": str(e),
                    "comments": [],
                    "success": False,
                    "request_id": request_id,
                    "extraction_time_seconds": round(time.time() - start_time, 2)
                }
    
    async def _smart_scroll_for_comments(self, tab):
        """Scroll inteligente para cargar comentarios"""
        try:
            # Scroll gradual para simular comportamiento humano
            for _ in range(3):
                await tab.scroll_down(300)
                await asyncio.sleep(random.uniform(0.5, 1.0))
                
            # Scroll hasta la sección de comentarios
            await tab.evaluate("""
                const commentsSection = document.querySelector('[data-e2e="comment-list"]') || 
                                      document.querySelector('.comment-list') ||
                                      document.querySelector('[id*="comment"]');
                if (commentsSection) {
                    commentsSection.scrollIntoView({behavior: 'smooth'});
                }
            """)
            
            await asyncio.sleep(1.0)
            
        except Exception as e:
            logger.warning(f"Error en scroll inteligente: {e}")
    
    def _remove_duplicates(self, comments: List[Dict]) -> List[Dict]:
        """Eliminar comentarios duplicados de forma eficiente"""
        seen_ids = set()
        unique_comments = []
        
        for comment in comments:
            comment_id = comment.get('id')
            if comment_id and comment_id not in seen_ids:
                seen_ids.add(comment_id)
                unique_comments.append(comment)
        
        return unique_comments
    
    async def extract_comments_batch(self, video_urls: List[str], max_concurrent: int = 3) -> List[Dict]:
        """Extraer comentarios de múltiples videos concurrentemente"""
        
        # Limitar concurrencia según el pool de browsers
        max_concurrent = min(max_concurrent, browser_pool.pool_size)
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_single_url(url: str, index: int) -> Dict:
            async with semaphore:
                request_id = f"batch_{index}_{int(time.time())}"
                return await self.extract_comments_single(url, request_id)
        
        # Crear tasks para todas las URLs
        tasks = [
            process_single_url(url, i) 
            for i, url in enumerate(video_urls)
        ]
        
        # Ejecutar con timeout global
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=300.0  # 5 minutos timeout total
            )
            
            # Procesar resultados y manejar excepciones
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    processed_results.append({
                        "url": video_urls[i],
                        "error": str(result),
                        "success": False,
                        "extraction_time_seconds": 0
                    })
                else:
                    processed_results.append(result)
            
            return processed_results
            
        except asyncio.TimeoutError:
            logger.error("Timeout en procesamiento batch")
            return [
                {
                    "url": url,
                    "error": "Batch timeout exceeded",
                    "success": False
                } for url in video_urls
            ]

# Instancia global del scraper
scraper = NoDriverTikTokScraper()

@app.on_event("startup")
async def startup_event():
    """Inicializar recursos al startup"""
    logger.info("Iniciando TikTok NoDriver Scraper...")
    await browser_pool.initialize()
    logger.info("Scraper listo!")

@app.on_event("shutdown")
async def shutdown_event():
    """Limpiar recursos al shutdown"""
    logger.info("Cerrando scraper...")
    await browser_pool.cleanup()
    logger.info("Recursos liberados")

@app.get("/")
async def root():
    return {
        "message": "TikTok NoDriver Anti-Captcha Scraper",
        "status": "running",
        "version": "5.0.0",
        "browser_pool_initialized": browser_pool.initialized,
        "available_browsers": browser_pool.available_browsers.qsize() if browser_pool.initialized else 0
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "browser_pool_size": browser_pool.pool_size,
        "available_browsers": browser_pool.available_browsers.qsize() if browser_pool.initialized else 0,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/scrape-comments/")
async def scrape_comments(request: VideoRequest):
    """Endpoint para scraping individual con NoDriver"""
    try:
        if not request.url or "tiktok.com" not in request.url:
            raise HTTPException(status_code=400, detail="Invalid TikTok URL")
        
        if not browser_pool.initialized:
            raise HTTPException(status_code=503, detail="Browser pool not initialized")
        
        # Generar ID único para el request
        request_id = f"single_{int(time.time())}_{random.randint(1000, 9999)}"
        
        result = await scraper.extract_comments_single(request.url, request_id)
        return result
        
    except Exception as e:
        logger.error(f"Error en scrape individual: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scrape-comments-batch/")
async def scrape_comments_batch(request: BatchVideoRequest):
    """Endpoint para scraping concurrente de múltiples videos"""
    try:
        if not request.urls:
            raise HTTPException(status_code=400, detail="No URLs provided")
        
        # Validar URLs
        invalid_urls = [url for url in request.urls if "tiktok.com" not in url]
        if invalid_urls:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid TikTok URLs: {invalid_urls}"
            )
        
        if len(request.urls) > 20:
            raise HTTPException(
                status_code=400, 
                detail="Maximum 20 URLs per batch"
            )
        
        if not browser_pool.initialized:
            raise HTTPException(status_code=503, detail="Browser pool not initialized")
        
        start_time = time.time()
        
        results = await scraper.extract_comments_batch(
            request.urls,
            max_concurrent=min(request.max_concurrent, browser_pool.pool_size)
        )
        
        # Estadísticas del batch
        successful = sum(1 for r in results if r.get('success', False))
        total_comments = sum(r.get('total_comments_scraped', 0) for r in results)
        total_time = round(time.time() - start_time, 2)
        
        return {
            "batch_results": results,
            "batch_statistics": {
                "total_urls": len(request.urls),
                "successful_extractions": successful,
                "failed_extractions": len(request.urls) - successful,
                "total_comments_extracted": total_comments,
                "batch_time_seconds": total_time,
                "max_concurrent_used": min(request.max_concurrent, browser_pool.pool_size),
                "average_time_per_url": round(total_time / len(request.urls), 2) if request.urls else 0
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error en scrape batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pool-status")
async def pool_status():
    """Endpoint para monitorear el estado del pool de browsers"""
    return {
        "pool_initialized": browser_pool.initialized,
        "pool_size": browser_pool.pool_size,
        "available_browsers": browser_pool.available_browsers.qsize() if browser_pool.initialized else 0,
        "total_browsers": len(browser_pool.browsers),
        "headless_mode": browser_pool.headless
    }

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        workers=1  # Importante: solo 1 worker para compartir pool
    )