import time, logging, requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type,before_sleep_log

log = logging.getLogger(__name__)

class ApiClient:
    def __init__(self, base_url: str, token: str, rate_limit_per_sec: float = 5.0):
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        # reqres.in uses x-api-key header
        self.session.headers.update({"x-api-key": token})
        self.min_interval = 1.0 / rate_limit_per_sec
        self._last_call: float = 0.0

    #---------------------------------------------------------------------------------
    # Internal helpers
    #---------------------------------------------------------------------------------    
        
    def _throttle(self) -> None:
        """Enfore rate limit between consecutive requests."""
        elapsed = time.monotonic() - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call = time.monotonic()
        
    @retry(stop=stop_after_attempt(5),
           wait=wait_exponential(multiplier=1, min=1, max=30),
           retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
           before_sleep=before_sleep_log(log, logging.WARNING),
           reraise=True,
           )
    def get(self, path: str, params: dict | None = None) -> dict:
        self._throttle()
        url = f"{self.base}/{path.lstrip('/')}"
        log.debug("GET %s params=%s", url, params)
        r = self.session.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    
    #---------------------------------------------------------------------------------
    # Pagination
    #---------------------------------------------------------------------------------

    def paginate(self, endpoint: str, page_param: str ="page", per_page_param: str ="per_page",
                 start_page: int =1, per_page: int =6, data_key="data"):
        """
        Yield individual records across all pages.
        stops when the API returns an empty data list or we exceed total_pages.
        """
        page = start_page
        while True:
            payload = self.get(
                endpoint, params={page_param:page, per_page_param: per_page}
            )
            records = payload.get(data_key, [])
            total_pages = payload.get("total_pages", 1)

            if not records:
                break
            
            log.info("Fetched page %d/%d (%d records)", page, total_pages, len(records))
            yield from records

            if page >= total_pages:
                break
            page += 1
    
    