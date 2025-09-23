import time, logging, requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

log = logging.getLogger(__name__)

class ApiClient:
    def __init__(self, base_url: str, token: str, rate_limit_per_sec: float = 5.0):
        self.base = base_url.strip("/")
        self.session = requests.session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        self.min_interval = 1 / rate_limit_per_sec
        self.last = 0.0
        
    def _throttle(self):
        elapsed = time.time() - self._last
        if elapsed > self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.sleep()
        
    @retry(stop=stop_after_attempt(5),
           wait=wait_exponential(min=1, max=30),
           retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
           reraise=True)
    def get(self, path, param):
        self._throttle()
        r = self.session.get(f"{self.base}/{path.lstrip('/')}", params = params, timeout=30 )
        r.raise_for_status()
        return r.json()
    
    def paginate(self, endpoint, page_param="page", per_page_param="limit",
                 start_page=1, per_page=100, data_key="data"):
        page = start_page
        while True:
            payload = self.get(endpoint, {page_param:page, per_page_param: per_page})
            records = payload.get(data_key, [])
            if not records:
                break
            for rec in records:
                yield rec
            page += 1
    
    