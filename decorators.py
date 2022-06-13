import logging
import time
from functools import wraps


def ensure_login(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.is_logged_in:
            logger = logging.getLogger(func.__name__)
            logger.exception(f"notebook not logged in while calling {func.__name__}")
            self.login()

        res = func(self, *args, **kwargs)
        if "X-Hue-Middleware-Response" in res.headers \
            and res.headers["X-Hue-Middleware-Response"] == "LOGIN_REQUIRED":
            self.login()
        return res

    return wrapper


def ensure_active_session(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if time.perf_counter() - self._session_time >= 3600.:
            logger = logging.getLogger(func.__name__)
            logger.exception(f"notebook session expired while calling {func.__name__}")
            self._prepare_notebook()

        return func(self, *args, **kwargs)

    return wrapper

def retry(attempts: int = 3, wait_sec: int = 3):
    def retry_wrapper(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            logger = logging.getLogger(func.__name__)
            i = 1
            while i < attempts:
                try:
                    res = func(self, *args, **kwargs)
                    text = res.text if len(res.text) <= 250 else res.text[:250] + "..."
                    logger.debug(f"response {i}/{attempts} attempts: {text}")
                    if res.status_code == 200:
                        return res
                    else:
                        logger.warning(f"response error in {i}/{attempts} attempts: {text}")
                        time.sleep(wait_sec)
                except Exception as e:
                    logger.warning(f"exception thrown in {i}/{attempts} attempts:")
                    logger.warning(e)
                    i += 1
                    time.sleep(wait_sec)
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logger.exception(e)
                raise e

        return wrapper

    return retry_wrapper
