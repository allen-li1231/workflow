import logging
import time
import requests
from functools import wraps


def ensure_login(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.is_logged_in:
            logger = logging.getLogger(func.__name__)
            logger.warning(f"notebook not logged in while calling {func.__name__}")
            self.login()

        res = func(self, *args, **kwargs)
        if isinstance(res, requests.models.Response) \
                and "X-Hue-Middleware-Response" in res.headers \
                and res.headers["X-Hue-Middleware-Response"] == "LOGIN_REQUIRED":
            self.login()
        return res

    return wrapper


def ensure_active_session(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if time.perf_counter() - self.session["last_used"] >= 600.:
            logger = logging.getLogger(func.__name__)
            logger.warning(f"notebook session expired while calling {func.__name__}")
            self.session["last_used"] = time.perf_counter()
            self._set_hive(self.hive_settings)

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

                except (KeyboardInterrupt, AssertionError, RuntimeError) as e:
                    raise e

                except Exception as e:
                    logger.warning(f"exception thrown in {i}/{attempts} attempts:")
                    logger.warning(e)
                    i += 1
                    time.sleep(wait_sec)
                    continue

                if res.status_code == 200:
                    return res

                logger.warning(f"response error in {i}/{attempts} attempts: {text}")
                if func.__name__ == "_fetch_result" \
                        and "Proxy Error" in res.text:
                    error_msg = "the proxy server is down. " \
                                "perhaps due to large result of sql query.\n" \
                                "please hold a while and retry " \
                                "by setting Notebook.rows_per_fetch smaller"
                    logger.exception(error_msg)
                    raise RuntimeError(error_msg)

                i += 1
                time.sleep(wait_sec)

            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logger.exception(e)
                raise e

        return wrapper

    return retry_wrapper
