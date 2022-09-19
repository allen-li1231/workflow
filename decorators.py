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
                and (("X-Hue-Middleware-Response" in res.headers
                      and res.headers["X-Hue-Middleware-Response"] == "LOGIN_REQUIRED")
                     or (res._content_consumed and "Unauthorized" in res.text)):
            self.login()
            return func(self, *args, **kwargs)
        return res

    return wrapper


def retry(module='', attempts: int = 3, wait_sec: int = 3):
    def retry_wrapper(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            logger = logging.getLogger(f"{module}{'.' if len(module) else ''}{func.__name__}")
            i = 1
            while i < attempts:
                try:
                    res = func(self, *args, **kwargs)
                    if isinstance(res, requests.models.Response) \
                            and res._content_consumed:
                        text = res.text if len(res.text) <= 250 else res.text[:250] + "..."
                        logger.debug(f"response {i}/{attempts} attempts: {text}")
                    else:
                        logger.debug(f"{i}/{attempts} attempts")

                except (KeyboardInterrupt, AssertionError, RuntimeError) as e:
                    raise e

                except Exception as e:
                    logger.warning(f"exception thrown in {i}/{attempts} attempts:")
                    logger.warning(e)
                    i += 1
                    time.sleep(wait_sec)
                    continue

                if not isinstance(res, requests.models.Response) \
                        or res.status_code in (200, 201, 204):
                    return res

                if isinstance(res, requests.models.Response) \
                        and res._content_consumed:
                    logger.warning(f"response error in {i}/{attempts} attempts: {text}")
                else:
                    logger.warning(f"return error in {i}/{attempts} attempts")
                if func.__name__ == "_fetch_result" \
                        and "Proxy Error" in res.text:
                    error_msg = "the proxy server is down. " \
                                "perhaps due to large result of sql query.\n" \
                                "please hold a while and retry " \
                                "by setting NotebookResult.rows_per_fetch smaller"
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
