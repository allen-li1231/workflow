import base64
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO, StringIO

import numpy as np
import pandas as pd
import requests
from PIL import Image
from requests_toolbelt import MultipartEncoder
import logging

from .settings import HUE_DOWNLOAD_BASE_URL
from .decorators import retry
from . import logger


class HueDownload(requests.Session):

    def __init__(self,
                 username: str = None,
                 password: str = None,
                 verbose: bool = False):
        self.base_url = HUE_DOWNLOAD_BASE_URL

        self.username = username
        self._password = password
        self.verbose = verbose
        self._set_log(verbose)

        self.log.debug("loading img_dict")
        self.benchmark_imgs = np.load(r"W:\Python3\Lib\site-packages\wx_custom\img_dict.npy", allow_pickle=True).item()
        super(HueDownload, self).__init__()

        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Referer": "http://10.19.185.103:8015/login?redirect=%2Fdashboard",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/76.0.3809.100 Safari/537.36"
        }

        self.login(self.username, self._password)

    def _set_log(self, verbose):
        self.log = logging.getLogger(__name__ + f".HueDownload")
        has_stream_handler = False
        for handler in self.log.handlers:
            if isinstance(handler, logging.StreamHandler):
                has_stream_handler = True
                if verbose:
                    handler.setLevel(logging.INFO)
                else:
                    handler.setLevel(logging.WARNING)

        if not has_stream_handler:
            if verbose:
                logger.setup_stdout_level(self.log, logging.INFO)
            else:
                logger.setup_stdout_level(self.log, logging.WARNING)

    @retry()
    def _login(self, username, password):
        login_url = self.base_url + "/auth/login"
        self.id_answer()
        form_data = dict(username=username,
                         password=password,
                         code=self.code,
                         uuid=self.uuid)

        res = self.post(login_url,
                        data=json.dumps(form_data))
        r = res.json()
        if "status" in r.keys():
            if r["status"] == 400 and r["message"] == "验证码错误":
                raise ConnectionError("captcha guess failed")

            self.log.exception(res.text)
            raise RuntimeError(r["message"])

        return r

    def login(self, username=None, password=None):
        self.is_logged_in = False
        self.username = username or self.username
        self._password = password or self._password
        if self.username is None and self._password is None:
            raise ValueError("please provide username and password")

        if self.username is None and self._password is not None:
            raise KeyError("username must be specified with password")

        if self.username is not None and self._password is None:
            print("Please provide Hue password:", end='')
            self._password = input("")

        self.log.debug(f"logging in for user [{self.username}]")
        r = self._login(self.username, self._password)

        self.log.info('login succeeful [%s] at %s'
                      % (self.username, self.base_url))
        self.is_logged_in = True
        self.headers["Authorization"] = "Bearer " + r["token"]

    def get_column(self, table_name):
        url = self.base_url + '/api/hive/getColumns?tableName=' + table_name
        r = requests.get(url, headers=self.headers)
        columns = pd.DataFrame(r.json())['name'].to_list()
        return columns

    def upload_data(self, file_path, reason, uploadColumnsInfo='1', uploadEncryptColumns=''):
        '''
            file_path  必填，需要上传文件位置
            reason 必填，上传事由
            uploadColumnsInfo 选填，默认写1，可用作备注，与上传数据无关
            uploadEncryptColumns 选填，默认'',需要加密的列，多个用逗号隔开
        '''
        self.headers['Referer'] = 'http://10.19.185.103:8015/ud/uploadInfo'
        file = (file_path, open(file_path, 'rb'))
        upload_info = {'reason': reason,
                       'uploadColumnsInfo': uploadColumnsInfo,
                       'uploadEncryptColumns': uploadEncryptColumns}

        if 'Authorization' not in self.headers.keys():
            self.login(self.username, self._password)
        url = self.base_url + '/api/uploadInfo/upload'
        if re.findall('\.csv$', file_path):
            upload_data = pd.read_csv(file_path)
        elif re.findall('\.xlsx$', file_path):
            upload_data = pd.read_excel(file_path)
        else:
            'data format is not supported yet! please upload csv or xlsx with english title'
        upload_info['uploadColumns'] = ','.join(upload_data.columns.tolist())
        upload_info['uploadRow'] = str(upload_data.shape[0])

        upload_info['file'] = file

        data = MultipartEncoder(fields=upload_info)
        self.headers['Content-Type'] = data.content_type

        r = requests.post(url, data=data, headers=self.headers)
        r = r.json()

        t_sec = 30
        t_try = 100
        t_tol = t_sec * t_try
        job_id = r['id']
        tag = 0
        for i in range(t_try):
            print('waiting %3d/%d...' %
                  (t_sec * i, t_tol))
            r = requests.get(self.base_url + '/api/uploadInfo?page=0&size=10&sort=id,desc', headers=self.headers)
            task_list = r.json()['content']
            for task in task_list:
                if task['id'] == job_id and task['status'] == 3:
                    tag = 1
                    table_name = task['rsTable']
                    break
            if tag == 1:
                break
            time.sleep(t_sec)
        return table_name

    def download_data(self, table_name, reason, col_info=' ', limit=None, columns=None, Decode_col=[]):
        '''
        table_name 必填，需要下载的表名
        reason 必填，下载事由
        col_info 选填,默认值' ',
        limit 选填，默认值None，下载条数不填则全部下载，最多10万行
        columns 选填，默认值None，不填则全部下载
        Decode_col 选填，默认值[]， 不填则不解密
        '''

        download_info = {}
        self.headers['Referer'] = 'http://10.19.185.103:8015/ud/downloadInf'
        self.headers['Content-Type'] = 'application/json'

        if 'Authorization' not in self.headers.keys():
            self.login(self.username, self._password)
        url = self.base_url + '/api/downloadInfo'

        if columns is None:
            columns = self.get_column(table_name)
            if len(columns) > 200:
                th = ThreadPoolExecutor(max_workers=3)
                results = []

                for i in range(0, int(len(columns) / 200) + 1):
                    start_num = i * 200
                    end_num = (i + 1) * 200 - 1
                    if end_num > len(columns) - 1:
                        end_num = len(columns)

                    temp_column = columns[start_num:end_num]
                    temp_reason = reason + ' part ' + str(i)
                    # print(temp_column)
                    results.append(th.submit(self.download_data, table_name, temp_reason, columns=temp_column))
                    # print('sub')
                cnt = 1
                # print('submited')
                for result in results:
                    temp_df = result.result()
                    if cnt == 1:
                        result_df = temp_df
                        cnt += 1

                    else:
                        result_df = pd.merge(result_df, temp_df, left_index=True, right_index=True)

                return result_df

        if limit is not None:
            download_info['downloadLimit'] = limit

        download_info['downloadTable'] = table_name
        download_info['downloadColumns'] = columns
        download_info['reason'] = reason

        if Decode_col is not None:
            download_info['downloadDecryptionColumns'] = Decode_col
        download_info['columnsInfo'] = col_info

        r = requests.post(url, data=json.dumps(download_info), headers=self.headers)
        r = r.json()
        # print(r)
        if r['status'] != 0:
            print(r['message'])
            return
        t_sec = 30
        t_try = 100
        t_tol = t_sec * t_try
        job_id = r['id']
        tag = 0
        for i in range(t_try):
            print('waiting %3d/%d...' %
                  (t_sec * i, t_tol) + '\r', end='')
            r = requests.get(self.base_url + '/api/downloadInfo?page=0&size=10&sort=id,desc', headers=self.headers)
            task_list = r.json()['content']
            for task in task_list:
                if task['id'] == job_id and task['status'] == 3:
                    tag = 1
                    break
            if tag == 1:
                break
            time.sleep(t_sec)
        if col_info == ' ':
            csv_header = 0
        else:
            csv_header = 1
        r = requests.get(self.base_url + '/api/downloadInfo/downloadData?id=' + str(job_id), headers=self.headers)
        r = pd.read_csv(StringIO(r.text), header=csv_header)
        return r

    def kill_app(self, app_id):
        """
        kill a YARN application

        :param app_id: str or string iterable
        :return: server response context
        """

        if isinstance(app_id, str):
            res = self._kill_app(app_id)
            r_json = res.json()
            if r_json["status"] != 1:
                raise RuntimeError(res.text)

        # let it fail if app_id is not iterable
        for app in app_id:
            res = self._kill_app(app)
            r_json = res.json()
            if r_json["status"] != 1:
                raise RuntimeError(res.text)

    @retry()
    def _kill_app(self, app_id: str):
        url = self.base_url + '/api/killJobHist'
        res = self.post(url, data=json.dumps({
            "appId": app_id,
            "createTime": "",
            "id": "",
            "ip": "",
            "reason": "",
            "status": "",
            "username": ""
        }))
        self.log.debug(f"_kill_app responds: {res.text}")
        return res

    def base64_pil(self):
        self.img = base64.b64decode(self.img)
        self.img = Image.open(BytesIO(self.img)).convert("L")
        self.img = np.array(self.img)

    def clear_edged(self, img):
        temp = np.sum(img, axis=0)
        # crop image, drop empty vertical pixels
        img = img[:, temp < img.shape[0]]
        return img

    def compare_img(self, imga, imgb):
        # 1 means a complete mismatch, 0 means perfect match
        score = 1.
        ax, ay = imga.shape
        bx, by = imgb.shape

        for i in range(0, abs(ay - by) + 1):
            if ay >= by:
                tmp_score = (imga[:, i:by + i] ^ imgb).sum() / (bx * by)
            else:
                tmp_score = (imga ^ imgb[:, i:ay + i]).sum() / (ax * ay)

            if tmp_score < score:
                score = tmp_score

        return score

    def match_img(self, img):
        score = 1
        result = -1
        for i, benchmark_img in self.benchmark_imgs.items():
            tmp_score = self.compare_img(img, benchmark_img)
            if tmp_score < score:
                score = tmp_score
                result = i
        return result

    @retry()
    def get_img(self):
        code_url = self.base_url + "/auth/code"
        code = self.get(code_url).json()
        self.img = re.sub("data:image/png;base64,", "", code["img"]).replace("%0A", "\n")
        self.uuid = code["uuid"]

    def id_answer(self):
        self.get_img()
        self.base64_pil()
        self.img[self.img <= 180] = 0
        self.img[self.img > 180] = 1
        p1 = self.clear_edged(self.img[:, :24])
        p2 = self.clear_edged(self.img[:, 25:50])
        p3 = self.clear_edged(self.img[:, 51:70])

        num1 = int(self.match_img(p1))
        method = self.match_img(p2)
        num2 = int(self.match_img(p3))

        if method == "+":
            answer = num1 + num2
        elif method == "-":
            answer = num1 - num2
        elif method == "x":
            answer = num1 * num2

        self.code = answer
