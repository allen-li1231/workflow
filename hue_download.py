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


class Hue_download(requests.Session):
    BASE_URL = "http://10.19.185.103:8000"

    HEADER = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Referer": "http://10.19.185.103:8015/login?redirect=%2Fdashboard",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/76.0.3809.100 Safari/537.36"
    }

    def __init__(self, base_url=None, header=None):
        if base_url is None:
            self.base_url = self.BASE_URL
        else:
            self.base_url = base_url
        if header is None:
            self.header = self.HEADER
        else:
            self.header = header
        self.benchmark_imgs = np.load(r"W:\Python3\Lib\site-packages\wx_custom\img_dict.npy", allow_pickle=True).item()
        super(Hue_download, self).__init__()

    def login(self, username=None, password=None):
        if username is None:
            username = self.username
        if password is None:
            password = self.password
        self.header["Content-Type"] = "application/json"
        login_url = self.base_url + "/auth/login"
        try_cnt = 1
        while try_cnt < 3:
            try_cnt += 1
            self.id_answer()
            form_data = dict(username=username,
                             password=password,
                             code=self.code,
                             uuid=self.uuid)

            r = self.post(login_url,
                          data=json.dumps(form_data),
                          headers=self.header)
            r = r.json()
            if "status" in r.keys():
                if r["status"] == 400:
                    if r["message"] == "验证码错误":
                        print("验证码错误，继续尝试请按1")
                        time.sleep(3)
                    else:
                        print(r["message"])
                        try_cnt = 3
            try:
                self.header["Authorization"] = "Bearer " + r["token"]
                print("login succeeded for user [%s] at %s\n" %
                      (username, self.base_url))
                return "success"
            except Exception as e:
                print(e)
                try_cnt = 3

    def get_column(self, table_name):
        url = self.base_url + "/api/hive/getColumns?tableName=" + table_name
        r = requests.get(url, headers=self.header)
        columns = pd.DataFrame(r.json())["name"].to_list()
        return columns

    def upload_data(self, file_path, reason, uploadColumnsInfo="1", uploadEncryptColumns=""):
        """
            file_path  必填，需要上传文件位置
            reason 必填，上传事由
            uploadColumnsInfo 选填，默认写1，可用作备注，与上传数据无关
            uploadEncryptColumns 选填，默认"",需要加密的列，多个用逗号隔开
        """
        self.header["Referer"] = "http://10.19.185.103:8015/ud/uploadInfo"
        file = (file_path, open(file_path, "rb"))
        upload_info = {}
        upload_info["reason"] = reason
        upload_info["uploadColumnsInfo"] = uploadColumnsInfo
        upload_info["uploadEncryptColumns"] = uploadEncryptColumns  # 解密列

        if "Authorization" not in self.header.keys():
            self.login(self.username, self.password)
        url = self.base_url + "/api/uploadInfo/upload"
        if re.findall("\.csv$", file_path):
            upload_data = pd.read_csv(file_path)
        elif re.findall("\.xlsx$", file_path):
            upload_data = pd.read_excel(file_path)
        else:
            "data format is not supported yet! please upload csv or xlsx with english title"
        upload_info["uploadColumns"] = ",".join(upload_data.columns.tolist())
        upload_info["uploadRow"] = str(upload_data.shape[0])

        upload_info["file"] = file

        data = MultipartEncoder(fields=upload_info)
        self.header["Content-Type"] = data.content_type

        r = requests.post(url, data=data, headers=self.header)
        r = r.json()

        t_sec = 30
        t_try = 100
        t_tol = t_sec * t_try
        job_id = r["id"]
        tag = 0
        for i in range(t_try):
            print("waiting %3d/%d..." %
                  (t_sec * i, t_tol))
            r = requests.get(self.base_url + "/api/uploadInfo?page=0&size=10&sort=id,desc", headers=self.header)
            task_list = r.json()["content"]
            for task in task_list:
                if task["id"] == job_id and task["status"] == 3:
                    tag = 1
                    table_name = task["rsTable"]
                    break
            if tag == 1:
                break
            time.sleep(t_sec)
        return table_name

    def download_data(self, table_name, reason, col_info=" ", limit=None, columns=None, Decode_col=[]):
        """
        table_name 必填，需要下载的表名
        reason 必填，下载事由
        col_info 选填,默认值" ",
        limit 选填，默认值None，下载条数不填则全部下载，最多10万行
        columns 选填，默认值None，不填则全部下载
        Decode_col 选填，默认值[]， 不填则不解密
        """

        download_info = {}
        self.header["Referer"] = "http://10.19.185.103:8015/ud/downloadInf"
        self.header["Content-Type"] = "application/json"

        if "Authorization" not in self.header.keys():
            self.login(self.username, self.password)
        url = self.base_url + "/api/downloadInfo"

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
                    temp_reason = reason + " part " + str(i)
                    # print(temp_column)
                    results.append(th.submit(self.download_data, table_name, temp_reason, columns=temp_column))
                    # print("sub")
                cnt = 1
                # print("submited")
                for result in results:
                    temp_df = result.result()
                    if cnt == 1:
                        result_df = temp_df
                        cnt += 1

                    else:
                        result_df = pd.merge(result_df, temp_df, left_index=True, right_index=True)

                return result_df

        if limit is not None:
            download_info["downloadLimit"] = limit

        download_info["downloadTable"] = table_name
        download_info["downloadColumns"] = columns
        download_info["reason"] = reason

        if Decode_col is not None:
            download_info["downloadDecryptionColumns"] = Decode_col
        download_info["columnsInfo"] = col_info

        r = requests.post(url, data=json.dumps(download_info), headers=self.header)
        r = r.json()
        # print(r)
        if r["status"] != 0:
            print(r["message"])
            return
        t_sec = 30
        t_try = 100
        t_tol = t_sec * t_try
        job_id = r["id"]
        tag = 0
        for i in range(t_try):
            print("waiting %3d/%d..." %
                  (t_sec * i, t_tol) + "\r", end="")
            r = requests.get(self.base_url + "/api/downloadInfo?page=0&size=10&sort=id,desc", headers=self.header)
            task_list = r.json()["content"]
            for task in task_list:
                if task["id"] == job_id and task["status"] == 3:
                    tag = 1
                    break
            if tag == 1:
                break
            time.sleep(t_sec)
        if col_info == " ":
            csv_header = 0
        else:
            csv_header = 1
        r = requests.get(self.base_url + "/api/downloadInfo/downloadData?id=" + str(job_id), headers=self.header)
        r = pd.read_csv(StringIO(r.text), header=csv_header)
        return r

    def base64_pil(self):
        self.img = base64.b64decode(self.img)
        self.img = BytesIO(self.img)
        self.img = Image.open(self.img).convert("L")
        self.img = np.array(self.img)

    def clear_edged(self, img):
        temp = np.sum(img, axis=0)
        temp1 = np.sum(img, axis=1)
        img = img[:, temp < len(temp1)]
        return img

    def compare_img(self, imga, imgb):
        value = 1
        ax, ay = imga.shape
        bx, by = imgb.shape
        cal_v = lambda x, y: len(x[x == 1]) / y
        if ay == by:
            temp = imga + imgb
            value = cal_v(temp, ax * ay)
        else:
            for i in range(0, abs(ay - by) + 1):
                if ay > by:
                    temp = imga[:, i:by + i] + imgb
                    t_value = cal_v(temp, bx * by)
                    if t_value <= value:
                        value = t_value
                else:
                    temp = imga + imgb[:, i:ay + i]
                    t_value = cal_v(temp, ax * ay)
                    if t_value <= value:
                        value = t_value
        return value

    def match_img(self, img):
        value = 1
        for i in self.benchmark_imgs.keys():
            t_value = self.compare_img(img, self.benchmark_imgs[i])
            if t_value < value:
                value = t_value
                result = i
        return result

    def get_img(self):
        try:
            code_url = self.base_url + "/auth/code"
            code = self.get(code_url).json()
            self.img = re.sub("data:image/png;base64,", "", code["img"]).replace("%0A", "\n")
            self.uuid = code["uuid"]
        except:
            print("get image failed, waiting for 3 seconds")
            time.sleep(3)
            self.get_img()

    def id_answer(self):
        self.get_img()
        try:
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
        except:
            self.get_img()
            time.sleep(3)
            self.id_answer()
