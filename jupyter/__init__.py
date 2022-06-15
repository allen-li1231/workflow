import os
import base64
import urllib
import json
import requests

JUPYTER_URL = 'http://10.19.181.26:9999/'


def upload(token, file_path, dst_path):
    """
        Uploads File to Jupyter Notebook Server
        ----------------------------------------
        :param token:
            The authorization token issued by Jupyter for authentification
            (enabled by default as of version 4.3.0)
        :param file_path:
            The file path to the local content to be uploaded

        :param dst_path:
            The path where resource should be placed.
            The destination directory must exist.

        :return: server response

    """

    dst_path = urllib.quote(dst_path)
    dst_url = '%s/api/contents/%s' % (JUPYTER_URL, dst_path)
    file_name = os.path.basename(file_path)
    headers = {'Authorization': 'token ' + token}

    with open(file_path, 'r') as f:
        data = f.read()

    b64data = base64.encodebytes(data)
    body = json.dumps({
        'content': b64data,
        'name': file_name,
        'path': dst_path,
        'format': 'base64',
        'type': 'file'
        })
    return requests.put(dst_url, data=body, headers=headers, verify=True)
