import os
import base64
import urllib
import json
import requests

JUPYTER_URL = 'http://10.19.181.26:9999/'
TOKEN = "fengkong"


def upload(file_path, dst_path):
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

    dst_path = urllib.parse.quote(dst_path)
    dst_url = '%s/api/contents/%s' % (JUPYTER_URL, dst_path)
    file_name = os.path.basename(file_path)
    headers = {'Authorization': 'token ' + TOKEN}

    # TODO: upload large files in chunks
    # size_of_file = os.path.getsize(file_path) / 1024. / 1024.
    # if size_of_file > 10
    with open(file_path, 'r') as f:
        data = f.read()

    data = base64.encodebytes(data)
    body = json.dumps({
        'content': data,
        'name': file_name,
        'path': dst_path,
        'format': 'base64',
        'type': 'file'
        })
    return requests.put(dst_url, data=body, headers=headers, verify=True)


def _read_in_chunks(file_object, blocksize=1024 * 1024, chunks=-1):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while chunks:
        data = file_object.read(blocksize)
        if not data:
            break

        yield data
        chunks -= 1
