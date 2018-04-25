# -*- coding: utf-8 -*-


from logging import getLogger
from migrate_tool import storage_service
from qcloud_cos import CosClient

from qcloud_cos import UploadFileRequest, StatFileRequest, ListFolderRequest, DownloadFileRequest

import os

from migrate_tool.task import Task
import urllib


def to_unicode(s):
    if isinstance(s, str):
        return s.decode('utf-8')
    else:
        return s


def to_utf8(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    else:
        return s


logger = getLogger(__name__)


class CosV4StorageService(storage_service.StorageService):

    def __init__(self, *args, **kwargs):

        appid = int(kwargs['appid'])
        region = kwargs['region']
        accesskeyid = unicode(kwargs['accesskeyid'])
        accesskeysecret = unicode(kwargs['accesskeysecret'])
        bucket = unicode(kwargs['bucket'])
        if 'prefix_dir' in kwargs:
            self._prefix_dir = kwargs['prefix_dir']
        else:
            self._prefix_dir = None

        # filelist存在时, prefix_dir不生效
        if 'filelist' in kwargs:
            self._filelist = kwargs['filelist']
            self._prefix_dir = None
        else:
            self._filelist = None

        if 'sync_files' in kwargs:
            self._sync_files_dir = kwargs['sync_files']
        else:
            self._sync_files_dir = '/tmp/workspace/sync_files'

        self._cos_api = CosClient(appid, accesskeyid, accesskeysecret, region=region)
        self._bucket = bucket
        self._overwrite = kwargs['overwrite'] == 'true' if 'overwrite' in kwargs else False
        self._max_retry = 20

    def download(self, task, local_path):
        return self._download(task, local_path, False)


    def _download(self, task, local_path, is_ignore_size):
        for i in range(20):
            logger.info("download file with rety {0}".format(i))
            import os
            try:
                os.remove(task.key)
            except:
                pass
            req = DownloadFileRequest(self._bucket, task.key, local_path)
            ret = self._cos_api.download_file(req)

            logger.debug(str(ret))
            if ret['code'] != 0:
                logger.error("error code: " + str(ret['code']))
                raise IOError("Download Failed, ret=" + str(ret))

            if is_ignore_size:
                logger.info("Download Successfully, break")
                break

            if task.size is None:
                logger.info("task's size is None, skip check file size on local")
                break

            from os import path
            task_size = 0
            if isinstance(task.size, int):
                task_size = task.size
            else:
                task_size = int(task.size['filelen'])

            if path.getsize(local_path) != task_size:
                logger.error("Download Failed, size1: {size1}, size2: {size2}".format(size1=path.getsize(local_path),
                                                                                      size2=task_size))
            else:
                logger.info("Download Successfully, break")
                break
        else:
            raise IOError("Download Failed with 20 retry")

    def upload(self, task, local_path):
        cos_path = task.key
        if not cos_path.startswith('/'):
            cos_path = '/' + cos_path

        if self._prefix_dir:
            cos_path = self._prefix_dir + cos_path

        if isinstance(local_path, unicode):
            local_path.encode('utf-8')
        insert_only = 0 if self._overwrite else 1
        for i in range(10):
            try:
                upload_request = UploadFileRequest(self._bucket, unicode(cos_path), local_path, insert_only=insert_only)
                upload_file_ret = self._cos_api.upload_file(upload_request)

                if upload_file_ret[u'code'] != 0:
                    logger.warn("upload failed:" + str(upload_file_ret))
                    raise OSError("UploadError: " + str(upload_file_ret))
                else:
                    break
            except:
                pass
        else:
            raise IOError("upload failed")

    def list(self, marker):
        if self._filelist is not None and len(self._filelist) > 0:
            filelist_path = self._sync_files_dir + '/' + os.path.basename(self._filelist)
	    filelist_task = Task(self._filelist, None, None, None)
            self._download(filelist_task, filelist_path, True)
            with open(filelist_path) as f:
                line = f.readline()
                while line:
                    if not line.startswith('/'):
                        line = '/' + line
                    task = self.get_task_by_key(line.strip())
                    yield task
                    line = f.readline()
        else:
            if self._prefix_dir is None:
                for i in self.dfs('/'):
                    yield i
            else:
                for i in self.dfs(self._prefix_dir):
                    yield i

    def dfs(self, path):
        print "DFS: {path}".format(path=to_utf8(path))
        _finish = False
        _context = u''
        max_retry = self._max_retry

        path = to_unicode(path)

        while not _finish:
            request = ListFolderRequest(bucket_name=self._bucket, cos_path=path, context=_context)
            request.set_delimiter = '/'
            ret = self._cos_api.list_folder(request)
            logger.debug(str(ret))

            if ret['code'] != 0:
                max_retry -= 1
            else:
                _finish = ret['data']['listover']
                _context = ret['data']['context']
                for item in ret['data']['infos']:
                    if 'filelen' in item:
                        try:
                            key = "{prefix}{filename}".format(prefix=path, filename=item['name'])
                            logger.info("key=%s", key)
                            yield Task(key, item, None, None)
                        except:
                            pass
                    else:
                        _sub_dir = "{prefix}{filename}".format(prefix=path.encode('utf-8'),
                                                               filename=item['name'].encode('utf-8'))
                        for i in self.dfs(_sub_dir):
                            yield i

            if max_retry == 0:
                _finish = True

    def exists(self, task):
        _path = task.key
        _size = task.size

        if not _path.startswith('/'):
            _path = '/' + _path
        logger = getLogger(__name__)
        # logger.info("func: exists: " + str(_path))
        if self._prefix_dir:
            _path = self._prefix_dir + _path

        if isinstance(_path, str):
            _path = _path.decode('utf-8')
        request = StatFileRequest(self._bucket, _path)
        ret = self._cos_api.stat_file(request)
        logger.debug("ret: " + str(ret))
        # import json
        # v = json.loads(ret)
        if ret['code'] != 0:
            # logger.warn("error code: " + str(ret['code']))
            return False

        if int(ret['data']['filelen']) != int(ret['data']['filesize']):
            logger.warn("file is broken, filelen: {len}, filesize: {size}".format(
                len=ret['data']['filelen'],
                size=ret['data']['filesize']
            ))
            return False
        elif _size is not None and int(ret['data']['filelen']) != int(_size):
            return False
        return True

    def get_task_by_key(self, key):
        _path = key

        if not _path.startswith('/'):
            _path = '/' + _path
        logger = getLogger(__name__)

        if isinstance(_path, str):
            _path = _path.decode('utf-8')
        request = StatFileRequest(self._bucket, _path)
        ret = self._cos_api.stat_file(request)
        logger.info("ret: " + str(ret))
        # import json
        # v = json.loads(ret)
        if ret['code'] != 0:
            logger.warn("get task by key error, key = {},error code: {}".format(key, str(ret['code'])))
            return Task(key, None, None, None)

        return Task(key, int(ret['data']['filesize']), None, None)
