# -*- coding: utf-8 -*-

from logging import getLogger
from migrate_tool import storage_service
from migrate_tool.task import Task
from boto.s3.key import Key
from boto.s3.multipart import MultiPartUpload

from boto.s3.connection import S3Connection
import boto
import math
import os
from filechunkio import FileChunkIO


logger = getLogger(__name__)


class S3StorageService(storage_service.StorageService):
    def __init__(self, *args, **kwargs):

        accesskeyid = kwargs['accesskeyid']
        accesskeysecret = kwargs['accesskeysecret']
        endpoint = kwargs['endpoint'] if 'endpoint' in kwargs else ''
        bucket = kwargs['bucket']
        self._prefix = kwargs['prefix'] if 'prefix' in kwargs else ''
        _s3_api = None
        if len(endpoint) > 0:
            _s3_api = boto.connect_s3(aws_access_key_id=accesskeyid, aws_secret_access_key=accesskeysecret, 
                    host=endpoint, is_secure=False, calling_format = boto.s3.connection.OrdinaryCallingFormat())
        else:
            _s3_api = S3Connection(aws_access_key_id=accesskeyid, aws_secret_access_key=accesskeysecret)
        self._bucket_api = _s3_api.get_bucket(bucket)

    def download(self, task, local_path):
        for i in range(20):
            key = self._bucket_api.get_key(task.key)

            if key is not None:
                key.get_contents_to_filename(local_path)
            else:
                raise IOError("Download failed 404")
            if task.size is None:
                logger.info("task's size is None, skip check file size on local")
                break

            from os import path
            if path.getsize(local_path) != int(task.size):
                logger.error("Download Failed, size1: {size1}, size2: {size2}".format(size1=path.getsize(local_path),
                                                                                      size2=task.size))
            else:
                logger.info("Download Successfully, break")
                break
        else:
            raise IOError("download failed with 20 retry")

    def upload(self, cos_path, local_path):
	s3_path = cos_path.key                                                                            
        if not s3_path.startswith('/'):                                                               
            s3_path = '/' + s3_path                                                                  
                                                                                                       
        if self._prefix:                                                                           
            s3_path = self._prefix + s3_path                                                     
                                                                                                       
        if isinstance(local_path, unicode):                                                            
            local_path.encode('utf-8')                                                              
        if s3_path.startswith('/'):                                                                
            s3_path = s3_path[1:]                                                                 
                                                                                                    
        file_size = os.stat(local_path).st_size
        # 大于2G的文件采用分块上传
        try:
            if file_size > 2 * 1024 * 1024 * 1024:
                mp = self._bucket_api.initiate_multipart_upload(s3_path)
                chunk_size = 52428800
                chunk_count = int(math.ceil(file_size/ float(chunk_size)))
                for i in range(chunk_count):
                    offset = chunk_size * i
                    bytes = min(chunk_size, file_size - offset)
                    with FileChunkIO(local_path, 'r', offset=offset, bytes=bytes) as fp:
                        mp.upload_part_from_file(fp, part_num=i + 1)
                mp.complete_upload()
            else:
                k = Key(self._bucket_api)
                k.key = s3_path
                k.set_contents_from_filename(local_path)
        except Exception as e:
            logger.warn('upload failed %s' % str(e))

    def list(self):
        for obj in self._bucket_api.list(prefix=self._prefix):
            if obj.name[-1] == '/':
                continue
            logger.info("yield new object: {}".format(obj.key))
            yield Task(obj.name, obj.size, None)

    def exists(self, _path):
	k = Key(self._bucket_api)
	k.key = _path.key
        try:                                                                                           
            return k.exists()                    
        except:                                                                                        
            logger.exception("head s3 file failed")                                                   

	return False
