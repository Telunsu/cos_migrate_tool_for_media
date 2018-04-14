# -*- coding: utf-8 -*-

from logging import getLogger
from migrate_tool import storage_service
from migrate_tool.task import Task
from boto.s3.key import Key

from boto.s3.connection import S3Connection
import boto


logger = getLogger(__name__)


class S3StorageService(storage_service.StorageService):
    def __init__(self, *args, **kwargs):

        accesskeyid = kwargs['accesskeyid']
        accesskeysecret = kwargs['accesskeysecret']
        endpoint = kwargs['endpoint']
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
                                                                                                    

	k = Key(self._bucket_api)
	k.key = s3_path
        for j in range(5):                                                                          
            try:                                                                                    
		k.set_contents_from_filename(local_path)
                break                                                                               
            except Exception as e:                                                                  
                logger.warn('upload failed %s' % str(e))                                            
        else:                                                                                       
            raise OSError("uploaderror")

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
