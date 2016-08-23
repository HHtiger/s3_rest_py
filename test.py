#!/usr/bin/env python
#coding=utf-8

import s3

if __name__ == '__main__':
    client = s3.S3Client('AKIAI6JWCKPLTSGF7I3A', 'N/5IHNUxrrt84aZPg7nJI2XJhaXWzV5xvdhxyf9A')
    for bucket in client.list_buckets()[1]:
        print bucket.name

    client.upload_file('E:\\tiger\\bb','founder-test','img/bb',s3.X_AMZ_ACL.private)