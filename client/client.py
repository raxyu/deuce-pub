#!/usr/bin/env python

"""
Deuce Client Sample

:author:  Xuan Yu
:date:  2014/01/22
"""

from __future__ import print_function

import json
import sys
import requests
import os
import io
import hashlib
from rabin import RabinFingerprint


# -*- coding: utf-8 -*-
#import sqlite3
#import subprocess
#from collections import namedtuple
#def from_utf8(data):
#    """
#    Short-hand function decoding utf8-encoded strings.
#    """
#    return data.decode('utf8')


'''
class Configuration:
'''
class Configuration:
  def __init__(self, configuration_file_name):
    if not os.path.exists(configuration_file_name):
      raise IOError('File {} does not exist.'.format(configuration_file_name))
    with open(configuration_file_name) as file_data: 
      self.config = json.load(file_data)

  def GetApiHost(self):
    return  self.config["ApiHostName"]

  def GetVaultId(self):
    return  self.config["VaultId"]

'''
class Blocks:
'''
class Blocks:
  def __init__(self):
    self.blocks = list()

  def Insert(self, blockId, blocksize, fileoffset):
    self.blocks.append((blockId, blocksize, fileoffset))

  def Decode(self):
    print (json.dumps(self.blocks))
    return json.dumps(self.blocks)
  #YUDEBUG
  def Dump(self):
    for block in self.blocks:
      print (block[0], repr(block[1]).rjust(10), repr(block[2]).rjust(12))
      
backup_blocks = Blocks()  


class FileBlocks:
  global backup_blocks
  
  def __init__(self, file_name):
    if not os.path.exists(file_name):
      raise IOError('File {} does not exist.'.format(file_name))
    self.fd = io.open(file_name, 'rb', buffering=4096*4)
    global config
    self.config = config

  def __del__(self):
    try:
      self.fd.close()
    except Exception, e:
      pass


  '''
    Run 
  '''
  def Run(self):
    # Create blocks and Calculate hashes
    self.RabinFile()
    # Upload file manifest
    self.UploadFileManifest()

    # Query blocks (with hashses)
    #self.QueryBlocks()
    # Upload blocks
    #self.UploadBlock()


  '''
    RabinFile 
  '''
  def RabinFile(self):
    total_bytes_in_blocks = 0
    min_block_size = 50 * 1024
    fingerprint = RabinFingerprint(0x39392FAAAAAAAE)
    block_size = 0
    sha1 = hashlib.sha1()

    while True:
        buff = self.fd.read(4096)
        bytes_read = len(buff)

        if bytes_read == 0:
            if block_size > 0:
                # Finish off the last part of the file as a block
                backup_blocks.Insert(sha1.hexdigest(), 
                      block_size, total_bytes_in_blocks)
                total_bytes_in_blocks += block_size
            break

        for i in range(0, bytes_read):
            fp = fingerprint.update(buff[i])
            sha1.update(buff[i:i + 1])

            block_size += 1

            if fp == 0x04 and block_size > min_block_size:
                backup_blocks.Insert(sha1.hexdigest(), 
                      block_size, total_bytes_in_blocks)
                total_bytes_in_blocks += block_size

                # Reset everything
                block_size = 0
                sha1 = hashlib.sha1()
                fingerprint.clear()


  '''
    QueryBlocks 
  '''
  def QueryBlocks(self):
    url = self.config.GetApiHost() + '/v1.0/' + self.config.GetVaultId() + '/blocks/query'  
    hdrs = {'content-type': 'application/json'}
    params = {'Query':'contains'}
    data = backup_blocks.Decode()
    response = requests.post(url, params=params, data=data, headers=hdrs)

    #print (response.text)
    print (response.status_code)


  '''
    UploadBlock 
  '''
  def UploadBlock(self):
    url = self.config.GetApiHost() + '/v1.0/' + self.config.GetVaultId() + '/blocks'  
    hdrs = {'content-type': 'application/octet-stream'}
    params = {}
    data = backup_blocks.Decode()
    response = requests.post(url, params=params, data=data, headers=hdrs)

    #print (response.text)
    print (response.status_code)


  '''
    UploadFileManifest
  '''
  def UploadFileManifest(self):
    url = self.config.GetApiHost() + '/v1.0/' + self.config.GetVaultId() + '/objects/metacreate'  
    hdrs = {'content-type': 'application/x-deuce-block-list'}
    params = {}
    data = backup_blocks.Decode()
    response = requests.post(url, params=params, data=data, headers=hdrs)

    #print (response.text)
    print (response.status_code)





"""
Execute the program.
"""
def main():
  if len(sys.argv) < 3:
    print('Usage: {}   Configuration_File_Name_json Backup_File_Name'.format(sys.argv[0]))
    print('   Configuration_File_Name_json ... Configuration file name in json, e.g., bootstrap.json')
    print('   Backup_File_Name ... The file to backup.')
    quit()

  try:
    # Load Configuration.
    global config
    config = Configuration(sys.argv[1])

    # Back up the File.
    backup = FileBlocks(sys.argv[2])
    backup.Run()
  except Exception, e:
    print ("Exception: ", e)
  

if __name__ == '__main__':
  main()

