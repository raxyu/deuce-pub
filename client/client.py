#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utility for checking whether a given Cloud Files bundle matches 
the metadata stored in a container's VaultDB.

:author:  Xuan Yu
:date:  2013/11/22
"""
from __future__ import print_function

import json
import sys
import sqlite3
import requests
import os
import hashlib
import subprocess
from collections import namedtuple


def from_utf8(data):
    """
    Short-hand function decoding utf8-encoded strings.
    """
    return data.decode('utf8')

#Container = namedtuple('Container',
#                       ['name', 'count', 'num_bytes'])
#
#CloudObject = namedtuple('CloudBundleObject',
#                         ['name', 'hash', 'num_bytes'])
#
#DbBundleObject = namedtuple('DbBundleObject',
#                            ['id', 'hash', 'num_bytes',
#                             'garbage_bytes', 'deleteflag'])





class Configuration:
  def __init__(self, configuration_file_name):
    if not os.path.exists(configuration_file_name):
      raise IOError('File {} does not exist.'.format(configuration_file_name))
    file_data = open(configuration_file_name)
    self.config = json.load(file_data)
    file_data.close()

  def GetApiHost(self):
    return  self.config["ApiHostName"]

  def GetVaultId(self):
    return  self.config["VaultId"]


class FileBlocks:
  def __init__(self, file_name):
    if not os.path.exists(file_name):
      raise IOError('File {} does not exist.'.format(file_name))
    self.fd = open(file_name)
    global config
    self.config = config

  def __del__(self):
    try:
      self.fd.close()
    except Exception, e:
      pass

  def Run(self):
    # Create blocks and Calculate hashes
    # Query blocks (with hashses)
    self.QueryBlocks()
    # Upload blocks
    self.UploadBlock()
    # Upload file manifest
    self.UploadFileManifest()
    
  def QueryBlocks(self):
    blocks = {'1': 'ALyE6SRSaIjtZbycQcV0asYHMt+H0h'}

    url = self.config.GetApiHost() + '/v1.0/' + self.config.GetVaultId() + '/blocks/query'  
    hdrs = {'content-type': 'application/json'}
    params = {'Query':'contains'}
    data = json.dumps(blocks)
    response = requests.post(url, params=params, data=data, headers=hdrs)

    #print (response.text)
    print (response.status_code)

  def UploadBlock(self):
    #blocks = {'1','123456'}
    blocks = {'1': 'ALyE6SRSaIjtZbycQcV0asYHMt+H0h'}

    url = self.config.GetApiHost() + '/v1.0/' + self.config.GetVaultId() + '/blocks'  
    hdrs = {'content-type': 'application/octet-stream'}
    params = {}
    #data = blocks
    data = json.dumps(blocks)
    response = requests.post(url, params=params, data=data, headers=hdrs)

    #print (response.text)
    print (response.status_code)


  def UploadFileManifest(self):
    blocks = {'1': 'ALyE6SRSaIjtZbycQcV0asYHMt+H0h'}

    url = self.config.GetApiHost() + '/v1.0/' + self.config.GetVaultId() + '/objects/metacreate'  
    hdrs = {'content-type': 'application/x-deuce-block-list'}
    params = {}
    data = json.dumps(blocks)
    response = requests.post(url, params=params, data=data, headers=hdrs)

    #print (response.text)
    print (response.status_code)





def main():
  #"""
  #Execute the program.
  #"""
  if len(sys.argv) < 3:
    print('Usage: {}   Configuration_File_Name_json Backup_File_Name'.format(sys.argv[0]))
    print('   Configuration_File_Name_json ... Configuration file name in json, e.g., bootstrap.json')
    print('   Backup_File_Name ... The file to backup.')
    quit()

  #if not os.path.exists('.validate_cache'):
  #    os.mkdir('.validate_cache')

  #credentials = Credentials(sys.argv[1], sys.argv[2])

  try:
    # Load Configuration.
    global config
    config = Configuration(sys.argv[1])

    # Back up the File.
    blocks = FileBlocks(sys.argv[2])
    blocks.Run()
    






  except Exception, e:
    print ("Exception: ", e)
  
if __name__ == '__main__':
  main()

