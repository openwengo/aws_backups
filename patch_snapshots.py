#!/usr/bin/python
# -*- coding: utf-8 -*-
import boto
import boto.ec2
import boto3
import sys
import re
import datetime
#from config import config
import dateutil
import dateutil.parser
import pytz
import subprocess
import argparse
import time
import signal
from pprint import pprint
import atexit
import os

class GracefulInterruptHandler(object):
    def __init__(self, sig=signal.SIGINT):
        self.sig = sig

    def __enter__(self):
        self.interrupted = False
        self.released = False
        self.original_handler = signal.getsignal(self.sig)
        def handler(signum, frame):
            self.release()
            self.interrupted = True
        signal.signal(self.sig, handler)
        return self

    def __exit__(self, type, value, tb):
        self.release()

    def release(self):
        if self.released:
            return False
        signal.signal(self.sig, self.original_handler)
        self.released = True
        return True

parser = argparse.ArgumentParser(usage="--src-host <on premises host to connect to> --src-volume <on premises lvm volume to synchronise> --new-src-host --new-src-volume")
parser.add_argument('--src-host', required=True)
parser.add_argument('--src-volume', required=True)
parser.add_argument('--new-src-host', required=True)
parser.add_argument('--new-src-volume', required=True)

prg_args = parser.parse_args()

src_host = prg_args.src_host.strip()
src_volume = prg_args.src_volume.strip()
new_src_host = prg_args.new_src_host.strip()
new_src_volume = prg_args.new_src_volume.strip()

print("Retagging from %s - %s to %s - %s\n" % ( src_host , src_volume, new_src_host, new_src_volume) )

lock_file="/dev/shm/snap_lock_%s_%s_%s_%s.pid" % ( src_host, src_volume.replace("/","_"), new_src_host, new_src_volume.replace("/","_") )
if os.path.exists(lock_file):
   print("ERROR: There is already a lock for this sync at %s\n" % lock_file)
   sys.exit(2)
else:
   with open(lock_file, 'a') as fd:
      fd.write("%i" % os.getpid())

@atexit.register
def remove_lock():
   os.unlink(lock_file)


ec2_region_name = "eu-west-1" #config['ec2_region_name']
ec2_region_endpoint = "ec2.eu-west-1.amazonaws.com"
#sns_arn = config.get('arn')
#proxyHost = config.get('proxyHost')
#proxyPort = config.get('proxyPort')
proxyHost = None
proxyPort = None
aws_access_key = None



region = boto.ec2.regioninfo.RegionInfo(name=ec2_region_name, endpoint=ec2_region_endpoint)

# Connect to AWS using the credentials provided above or in Environment vars or using IAM role.
print 'Connecting to AWS'
if proxyHost:
    # proxy:
    # using roles
    if aws_access_key:
        conn = boto.ec2.connection.EC2Connection(aws_access_key, aws_secret_key, region=region, proxy=proxyHost, proxy_port=proxyPort)
    else:
        conn = boto.ec2.connection.EC2Connection(region=region, proxy=proxyHost, proxy_port=proxyPort)
else:
    # non proxy:
    # using roles
    if aws_access_key:
        conn = boto.ec2.connection.EC2Connection(aws_access_key, aws_secret_key, region=region)
    else:
        conn = boto.ec2.connection.EC2Connection(region=region)

ec2_conn = conn

volumes_list = ec2_conn.get_all_volumes(filters={'tag:src_host':src_host,'tag:src_volume':src_volume })

is_new_volume = False

if len(volumes_list) > 0 :
   if len(volumes_list) > 1:
      print('ERROR: there are ' + str(len(volumes_list)) + ' volume for ' + src_host + ' and ' + src_volume + '. Clean the superflous volume(s) and run again')
      sys.exit(6)
   for v in volumes_list:
      print("Volume " + v.id + " has these tags: ")
      for tkey,tvalue in v.tags.iteritems():
         print(tkey + " => " + tvalue + ", ")
      print("\n")
      print("Retagging it!")
      ec2_conn.create_tags([v.id], {"Name":"backup_%s_%s" % ( new_src_host, new_src_volume ), "src_host":new_src_host, "src_volume":new_src_volume } )
else:
   print("No Volume found")

snapshots_list = ec2_conn.get_all_snapshots( owner = 'self', filters={'tag:src_host':src_host,'tag:src_volume':src_volume})

for snap in snapshots_list:
  print("Patching snap %s change name to %s" % ( snap.id,  snap.tags['Name'].replace(src_host, new_src_host).replace(src_volume,new_src_volume) ))
  ec2_conn.create_tags([snap.id], {"Name": snap.tags['Name'].replace(src_host, new_src_host).replace(src_volume,new_src_volume) , "src_host":new_src_host, "src_volume":new_src_volume } )
  #snap.description = snap.description.replace(src_host, new_src_host).replace(src_volume,new_src_volume)
