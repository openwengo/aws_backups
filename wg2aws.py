#!/usr/bin/python
# -*- coding: utf-8 -*-
import boto
import boto.ec2
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

def create_snapshot_and_wait(ebs_volume, description, tags, sleep_interval, max_sleep):
    new_snap = ebs_volume.create_snapshot(description = description )
    ec2_conn.create_tags([new_snap.id], tags )
    if max_sleep > 0:
       tot_sleep = 0
       print("snap progress is")
       while ( new_snap.status != 'completed' ) and (tot_sleep < max_sleep ):
          print(" " + str(new_snap.progress).strip() )
          time.sleep(sleep_interval)
          tot_sleep += sleep_interval
          new_snap.update()
    return new_snap

def detach_volume(ebs_volume, sleep_delay, max_count):
   print("Detach volume " + ebs_volume.id + "\n" )
   result = ebs_volume.detach()
   i=0
   ebs_volume.update()
   while (i < max_count):
     if ebs_volume.attachment_state() is not None:
        print(str(ebs_volume.attachment_state())  + "..." )
        time.sleep(sleep_delay)
        ebs_volume.update()
        i += 1
     else:
        break
   print('\n')
   if ebs_volume.attachment_state() is not None:
      print("Detach failed. Try forcing\n")
      result = ebs_volume.detach(force=True)
      i=0
      ebs_volume.update()
      while (i < max_count):
         if ebs_volume.attachment_state() is not None:
            print(str(ebs_volume.attachment_state())  + "..." )
            time.sleep(sleep_delay)
            ebs_volume.update()
            i += 1
         else:
            break
      print('\n')
      if ebs_volume.attachment_state() is not None:
         print("Failed to detach volume after "+ str(max_count * sleep_delay) +"seconds\n")
         return 7
   else:
      print("Detach was ok!"+ str(result) +"\n")
   return 0

def compare_snaps(snap1,snap2):
    if dateutil.parser.parse(snap1.start_time) < dateutil.parser.parse(snap2.start_time):
       return -1
    else:
       return 1

def timedelta_total_seconds(timedelta):
    return (
        timedelta.microseconds + 0.0 +
        (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6


def attach_volume_at_letter_or_more(ebs_volume, my_instance_id, device_letter, sleep_delay, max_count):
   is_attached=False
   while (not is_attached and device_letter < 'z'):
      last_device_ascii = ord(device_letter)
      last_device = "/dev/sd%s" % chr(last_device_ascii)
      print("Attach " + ebs_volume.id + " to " + last_device )
      try:
         result = ec2_conn.attach_volume(ebs_volume.id, my_instance_id, last_device)
         is_attached = True
      except boto.exception.EC2ResponseError as e:
         if "is already in use" in e.error_message:
            device_letter = chr(last_device_ascii + 1)
   ebs_device = last_device
   if not is_attached:
      print("ERROR: Volume attach failed with result: " + result + " Bailing out.\n")
      sys.exit(5)
   print 'Attach Volume Result: ', result
   i=0
   ebs_volume.update()
   while (i < max_count):
       if ebs_volume.attachment_state() != "attached":
          print(str(ebs_volume.attachment_state())  + "..." )
          time.sleep(sleep_delay)
          ebs_volume.update()
          i += 1
       else:
          break
   print('\n')
   if ebs_volume.attachment_state() != "attached":
      print("ERROR: Volume did not attach after " + str( max_count * sleep_delay ) + " seconds. Bailing out.\n")
      sys.exit(5)
   return (last_device_ascii,last_device)



max_count = 10
sleep_delay = 6
snapshot_sleep_delay = 300

nb_daily_backups = 7
nb_weekly_backups = 4
nb_monthly_backups = 3

parser = argparse.ArgumentParser(usage="--src-host <on premises host to connect to> --src-volume <on premises lvm volume to synchronise> --conversion-script <script to be run to convert the disk>")
parser.add_argument('--src-host', required=True)
parser.add_argument('--src-volume', required=True)
parser.add_argument('--conversion-script', required=True)

prg_args = parser.parse_args()

src_host = prg_args.src_host.strip()
src_volume = prg_args.src_volume.strip()
conversion_script = prg_args.conversion_script.strip()

print("Converting %s from %s\n" % ( src_host , src_volume) )

lock_file="/dev/shm/convert_lock_%s_%s.pid" % ( src_host, src_volume.replace("/","_") )
if os.path.exists(lock_file):
   print("ERROR: There is already a lock for this convert at %s\n" % lock_file)
   sys.exit(2)
else:
   with open(lock_file, 'a') as fd:
      fd.write("%i" % os.getpid())

@atexit.register
def remove_lock():
   os.unlink(lock_file)


my_instance_id=subprocess.check_output(['ec2metadata','--instance-id'] ).strip()

print("My instance id: %s" % my_instance_id )

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

instances_list = ec2_conn.get_only_instances()
my_reservations = ec2_conn.get_all_instances(instance_ids=[ my_instance_id ])
my_instance = [i for r in my_reservations for i in r.instances][0]

print("I am running in %s\n" % my_instance.placement )

snapshots_list =  ec2_conn.get_all_snapshots(filters={'tag:src_host':src_host,'tag:src_volume':src_volume })

now_str=str(datetime.datetime.now(pytz.utc))
most_recent_snap_date = dateutil.parser.parse('1970-01-01 00:00:00.000000+00:00')
most_recent_snap = None

for snap in snapshots_list:
   #print(snap,snap.tags)
   if 'is_dailybackup' in snap.tags:
      backup_date = snap.tags['is_dailybackup']
      if dateutil.parser.parse(snap.tags['is_dailybackup']) > most_recent_snap_date:
         most_recent_snap_date = dateutil.parser.parse(snap.tags['is_dailybackup'])
         most_recent_snap = snap

if most_recent_snap is None:
   print("No recent snapshot found!")
   sys.exit(2)

print("Most recent snap is",most_recent_snap,most_recent_snap_date)

name_filter={'tag-key': 'Name','tag-value':'Snap_instance_%s_%s' % (src_host, src_volume)  }
volumes_list = ec2_conn.get_all_volumes(filters = name_filter )

if len(volumes_list) > 1:
         print("ERROR: more than one volume with name %s" % 'Snap_instance_%s_%s' % (src_host, src_volume))
         sys.exit(3)
elif len(volumes_list) == 1:
         ebs_volume = volumes_list[0]
         print("Delete old volume %s" % ebs_volume.id)
         ec2_conn.delete_volume(ebs_volume.id)
if 1: 
         print("Create fresh volume")
         ebs_volume = most_recent_snap.create_volume( zone= my_instance.placement, volume_type='gp2' )
         print('Ebs ' + ebs_volume.id + ' created from snapshot ' + most_recent_snap.id)
         ec2_conn.create_tags([ebs_volume.id], {'Name':'Snap_instance_%s_%s' % (src_host, src_volume) } )
         i = 0
         while ebs_volume.status != "available":
            if i > max_count * 10:
               break
            time.sleep(sleep_delay)
            ebs_volume.update()
            i+=1
         if ebs_volume.status != "available":
            print("ERROR: Volume " + ebs_volume.id + " unavailable after " + str(sleep_delay * max_count * 10) + " seconds. Exit\n")
            sys.exit(6)

ebs_device = None
last_device_ascii = ord('y')
last_device = "/dev/sd%s" % chr(last_device_ascii)

print("I am mapped with %s\n" % my_instance.block_device_mapping )
for device, ebs in iter(sorted(my_instance.block_device_mapping.items())):
   print("Mapping: " + device + "=>" + ebs.volume_id + "\n" )
   if ebs.volume_id == ebs_volume.id:
      print("Volume is already attached at:" + device)
      ebs_device = device
      last_device_ascii = ord(device[-1])
      last_device = "/dev/sd%s" % chr(last_device_ascii)
   elif last_device == device:
      last_device_ascii += 1
      last_device = "/dev/sd%s" % chr(last_device_ascii)

if not ebs_device:
   (last_device_ascii, last_device) =  attach_volume_at_letter_or_more(ebs_volume, my_instance_id, chr(last_device_ascii), sleep_delay, max_count)

print("Run disk tweaking")

fix_disk=subprocess.call([conversion_script])

if fix_disk != 0:
   print("%s failed with status %i" % (conversion_script, fix_disk))
   sys.exit(4)

res_detach = detach_volume(ebs_volume, sleep_delay, max_count)
if res_detach != 0:
   print("ERROR: detach failed with code " + str(res_detach))
   sys.exit(7)

print("VOLUME_ID: %s" % ebs_volume.id )

sys.exit(0)
