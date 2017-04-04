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

def attach_volume_at(ebs_volume, my_instance_id, device_string, sleep_delay, max_count):
   is_attached=False
   while(True):
      print("Attach " + ebs_volume.id + " to " + device_string )
      try:
         result = ec2_conn.attach_volume(ebs_volume.id, my_instance_id, device_string)
         is_attached = True
      except boto.exception.EC2ResponseError as e:
            result = e.error_message
      break
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
   return True


max_count = 10
sleep_delay = 6
snapshot_sleep_delay = 300

nb_daily_backups = 7
nb_weekly_backups = 4
nb_monthly_backups = 3

parser = argparse.ArgumentParser(usage="--instance-id <instance-id> --root-volume <ebs-volume to set as root> [ --delete-existing-root ] [ --stop-instance ] [ --start-instance ]")
parser.add_argument('--instance-id', required=True, help="id of instance for which we need to swap the root device")
parser.add_argument('--root-volume', required=True, help="id of volume which will be used as root device")
parser.add_argument('--delete-existing-root', required=False, action="store_true", help="if set, existing root device will be deleted")
parser.add_argument('--stop-instance', required=False, action="store_true", help="if set, instance will be stopped if is running")
parser.add_argument('--start-instance', required=False, action="store_true", help="if set, instance will be started once the swap is done")

prg_args = parser.parse_args()

instance_id = prg_args.instance_id.strip()
root_volume = prg_args.root_volume.strip()
if prg_args.delete_existing_root:
   do_delete_existing_root = True
else:
   do_delete_existing_root = False
if prg_args.stop_instance:
   do_stop_instance = True
else:
   do_stop_instance = False
if prg_args.start_instance:
   do_start_instance = True
else:
   do_start_instance = False



print("Replace its root volume if instance %s to %s\n" % ( instance_id , root_volume ) )

lock_file="/dev/shm/swaproot_lock_%s_%s.pid" % ( instance_id, root_volume )

if os.path.exists(lock_file):
   print("ERROR: There is already a lock for this sync at %s\n" % lock_file)
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



instances_reservations = ec2_conn.get_all_instances(instance_ids=[ instance_id ])

if len(instances_reservations) != 1:
   print("ERROR: Found no instance with id %s" % instance_id)
   sys.exit(1)

l_instance = [ i for r in instances_reservations for i in r.instances ][0]

volumes_list = ec2_conn.get_all_volumes(filters={'volume-id':root_volume })

if len(volumes_list) != 1:
   print("ERROR: Found no volume with id %s" % root_volume)
   sys.exit(3)

l_volume = volumes_list[0]

do_attach=True
if l_volume.update(validate=True) != "available":
   if l_volume.attach_data:
       if l_volume.attach_data.instance_id == l_instance.id:
          print("Volume is already attached to instance!..")
          do_attach=False
   if do_attach:
       print("ERROR: Volume %s is not avalailable" % root_volume)
       sys.exit(4)

if do_attach and l_instance.state != 'stopped':
   if not(do_stop_instance):
      print("ERROR: Instance %s state is %s, which is not stopped." % ( instance_id , l_instance.state ))
      sys.exit(2)
   print("Unplugging instance %s" % instance_id)
   ec2_conn.stop_instances(instance_ids=[instance_id], force=True)
   i=0
   l_instance.update()
   while (i < max_count):
       if l_instance.state != "stopped":
          print(str(l_instance.state)  + "..." )
          time.sleep(sleep_delay)
          l_instance.update()
          i += 1
       else:
          break
   print('\n')


if do_attach:
    print("Instance %s is stopped and volume %s is available. Let's go" % ( instance_id, root_volume))
    
    for device, ebs in iter(sorted(l_instance.block_device_mapping.items())):
       print("Mapping: " + device + "=>" + ebs.volume_id + "\n" )
       if device == "/dev/sda1":
          print("Volume %s is already attached at: %s. Detach it" %  (ebs.volume_id, device))
          volumes_list = ec2_conn.get_all_volumes(filters={'volume-id':ebs.volume_id })
          res_detach = detach_volume(volumes_list[0], sleep_delay, max_count)
          print("Detach status: %s" % res_detach)
          if do_delete_existing_root and ebs.volume_id != l_volume.id:
              print("Delete old root")
              ec2_conn.delete_volume(ebs.volume_id)
    
    print("Attach volume as root device")
    
    
    attach_volume_at(l_volume, l_instance.id, "/dev/sda1", sleep_delay, max_count)

if do_start_instance:
   print("Starting instance with new root")
   ec2_conn.start_instances(instance_ids=[instance_id])

sys.exit(0)
