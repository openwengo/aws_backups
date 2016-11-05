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

max_count = 10
sleep_delay = 6
snapshot_sleep_delay = 300

nb_daily_backups = 7
nb_weekly_backups = 4
nb_monthly_backups = 3

parser = argparse.ArgumentParser(usage="--src-host <on premises host to connect to> --src-volume <on premises lvm volume to synchronise>")
parser.add_argument('--src-host', required=True)
parser.add_argument('--src-volume', required=True)
parser.add_argument('--remote-snap-size', required=False, default="1G")
parser.add_argument('--wg-entity', required=False, default="wengo")
parser.add_argument('--skip-if-fresher', required=False, default="40000")

prg_args = parser.parse_args()

src_host = prg_args.src_host.strip()
src_volume = prg_args.src_volume.strip()
remote_snap_size = prg_args.remote_snap_size.strip()
wg_entity = prg_args.wg_entity.strip()
skip_if_fresher = int(prg_args.skip_if_fresher.strip())

print("Syncing %s from %s\n" % ( src_host , src_volume) )

src_volume_size=subprocess.check_output(['ssh',src_host,'get_lv_size.sh',src_volume]).strip()

if src_volume_size == "":
   print("ERROR: Volume " + src_volume + " does not exists on " + src_host + "\n")
   sys.exit(2)

try:
   src_volume_size = int(src_volume_size)
except:
   print("ERROR: Volume " + src_volume + " does not exists on " + src_host + "\n")
   sys.exit(3)
   
print( src_volume + " has size " + str(src_volume_size) + " kbytes\n")

src_volume_size_gb = src_volume_size / 1024 / 1024

if( src_volume_size_gb * 1024 * 1024 != src_volume_size ):
   print("ERROR: " + str(src_volume_size) + " is not a round number of gigabytes\n")
   sys.exit(4)

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


volumes_list = ec2_conn.get_all_volumes(filters={'availability-zone':my_instance.placement,'tag:src_host':src_host,'tag:src_volume':src_volume })

if len(volumes_list) > 0 :
   for v in volumes_list:
      print("Volume " + v.id + " is in my az " + " and has these tags: ")
      for tkey,tvalue in v.tags.iteritems():
         print(tkey + " => " + tvalue + ", ")
      print("\n")
      ebs_volume = v
      if v.tags.has_key('last_sync_status'):
         if v.tags['last_sync_status'] == "0":
            if v.tags.has_key('last_sync'):
               last_sync_dt = dateutil.parser.parse(v.tags['last_sync'])
               now_dt=datetime.datetime.now(pytz.utc)
               if (now_dt - last_sync_dt).seconds < skip_if_fresher:
                  print("Last sync happened " + str((now_dt - last_sync_dt).seconds) + " seconds ago which is less than " + str(skip_if_fresher))
                  sys.exit(0)
else:
   print('No ebs volume found for ' + src_host + ' - ' + src_volume + ' in az ' + my_instance.placement + '\n')
   ebs_volume = ec2_conn.create_volume(size=src_volume_size_gb, zone= my_instance.placement, volume_type='standard', encrypted=True)
   print('Ebs ' + ebs_volume.id + ' created')
   ec2_conn.create_tags([ebs_volume.id], {"Name":"backup_%s_%s" % ( src_host, src_volume ), "src_host":src_host, "src_volume":src_volume, "dailybackups": nb_daily_backups, "weeklybackups": nb_weekly_backups, "monthlybackups": nb_monthly_backups, "wg_entity": wg_entity  })
   i = 0
   while ebs_volume.status != "available":
      if i > max_count:
         break
      time.sleep(sleep_delay)
      ebs_volume.update()
      i+=1
   if ebs_volume.status != "available":
      print("Volume " + ebs_volume.id + " unavailable after " + str(sleep_delay * max_count) + " seconds. Exit\n")
      sys.exit(6)

ebs_device = None
last_device_ascii = ord('i')
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
   print("Attach " + ebs_volume.id + " to " + last_device )
   result = ec2_conn.attach_volume(ebs_volume.id, my_instance_id, last_device)
   ebs_device = last_device
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


sync_status=-1
if 1:
   print("Calling sync_block.sh\n")
   ec2_conn.create_tags([ebs_volume.id], {"sync_in_progress":"True"})
   ec2_conn.create_tags([ebs_volume.id], {"last_sync_status":-1})
   start_sync_time=datetime.datetime.now(pytz.utc)
   with GracefulInterruptHandler(sig = signal.SIGINT) as h:
      with GracefulInterruptHandler(sig = signal.SIGTERM) as h2:
         sync_status=subprocess.call(["sync_block.sh","--src-host",src_host,"--src-volume",src_volume,"--dst-volume","xvd%s" % chr(last_device_ascii),"--remote-snap-size",remote_snap_size])
         if h2.interrupted:
             print("Sync interrupted! (TERM)")
             ec2_conn.create_tags([ebs_volume.id], {"last_sync_status":-3})
             res_detach = detach_volume(ebs_volume, sleep_delay, max_count)
             sys.exit(10)
      if h.interrupted:
          print("Sync interrupted! (INT)")
          ec2_conn.create_tags([ebs_volume.id], {"last_sync_status":-2})
          res_detach = detach_volume(ebs_volume, sleep_delay, max_count)
          sys.exit(9)
           
           
   end_sync_time=datetime.datetime.now(pytz.utc)
   ec2_conn.create_tags([ebs_volume.id], {"sync_in_progress":"False"})
   ec2_conn.create_tags([ebs_volume.id], {"last_sync_status":sync_status})
   ec2_conn.create_tags([ebs_volume.id], {"last_sync_duration":str((end_sync_time - start_sync_time).seconds)})
   print("Sync finished with status %i\n" % sync_status)

now_str=str(datetime.datetime.now(pytz.utc))
ec2_conn.create_tags([ebs_volume.id], {"last_sync":now_str})

res_detach = detach_volume(ebs_volume, sleep_delay, max_count)
if res_detach != 0:
   print("ERROR: detach failed with code " + str(res_detach))
   sys.exit(7)

if sync_status <> 0:
   print("Sync status is not success. No snapshots\n")
   sys.exit(8)

snapshots_list = ec2_conn.get_all_snapshots( owner = 'self', filters={'volume-id': ebs_volume.id })

bucket_dailies = []
max_daily_backups = 0
bucket_weeklies = []
max_weekly_backups = 0
bucket_monthlies = []
max_monthly_backups = 0
today = datetime.datetime.now(pytz.utc)
if 'wg_entity' in ebs_volume.tags:
   wg_entity = ebs_volume.tags[u'wg_entity']
try:
    # Get backup policy from tags
    if 'dailybackups' in ebs_volume.tags:
        max_daily_backups = int(ebs_volume.tags[u'dailybackups'])
    if 'weeklybackups' in ebs_volume.tags:
        max_weekly_backups = int(ebs_volume.tags[u'weeklybackups'])
    if 'monthlybackups' in ebs_volume.tags:
        max_monthly_backups = int(ebs_volume.tags[u'monthlybackups'])
    print("Volume can have %i daily, %i weekly and %i monthly backups" % (max_daily_backups, max_weekly_backups, max_monthly_backups) )

    for snap in snapshots_list:
       date_diff = (today - dateutil.parser.parse(snap.start_time)).days
       #print "L'image %s existe, c'est un %s backup qui date de %s / %s days old" % ( image.description, m.group(1), image.creationDate, date_diff )
       if 'is_dailybackup' in snap.tags:
              bucket_dailies.append(snap)
       if 'is_weeklybackup' in snap.tags:
              bucket_weeklies.append(snap)
       if 'is_monthlybackup' in snap.tags:
              bucket_monthlies.append(snap)
       
    bucket_dailies = sorted(bucket_dailies, compare_snaps)
    bucket_weeklies = sorted(bucket_weeklies, compare_snaps)
    bucket_monthlies = sorted(bucket_monthlies, compare_snaps)
    print("daily backups:", len(bucket_dailies), bucket_dailies)
    print("weekly backups:", len(bucket_weeklies), bucket_weeklies)
    print("bucket_monthlies:", len(bucket_monthlies), bucket_monthlies)
    if max_daily_backups > 0:
       if (len(bucket_dailies)>0):
            print (timedelta_total_seconds(today - dateutil.parser.parse(bucket_dailies[-1].start_time)), " seconds elapsed since last daily backup")
       if (len(bucket_dailies) == 0) or (len(bucket_dailies)>0) and ( timedelta_total_seconds(today - dateutil.parser.parse(bucket_dailies[-1].start_time)) > 83000 ) :
           print("We have to generate a daily backup")
           new_snap = create_snapshot_and_wait(ebs_volume, "Backup (daily) of volume %s of host %s last synched at %s" % ( src_volume, src_host, now_str ), {"src_host":src_host, "src_volume":src_volume, "is_dailybackup": now_str, "wg_entity":wg_entity }, (snapshot_sleep_delay / 10) + 1, snapshot_sleep_delay )
       if len(bucket_dailies) > max_daily_backups:
           print("We have to remove a daily backups")
           tot_dailies = len(bucket_dailies)
           tot_remove = 0
           for snap_to_remove in bucket_dailies:
              if timedelta_total_seconds(today - dateutil.parser.parse(snap_to_remove.start_time)) > ( 86400 * max_daily_backups):
                 print("Suppressing", snap_to_remove.id, "...")
                 snap_to_remove.delete()
    if max_weekly_backups > 0:
       if (len(bucket_weeklies)>0):
            print ((today - dateutil.parser.parse(bucket_weeklies[-1].start_time)).days, " days elapsed since last weekly backup")
       if (len(bucket_weeklies) == 0) or (len(bucket_weeklies)>0) and ( (today - dateutil.parser.parse(bucket_weeklies[-1].start_time)).days > 6 ) :
           print("We have to generate a weekly backup")
           new_snap = create_snapshot_and_wait(ebs_volume, "Backup (weekly) of volume %s of host %s last synched at %s" % ( src_volume, src_host, now_str ), {"src_host":src_host, "src_volume":src_volume, "is_weeklybackup": now_str, "wg_entity":wg_entity }, (snapshot_sleep_delay / 10) + 1, snapshot_sleep_delay )
       if len(bucket_weeklies) > max_weekly_backups:
           print("We have to remove a weekly backup")
           tot_weeklies = len(bucket_weeklies)
           for snap_to_remove in bucket_weeklies:
              if timedelta_total_seconds(today - dateutil.parser.parse(snap_to_remove.start_time)) > ( 86400 * 7 *  max_weekly_backups):
                 print("Suppressing", snap_to_remove.id, "...")
                 snap_to_remove.delete()
    if max_monthly_backups > 0:
       if (len(bucket_monthlies)>0):
            print ((today - dateutil.parser.parse(bucket_monthlies[-1].start_time)).days, " days elapsed since last monthly backup")
       if (len(bucket_monthlies) == 0) or (len(bucket_monthlies)>0) and ( (today - dateutil.parser.parse(bucket_monthlies[-1].start_time)).days > 30 ) :
           print("We have to generate a monthly backup")
           new_snap = create_snapshot_and_wait(ebs_volume, "Backup (monthly) of volume %s of host %s last synched at %s" % ( src_volume, src_host, now_str ), {"src_host":src_host, "src_volume":src_volume, "is_monthlybackup": now_str, "wg_entity":wg_entity }, (snapshot_sleep_delay / 10) + 1, snapshot_sleep_delay )
       if len(bucket_monthlies) > max_monthly_backups:
           print("We have to remove a monthly backup")
           tot_monthlies = len(bucket_monthlies)
           tot_remove = 0
           for snap_to_remove in bucket_monthlies:
              if timedelta_total_seconds(today - dateutil.parser.parse(snap_to_remove.start_time)) > ( 86400 * 30 *  max_monthly_backups):
                 print("Suppressing", snap_to_remove.id, "...")
                 snap_to_remove.delete()


except Exception as e:
    print("Error occured", e)
