#!/usr/bin/python
# -*- coding: utf-8 -*-
import boto
import boto.ec2
import sys
import re
import datetime
from config import config
import dateutil
import dateutil.parser
import pytz

def get_snapshots4ami(snapshots_list, ami):
   snap_list = []
   for snap in snapshots_list:
     m = re.match("Created by CreateImage(.*) for %s from vol-(.*)$" % ami.id, snap.description )
     if m:
       snap_list.append(snap)
   return snap_list

def compare_amis(ami1,ami2):
    # ami1 and ami2 are 2 lists, the ami is the first element of each list
    if dateutil.parser.parse(ami1[0].creationDate) < dateutil.parser.parse(ami2[0].creationDate):
       return -1
    else:
       return 1

def generate_image(ec2_conn,instance, period, today, wg_entity):
   inst_name =  instance.tags[u'Name']
   img_name = "wengo-%s-%s-%s" % ( inst_name, period, today.strftime("%d-%m-%Y"))
   img_id = ec2_conn.create_image(instance_id = instance.id, name = img_name, description = img_name , no_reboot = True, block_device_mapping = None, dry_run = False )
   ec2_conn.create_tags([img_id], {'wg_entity': wg_entity})
   return img_id

def timedelta_total_seconds(timedelta):
    return (
        timedelta.microseconds + 0.0 +
        (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6

# Get settings from config.py
aws_access_key = config['aws_access_key']
aws_secret_key = config['aws_secret_key']
ec2_region_name = config['ec2_region_name']
ec2_region_endpoint = config['ec2_region_endpoint']
#sns_arn = config.get('arn')
proxyHost = config.get('proxyHost')
proxyPort = config.get('proxyPort')


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

images_list = ec2_conn.get_all_images( owners = 'self' )

snapshots_list = ec2_conn.get_all_snapshots( owner = 'self' )

#print instances_list
today = datetime.datetime.now(pytz.utc)
for instance in instances_list:
  print "Instance %s was launched at %s and has tags " % ( instance.id , instance.launch_time )
  inst_name = "none"
  bucket_dailies = []
  max_daily_backups = 0
  bucket_weeklies = []
  max_weekly_backups = 0
  bucket_monthlies = []
  max_monthly_backups = 0
  if 'Name' in instance.tags:
   inst_name =  instance.tags[u'Name']
   print " Name: %s" % ( inst_name )
   try:
       # Get backup policy from tags
       if 'dailybackups' in instance.tags:
           max_daily_backups = int(instance.tags[u'dailybackups'])
       if 'weeklybackups' in instance.tags:
           max_weekly_backups = int(instance.tags[u'weeklybackups'])
       if 'monthlybackups' in instance.tags:
           max_monthly_backups = int(instance.tags[u'monthlybackups'])
       if 'wg_entity' in instance.tags:
           wg_entity = instance.tags[u'wg_entity']
       else:
           wg_entity = 'wengo'
       print("Instance can have %i daily, %i weekly and %i monthly backups" % (max_daily_backups, max_weekly_backups, max_monthly_backups) )
       for image in images_list:
         # ignore failed amis
         if image.state == "failed":
            continue
         m = re.match("wengo-%s-(daily|weekly|monthly)-(.*)$" % inst_name, image.description )
         if m:
           date_diff = (today - dateutil.parser.parse(image.creationDate)).days
           #print "L'image %s existe, c'est un %s backup qui date de %s / %s days old" % ( image.description, m.group(1), image.creationDate, date_diff )
           snap_list = get_snapshots4ami(snapshots_list, image)
           if m.group(1) == 'daily':
              bucket_dailies.append([image, snap_list])
           if m.group(1) == 'weekly':
              bucket_weeklies.append([image, snap_list])
           if m.group(1) == 'monthly':
              bucket_monthlies.append([image, snap_list])
          
           for snap in snap_list:
                 print("snap:", snap.id,"ami:", image.id, "instance:", inst_name, "created:", image.creationDate)
                 # fix tagging a posteriori
                 if not 'source_instance_name' in snap.tags: 
                    ec2_conn.create_tags([snap.id], {'source_instance_name': inst_name})
                 elif inst_name != snap.tags[u'source_instance_name']:
                    ec2_conn.create_tags([snap.id], {'source_instance_name': inst_name})
                 if not 'source_ami' in snap.tags: 
                    ec2_conn.create_tags([snap.id], {'source_ami': image.id})
                 elif image.id != snap.tags[u'source_ami']:
                    ec2_conn.create_tags([snap.id], {'source_ami': image.id})
                 elif not 'wg_entity' in snap.tags:
                    ec2_conn.create_tags([snap.id], {'wg_entity': wg_entity})
                 elif wg_entity != snap.tags[u'wg_entity']:
                    ec2_conn.create_tags([snap.id], {'wg_entity': wg_entity})
       bucket_dailies = sorted(bucket_dailies, compare_amis)
       bucket_weeklies = sorted(bucket_weeklies, compare_amis)
       bucket_monthlies = sorted(bucket_monthlies, compare_amis)
       print("daily backups:", len(bucket_dailies), bucket_dailies)
       print("weekly backups:", len(bucket_weeklies), bucket_weeklies)
       print("bucket_monthlies:", len(bucket_monthlies), bucket_monthlies)
       if max_daily_backups > 0:
          if (len(bucket_dailies)>0):
               print (timedelta_total_seconds(today - dateutil.parser.parse(bucket_dailies[-1][0].creationDate)), " seconds elapsed since last daily backup")
          if (len(bucket_dailies) == 0) or (len(bucket_dailies)>0) and ( timedelta_total_seconds(today - dateutil.parser.parse(bucket_dailies[-1][0].creationDate)) > 83000 ) :
              print("We have to generate a daily backup")
              img_id = generate_image(ec2_conn=ec2_conn,instance=instance, period="daily", today=today, wg_entity=wg_entity)
          if len(bucket_dailies) > max_daily_backups:
              print("We have to remove a daily backups")
              tot_dailies = len(bucket_dailies)
              tot_remove = 0
              for backup_set in bucket_dailies:
                 (image, snap_list) = backup_set
                 if timedelta_total_seconds(today - dateutil.parser.parse(image.creationDate)) > ( 86400 * max_daily_backups):
                    print("De-registering image", image.id, "...")
                    ec2_conn.deregister_image(image.id)
                    for snap in snap_list:
                       print("Remove snapshot", snap.id)
                       snap.delete()
                    
                    tot_remove += 1
                 if (tot_dailies - tot_remove) <= max_daily_backups:
                    break
                  
       if max_weekly_backups > 0:
          if (len(bucket_weeklies)>0):
               print ((today - dateutil.parser.parse(bucket_weeklies[-1][0].creationDate)).days, " days elapsed since last weekly backup")
          if (len(bucket_weeklies) == 0) or (len(bucket_weeklies)>0) and ( (today - dateutil.parser.parse(bucket_weeklies[-1][0].creationDate)).days > 6 ) :
              print("We have to generate a weekly backup")
              img_id = generate_image(ec2_conn=ec2_conn,instance=instance, period="weekly", today=today, wg_entity=wg_entity)
              print("image",img_id,"has been created")
          if len(bucket_weeklies) > max_weekly_backups:
              print("We have to remove a weekly backup", bucket_weeklies[0])
              tot_weeklies = len(bucket_weeklies)
              tot_remove = 0
              for backup_set in bucket_weeklies:
                 (image, snap_list) = backup_set
                 if timedelta_total_seconds(today - dateutil.parser.parse(image.creationDate)) > ( 86400 * 7 *  max_weekly_backups):
                    print("De-registering image", image.id, "...")
                    ec2_conn.deregister_image(image.id)
                    for snap in snap_list:
                       print("Remove snapshot", snap.id)
                       snap.delete()
                    
                    tot_remove += 1
                 if (tot_weeklies - tot_remove) <= max_weekly_backups:
                    break
                  
       if max_monthly_backups > 0:
          if (len(bucket_monthlies)>0):
               print ((today - dateutil.parser.parse(bucket_monthlies[-1][0].creationDate)).days, " days elapsed since last monthly backup")
          if (len(bucket_monthlies) == 0) or (len(bucket_monthlies)>0) and ( (today - dateutil.parser.parse(bucket_monthlies[-1][0].creationDate)).days > 30 ) :
              print("We have to generate a monthly backup")
              img_id = generate_image(ec2_conn=ec2_conn,instance=instance, period="monthly", today=today, wg_entity=wg_entity)
              print("image",img_id,"has been created")
          if len(bucket_monthlies) > max_monthly_backups:
              print("We have to remove a monthly backup", bucket_monthlies[0])
   except Exception as e:
      print("Error: ", e)
      raise
  else:
    print "not named"
