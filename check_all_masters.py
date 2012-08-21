#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from boto import *
from check_mesos import *

def is_active(instance):
  return (instance.state in ['pending', 'running', 'stopping', 'stopped'])

def main():
  conn = connect_ec2()
  res = conn.get_all_instances()
  # master reservations have only one node
  masters = [i for i in res if len(i.instances) == 1]
  master_instances = [m.instances[0] for m in masters if is_active(m.instances[0])]

  name_host = [(m.tags['cluster'], m.public_dns_name) for m in master_instances]
  
  for (name, host) in name_host:
    print(name + " " + host, end=' ')
    master_url = "http://" + host + ":8080"
    try:
      check_mesos_url(master_url)
    except Exception:
      print("Mesos master DOWN")

if __name__ == "__main__":
  main()
