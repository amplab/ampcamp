#!/usr/bin/env python
# -*- coding: utf-8 -*-

from boto import *
from check_mesos import *

def main():
  conn = connect_ec2()
  res = conn.get_all_instances()
  # master reservations have only one node
  masters = [i for i in res if len(i.instances) == 1]
  master_instances = [m.instances[0] for m in masters]

  name_host = [(m.tags['cluster'], m.public_dns_name) for m in master_instances]
  
  for (name, host) in name_host:
    print name + " " + host
    master_url = "http://" + host + ":8080"
    check_mesos_url(master_url)

if __name__ == "__main__":
  main()
