#!/usr/bin/env python

from BeautifulSoup import *
import logging
import sys
import urllib2
from optparse import OptionParser
from sys import stderr

def parse_args():
  parser = OptionParser(usage="check_mesos <mesos_master_url>",
      add_help_option=True)
  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)
  return (args[0])

def main():
  master = parse_args()
  check_mesos_url(master)

def check_mesos_url(url):
  #url = "http://" + master + ":8080"
  response = urllib2.urlopen(url)
  if response.code != 200:
    print "Mesos master " + url + " returned " + str(response.code)
    return -1
  master_html = response.read()
  return check_mesos_html(master_html)

def check_mesos_html(mesos_html):
  ## Find number of cpus from status page
  html_soup = BeautifulSoup(mesos_html)
  cpus_str = html_soup.findAll('td')[2].contents[0]
  mesos_num_cpus = int(cpus_str.strip("CPUs"))

  print "Mesos master reports " + str(mesos_num_cpus) + " CPUs"

if __name__ == "__main__":
  main()
