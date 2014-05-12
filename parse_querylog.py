#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

from common import common, database
from querylogs import querylogs
import argparse
import os


""" Start parsing input """
parser = argparse.ArgumentParser(description='Edurep querylogs parsing script')
parser.add_argument('-c', '--config', nargs=1, help='Config file', metavar='configfile', dest='configfile')
parser.add_argument('-p', '--path', nargs=1, help='Path of logfiles', metavar='path', dest='path')

args = parser.parse_args()

logfilepath = args.path[0]

""" Validating input """
if args.configfile:
    configfile = args.configfile[0]
    if not os.path.isfile(configfile):
        parser.error('Invalid config file: ' + configfile)
else:
    configfile = "metadateerrobot.cfg"


""" get config for source """
try:
    config = common.getConfig(configfile,"metadata")
except:
    parser.error('Invalid source: ' + source)

ParseQuerylog = querylogs.ParseQueryLog(config,logfilepath)
