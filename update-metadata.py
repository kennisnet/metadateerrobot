#!/usr/bin/python2
# -*- coding: utf-8 -*-

from common import common
from metadata import metadata
import argparse


""" Start parsing input """
parser = argparse.ArgumentParser(description='Edurep DB insert script')
parser.add_argument('-a', '--action', nargs=1, help='insert or delete', metavar='action', dest='action')
parser.add_argument('-c', '--config', nargs=1, help='config file', metavar='configfile', dest='configfile')
parser.add_argument('-i', '--identifier', nargs=1, help='record identifier', metavar='identifier', dest='identifier')
parser.add_argument('-p', '--path', nargs=1, help='record path', metavar='path', dest='path')

args = parser.parse_args()

action = args.action[0]
identifier = args.identifier[0]
path = args.path[0]

""" Validating input """
if args.configfile:
	configfile = args.configfile[0]
	if not os.path.isfile(configfile):
		parser.error('Invalid config file: ' + configfile)
else:
	configfile = "metadateerrobot.cfg"

if action != "insert" and action != "delete":
	parser.error('Action should be insert or delete: ' + action)

""" get config for source """
try:
	config = common.getConfig(configfile,"metadata")
except:
	parser.error('Invalid source: ' + source)

Update = metadata.UpdateMetadata(config,action,identifier,path)
