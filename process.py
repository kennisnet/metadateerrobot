#!/usr/bin/python2
# -*- coding: utf-8 -*-

from common import common
import os.path
from sys import exit
import argparse


""" Start parsing input """
parser = argparse.ArgumentParser(description='Metadateerrobot process starter')
parser.add_argument('-p', '--part', nargs=1, help='proces part', metavar='part', dest='part')
parser.add_argument('-a', '--action', nargs=1, help='insert or delete', metavar='action', dest='action')
parser.add_argument('-c', '--config', nargs=1, help='config file', metavar='configfile', dest='configfile')
parser.add_argument('-o', '--option', nargs=1, help='option for action', metavar='option', dest='option')

args = parser.parse_args()

if args.configfile:
    configfile = args.configfile[0]
    if not os.path.isfile(configfile):
        parser.error('Invalid config file: ' + configfile)
else:
    configfile = "metadateerrobot.cfg"

if args.part:
    part = args.part[0]

    if args.action:
        action = args.action[0]
    else:
        parser.error('Input a valid action')

    """ get config for part """
    try:
        config = common.getConfig(configfile,part)
    except:
        parser.error('Invalid part: ' + part)

    """ load and start process class """
    try:
        process = common.importFrom(part,'process')
        P = process.Process(config,action)
    except:
        print("Cannot load module for part: " + part)
        exit()

    P.start()

else:
    parser.error('Input a valid part')