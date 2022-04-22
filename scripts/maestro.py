#!/usr/bin/env python3

try:
  # in case we have root installed here
  import ROOT
  from ROOT import gROOT
  ROOT.PyConfig.IgnoreCommandLineOptions = True
  gROOT.SetBatch()
except:
  pass


import sys, os
import argparse
from orchestra.db import Database
from orchestra.parsers import PilotParser, DeviceParser, TaskParser
from orchestra.utils import get_config


parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest='mode')


config = get_config()

# create the database manager
db  = Database( config['postgres'] )


engine = [
            PilotParser(db, commands),
            DeviceParser(db, commands),
            TaskParser(db, commands),
          ]



if len(sys.argv)==1:
  print(parser.print_help())
  sys.exit(1)

args = parser.parse_args()

# Run!
for e in engine:
  e.compile(args)



























