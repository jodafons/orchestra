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
from orchestra.db import OrchestraDB
from orchestra.maestro import PilotParser, NodeParser, UserParser, DatasetParser, TaskParser
from orchestra.utils import getConfig

from Gaugi import fixArgParser
fixArgParser()

parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest='mode')


config = getConfig()

# create the database manager
db  = OrchestraDB( config['postgres'] )


engine = [
            PilotParser(db, commands),
            UserParser(db, commands),
            NodeParser(db, commands),
            DatasetParser(db, commands),
            TaskParser(db, commands),
          ]



if len(sys.argv)==1:
  print(parser.print_help())
  sys.exit(1)

args = parser.parse_args()

# Run!
for e in engine:
  e.compile(args)



























