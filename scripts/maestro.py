#!/usr/bin/env python3

import sys
import argparse

parser = argparse.ArgumentParser()
subparser = parser.add_subparsers(dest='mode')


from orchestra.db import OrchestraDB
from orchestra import Cluster
# create the database manager
db = OrchestraDB(cluster = Cluster.LPS)


from orchestra.maestro.parsers import DatasetParser, TaskParser
engine = [
            DatasetParser(db, subparser),
            TaskParser(db, subparser),
          ]

if len(sys.argv)==1:
  print(parser.print_help())
  sys.exit(1)

args = parser.parse_args()

# Run!
for e in engine:
  e.compile(args)



























