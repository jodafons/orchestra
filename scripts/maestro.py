#!/usr/bin/env python3

import sys
import argparse
from orchestra.db import OrchestraDB
from orchestra import Cluster
from orchestra.maestro.parsers import DatasetParser, TaskParser


parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest='mode')

from partitura import lps

# create the database manager
db  = OrchestraDB( lps.postgres_url, lps.cluster_volume , cluster=Cluster.LPS  )
engine = [
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



























