#!/usr/bin/env python3

import sys, os
import argparse
from orchestra.db import OrchestraDB
from orchestra.maestro import NodeParser, UserParser, DatasetParser, TaskParser
from orchestra.utils import getEnv


parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest='mode')


postgres_url = getEnv("ORCHESTRA_POSTGRES_URL")

print(postgres_url)
# create the database manager
db  = OrchestraDB( postgres_url )


engine = [
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



























