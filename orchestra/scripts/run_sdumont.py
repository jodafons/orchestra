#!/usr/bin/env python3
import sys, os
import argparse
from Gaugi.messenger import LoggingLevel, Logger
from orchestra.constants import HOUR, MINUTE

logger = Logger.getModuleLogger("orchestra_create")
parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()


parser.add_argument('-s','--schedule_time', action='store',
                    dest='schedule_time', required = False, type=int, default=-1,
                    help = "The time in minutes to configurate the schedule clock calc. time.")


parser.add_argument('-q','--queue', action='store',
                    dest='queue', required = True,
                    help = "The SDUMONT queue name.")



if len(sys.argv)==1:
  logger.info(parser.print_help())
  sys.exit(1)

args = parser.parse_args()


if args.queue == 'nvidia':
  timeout = 40*HOUR
elif args.queue == 'cpu_small':
  timeout = 64*HOUR
elif args.queue == 'cpu':
  timeout = 88*HOUR
elif args.queue == 'gdl':
  timeout = 40*HOUR
else:
  logger.info( "The queue not defined." )


from orchestra import Pilot, Schedule, LCGRule, Cluster
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.constants import HOUR


max_update_time = None if args.schedule_time < 0 else args.schedule_time * MINUTE


# Create all services
schedule      = Schedule( "Schedule", LCGRule(), max_update_time=max_update_time)
db            = OrchestraDB(cluster=Cluster.SDUMONT)

from orchestra.subprocess.Orchestrator import Orchestrator
orchestrator  = Orchestrator()

# create the pilot
pilot = Pilot( db, schedule, orchestrator, bypass_gpu_rule=True ,
               cluster= Cluster.SDUMONT ,
               queue_name = args.queue,
               timeout = timeout )


# start!
pilot.initialize()
pilot.execute()
pilot.finalize()


