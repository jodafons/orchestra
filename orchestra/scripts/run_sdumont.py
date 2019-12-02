#!/usr/bin/env python3
import sys, os
import argparse
from Gaugi.messenger import LoggingLevel, Logger
from orchestra.constants import HOUR, MINUTE

logger = Logger.getModuleLogger("orchestra_create")
parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()



parser.add_argument('-q','--queue', action='store',
                    dest='queue', required = True,
                    help = "The SDUMONT queue name.")


parser.add_argument('--as_server', action='store_true',
                    dest='as_server', required = False, default = False,
                    help = "Run as server. Disable slots and run only the schedule.")



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


print ("Queue name: %s"%args.queue)

from orchestra import Pilot, Schedule, LCGRule , NoRule, Cluster
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.constants import HOUR


if args.as_server:
  print( "Run as server...")
  # Create all services
  schedule      = Schedule( "Schedule", LCGRule(), max_update_time=0.5*MINUTE)
  db            = OrchestraDB(cluster=Cluster.SDUMONT)
  
  from orchestra.subprocess.Orchestrator import Orchestrator
  orchestrator  = Orchestrator()
  
  # create the pilot
  pilot = Pilot( db, schedule, orchestrator, 
                 bypass_gpu_rule=True ,
                 run_slots = False,
                 update_task_boards = True,
                 cluster= Cluster.SDUMONT , # cluster
                 queue_name = args.queue, # queue name
                 timeout = None, # run forever
                 )

else:

  # Create all services
  schedule      = Schedule( "Schedule", NoRule(), max_update_time=None)
  db            = OrchestraDB(cluster=Cluster.SDUMONT)

  from orchestra.subprocess.Orchestrator import Orchestrator
  orchestrator  = Orchestrator()

  # create the pilot
  pilot = Pilot( db, schedule, orchestrator, 
                 bypass_gpu_rule=True ,
                 run_slots = True,
                 update_task_boards = False,
                 cluster= Cluster.SDUMONT ,
                 queue_name = args.queue,
                 timeout = timeout )


# start!
pilot.initialize()
pilot.execute()
pilot.finalize()


