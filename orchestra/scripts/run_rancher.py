
from orchestra import *
from orchestra.db import OrchestraDB


# 1 core per job with 5GB
MAX_PODS_NODE_02  = 0
MAX_PODS_NODE_03  = 6
MAX_PODS_NODE_04  = 6
MAX_PODS_NODE_05  = 2
MAX_PODS_NODE_06  = 2
MAX_PODS_NODE_07  = 5
MAX_PODS_NODE_08  = 1
MAX_PODS_CESSY    = 0
MAX_PODS_MARSELHA = 0
MAX_PODS_VERDUN   = 0


url = 'postgres://postgres:postgres@localhost:5432/postgres'


# Create all services
schedule      = Schedule( "Schedule", LCGRule())
db            = OrchestraDB(url)
orchestrator  = Orchestrator( "../data/job_template.yaml",  "../data/lps_cluster.yaml" )

# create the pilot
pilot = Pilot( db, schedule, orchestrator, bypass_gpu_rule=True )


# start!
pilot.initialize()
pilot.execute()
pilot.finalize()


