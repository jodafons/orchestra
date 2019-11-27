
from orchestra import Pilot, Schedule, LCGRule, Cluster
from orchestra.db import OrchestraDB


u
l = 'postgres://postgres:postgres@postgres.cahhufxxnnnr.us-east-2.rds.amazonaws.com:5432/postgres'


# Create all services
schedule      = Schedule( "Schedule", LCGRule(), calculate=False)
db            = OrchestraDB(url)

from orchestra.slurm.Orchestrator import Orchestrator
orchestrator  = Orchestrator()

# create the pilot
pilot = Pilot( db, schedule, orchestrator, bypass_gpu_rule=False , cluster= Cluster.SDUMONT)


# start!
pilot.initialize()
pilot.execute()
pilot.finalize()


