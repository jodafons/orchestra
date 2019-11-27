
from orchestra import Pilot
from orchestra.db import OrchestraDB


url = 'postgres://postgres:postgres@postgres.cahhufxxnnnr.us-east-2.rds.amazonaws.com:5432/postgres'


# Create all services
schedule      = Schedule( "Schedule", LCGRule(), calculate=False)
db            = OrchestraDB(url)

from orchestra.slurm import Orchestra
orchestrator  = Orchestrator()

# create the pilot
pilot = Pilot( db, schedule, orchestrator, bypass_gpu_rule=False , cluster= SDUMONT)


# start!
pilot.initialize()
pilot.execute()
pilot.finalize()


