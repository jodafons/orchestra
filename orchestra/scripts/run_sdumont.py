
from orchestra import Pilot, Schedule, LCGRule, Cluster
from orchestra.db import OrchestraDB
from orchestra import Cluster



# Create all services
schedule      = Schedule( "Schedule", LCGRule(), calculate=False)
db            = OrchestraDB()

from orchestra.subprocess.Orchestrator import Orchestrator
orchestrator  = Orchestrator(cluster=Cluster=SDUMONT)

# create the pilot
pilot = Pilot( db, schedule, orchestrator, bypass_gpu_rule=False , cluster= Cluster.SDUMONT)


# start!
pilot.initialize()
pilot.execute()
pilot.finalize()


