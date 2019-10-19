

from ringerdb import RingerDB
#url = 'postgres://ringer:6sJ09066sV1990;6@postgres-ringer-db.cahhufxxnnnr.us-east-2.rds.amazonaws.com/ringer'
url = 'postgres://lps:DuQDYsBP@postgres-lps-cluster-db.cahhufxxnnnr.us-east-2.rds.amazonaws.com/postgres'


from orchestra import *


# Create all services
schedule      = Schedule( "Schedule", LCGRule())
db            = RingerDB(url)
orchestrator  = Orchestrator( "../data/job_template.yaml",  "../data/lps_cluster.yaml" )


pilot = Pilot( db, schedule, orchestrator, bypass_gpu_rule=True )


cpu_nodes = 100
cpu = CPUSlots( "CPU" , cpu_nodes )



gpu_nodes = [
              # GPUNode( name, device )
              GPUNode( 'node04'     , 0 ),
              GPUNode( 'cessy'      , 0 ),
              GPUNode( 'marselha'   , 0 ),
            ]
gpu = GPUSlots( "CPU" , gpu_nodes )


pilot.setSlots(cpu)
pilot.setSlots(gpu)

pilot.initialize()
pilot.execute()
pilot.finalize()


