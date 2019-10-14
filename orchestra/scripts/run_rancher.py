

from ringerdb import RingerDB
url = 'postgres://ringer:6sJ09066sV1990;6@postgres-ringer-db.cahhufxxnnnr.us-east-2.rds.amazonaws.com/ringer'



from orchestra import *


# Create all services
schedule      = Schedule( "Schedule", LCGRule())
db            = RingerDB('jodafons', url)

orchestrator  = Orchestrator( "../data/job_template.yaml",  "../data/lps_cluster.yaml" )


pilot = Pilot( db, schedule, orchestrator )
cpu = CPUSlots( "CPU" , 10 ) 
gpu = GPUSlots( "CPU" , [5] ) 


pilot.setSlots(cpu)
pilot.setSlots(gpu)

pilot.initialize()
pilot.execute()
pilot.finalize()


