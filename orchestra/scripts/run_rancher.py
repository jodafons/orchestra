

from ringerdb import RingerDB
url = 'postgres://ringer:6sJ09066sV1990;6@postgres-ringer-db.cahhufxxnnnr.us-east-2.rds.amazonaws.com/ringer'



from lpsgrid import *


# Create all services
schedule      = Schedule( "Schedule", LCGRule())
db            = RingerDB('jodafons', url)
orchestrator  = Orchestrator(  "../data/lps_cluster.yaml" )


pilot = Pilot( db, schedule, orchestrator )


cpu = CPUSlots( "CPU" , 20 ) 
gpu = GPUSlots( "CPU" , [1,2] ) 


pilot.setSlots(cpu)
pilot.setSlots(gpu)

pilot.initialize()
pilot.execute()

# Create the pilot
#
## Add slots
#pilot.setCPUSlot( Slots( "CPU" , maxlenght=100 ) )
#pilot.setGPUSlot(  Slots( "GPU" , maxlenght=1  , nodes=["node06"]) )
#
## Set services
#pilot.setDatabase( db )
#pilot.setSchedule( schedule )
#pilot.setOrchestrator( orchestrator )
#
#
## Start
#pilot.initialize()

#pilot.finalize()


