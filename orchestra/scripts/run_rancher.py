
from orchestra import *
from ringerdb import RingerDB


# 1 core per job with 5GB
MAX_PODS_NODE_02  = 1
MAX_PODS_NODE_03  = 12
MAX_PODS_NODE_04  = 12
MAX_PODS_NODE_05  = 6
MAX_PODS_NODE_06  = 6
MAX_PODS_NODE_07  = 12
MAX_PODS_NODE_08  = 8
MAX_PODS_CESSY    = 8
MAX_PODS_MARSELHA = 8
MAX_PODS_VERDUN   = 8



cpu_pods = []
cpu_pods.extend(  [CPUNode('node02')    for _ in range(MAX_PODS_NODE_02)]    )
cpu_pods.extend(  [CPUNode('node03')    for _ in range(MAX_PODS_NODE_03)]    )
cpu_pods.extend(  [CPUNode('node04')    for _ in range(MAX_PODS_NODE_04)]    )
cpu_pods.extend(  [CPUNode('node05')    for _ in range(MAX_PODS_NODE_05)]    )
cpu_pods.extend(  [CPUNode('node06')    for _ in range(MAX_PODS_NODE_06)]    )
cpu_pods.extend(  [CPUNode('node07')    for _ in range(MAX_PODS_NODE_07)]    )
cpu_pods.extend(  [CPUNode('node08')    for _ in range(MAX_PODS_NODE_08)]    )
cpu_pods.extend(  [CPUNode('cessy')     for _ in range(MAX_PODS_CESSY)  ]    )
cpu_pods.extend(  [CPUNode('marselha')  for _ in range(MAX_PODS_MARSELHA)]   )



gpu_pods = [
              GPUNode( 'node04'     , 0 ),
              GPUNode( 'cessy'      , 0 ),
              GPUNode( 'marselha'   , 0 ),
              GPUNode( 'verdun'     , 0 ),
              GPUNode( 'verdun'     , 1 ),
            ]





#url = 'postgres://ringer:6sJ09066sV1990;6@postgres-ringer-db.cahhufxxnnnr.us-east-2.rds.amazonaws.com/ringer'
url = 'postgres://lps:DuQDYsBP@postgres-lps-cluster-db.cahhufxxnnnr.us-east-2.rds.amazonaws.com/postgres'


# Create all services
schedule      = Schedule( "Schedule", LCGRule())
db            = RingerDB(url)
orchestrator  = Orchestrator( "../data/job_template.yaml",  "../data/lps_cluster.yaml" )

# create the pilot
pilot = Pilot( db, schedule, orchestrator, bypass_gpu_rule=True )

# Set all slots (GPU and CPU)
pilot.setCPUSlots( Slots("CPUSlots", cpu_pods) )
pilot.setGPUSlots( Slots("GPUSlots", gpu_pods) )

# start!
pilot.initialize()
pilot.execute()
pilot.finalize()


