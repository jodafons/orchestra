
from orchestra import Schedule, Pilot, LCGRule
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.kubernetes import Orchestrator
from orchestra.constants import *



# Create all services
schedule      = Schedule( "Schedule", LCGRule(),  max_update_time = 0.5*MINUTE )

# Create the state machine
schedule.add_transiction( source=Status.REGISTERED, destination=Status.TESTING    , trigger=['all_jobs_are_registered', 'assigned_one_job_to_test'] )
schedule.add_transiction( source=Status.TESTING   , destination=Status.TESTING    , trigger='test_job_still_running'                                )
schedule.add_transiction( source=Status.TESTING   , destination=Status.BROKEN     , trigger='test_job_fail'                                         )
schedule.add_transiction( source=Status.BROKEN    , destination=Status.REGISTERED , trigger='retry_all_jobs'                                        )
schedule.add_transiction( source=Status.TESTING   , destination=Status.RUNNING    , trigger=['test_job_done','assigned_all_jobs']                   )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.DONE       , trigger='all_jobs_are_done'                                     )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.FINALIZED  , trigger='all_jobs_ran'                                          )
schedule.add_transiction( source=Status.FINALIZED , destination=Status.RUNNING    , trigger='retry_all_jobs'                                        )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.KILL       , trigger='kill_all_jobs'                                         )
schedule.add_transiction( source=Status.KILL      , destination=Status.KILLED     , trigger='all_jobs_are_killed'                                   )
schedule.add_transiction( source=Status.KILLED    , destination=Status.REGISTERED , trigger='retry_all_jobs'                                        )











db            = OrchestraDB(cluster=Cluster.LPS)
#orchestrator  = Orchestrator( "../data/job_template.yaml",  "../data/lps_cluster.yaml" )
orchestrator  = Orchestrator( "/home/rancher/.cluster/orchestra/external/partitura/data/job_template.yaml",
                              "/home/rancher/.cluster/orchestra/external/partitura/data/lps_cluster.yaml" )

# create the pilot
pilot = Pilot(db, schedule, orchestrator,
              bypass_gpu_rule=True,
              run_slots = True,
              update_task_boards = True,
              timeout = None, # run forever
              queue_name = Queue.LPS, cluster=Cluster.LPS )


# start!
pilot.initialize()
pilot.execute()
pilot.finalize()


