
from orchestra import Schedule, Pilot, LCGRule, Status
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.kubernetes import Orchestrator
from orchestra.constants import *
from orchestra.slots import *

url = "postgres://postgres:postgres@localhost:5432/lps_dev"

# Create DB API
db  = OrchestraDB( cluster=Cluster.LPS, url=url)

# Create all services
schedule      = Schedule( "Schedule", LCGRule() )

# Create the state machine
schedule.add_transiction( source=Status.REGISTERED, destination=Status.TESTING    , trigger=['all_jobs_are_registered', 'assigned_one_job_to_test']         )
schedule.add_transiction( source=Status.TESTING   , destination=Status.TESTING    , trigger='test_job_still_running'                                        )
schedule.add_transiction( source=Status.TESTING   , destination=Status.BROKEN     , trigger=['test_job_fail','broken_all_jobs','send_email_task_broken','start_timer'] )
schedule.add_transiction( source=Status.BROKEN    , destination=Status.REGISTERED , trigger='retry_all_jobs'                                                )
schedule.add_transiction( source=Status.TESTING   , destination=Status.RUNNING    , trigger=['test_job_pass','assigned_all_jobs']                           )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.DONE       , trigger=['all_jobs_are_done', 'send_email_task_done','start_timer']     )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.FINALIZED  , trigger=['all_jobs_ran','send_email_task_finalized','start_timer']      )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.KILL       , trigger='kill_all_jobs'                                                 )

# Machine state hack for now
schedule.add_transiction( source=Status.RUNNING   , destination=Status.RUNNING    , trigger='check_not_allow_job_status_in_running_state'                   )
schedule.add_transiction( source=Status.FINALIZED , destination=Status.RUNNING    , trigger='retry_all_failed_jobs'                                         )
schedule.add_transiction( source=Status.KILL      , destination=Status.KILLED     , trigger=['all_jobs_were_killed','send_email_task_killed','start_timer'] )
schedule.add_transiction( source=Status.KILLED    , destination=Status.REGISTERED , trigger='retry_all_jobs'                                                )


schedule.add_transiction( source=Status.BROKEN          , destination=Status.REMOVED         , trigger='remove_this_task')
schedule.add_transiction( source=Status.FINALIZED       , destination=Status.REMOVED         , trigger='remove_this_task')
schedule.add_transiction( source=Status.KILLED          , destination=Status.REMOVED         , trigger='remove_this_task')
schedule.add_transiction( source=Status.DONE            , destination=Status.REMOVED         , trigger='remove_this_task')



schedule.add_transiction( source=Status.BROKEN    , destination=Status.TO_BE_REMOVED  ,
    trigger=['passed_five_days','send_email_this_task_will_be_removed_in_five_days', 'assigned_task_to_be_removed'])

schedule.add_transiction( source=Status.FINALIZED , destination=Status.TO_BE_REMOVED  ,
    trigger=['passed_five_days','send_email_this_task_will_be_removed_in_five_days', 'assigned_task_to_be_removed'])

schedule.add_transiction( source=Status.KILLED    , destination=Status.TO_BE_REMOVED  ,
    trigger=['passed_five_days','send_email_this_task_will_be_removed_in_five_days', 'assigned_task_to_be_removed'])

schedule.add_transiction( source=Status.DONE      , destination=Status.TO_BE_REMOVED  ,
    trigger=['passed_five_days','send_email_this_task_will_be_removed_in_five_days', 'assigned_task_to_be_removed'])

schedule.add_transiction( source=Status.TO_BE_REMOVED, destination=Status.TO_BE_REMOVED_SOON ,
    trigger=['passed_nine_days','send_email_this_task_will_be_removed_tomorrow', 'assigned_task_to_be_removed_soon'])

schedule.add_transiction( source=Status.TO_BE_REMOVED_SOON, destination=Status.REMOVED  ,
    trigger=['passed_ten_days' ,'send_delete_signal','remove_this_task','assigned_task_to_be_removed_soon'])



#orchestrator  = Orchestrator( "../data/job_template.yaml",  "../data/lps_cluster.yaml" )
orchestrator  = Orchestrator( "/home/rancher/.cluster/orchestra/external/partitura/data/job_template.yaml",
                              "/home/rancher/.cluster/orchestra/external/partitura/data/lps_cluster.yaml" )

# create the pilot
pilot = Pilot(db, schedule, orchestrator,
              timeout = None, # run forever
              cluster=Cluster.LPS,
              max_update_time = 0.1*MINUTE )


# Set all queues
pilot.add( Slots("CPU", Cluster.LPS, "cpu_small") )
pilot.add( Slots("CPU", Cluster.LPS, "cpu_large") )
pilot.add( Slots("CPU", Cluster.LPS, "nvidia", gpu=True) )






postman = pilot.postman()


import traceback
try:
  pilot.run()
except Exception as e:
  print(e)
  subject = "[Cluster LPS] (ALARM) Orchestra stop"
  message=traceback.format_exc()
  postman.sendNotification('jodafons',subject,message)
  print(message)


