
from orchestra import Schedule, Pilot, LCGRule, Status
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.kubernetes import Orchestrator
from orchestra.constants import *
from orchestra.slots import *
from orchestra import Postman

schedule = Schedule( "Schedule" )

# Create the state machine
schedule.add_transiction( source=Status.REGISTERED, destination=Status.TESTING    , trigger=['all_jobs_are_registered', 'assigned_one_job_to_test']         )
schedule.add_transiction( source=Status.TESTING   , destination=Status.TESTING    , trigger='test_job_still_running'                                        )
schedule.add_transiction( source=Status.TESTING   , destination=Status.BROKEN     , trigger=['test_job_fail','broken_all_jobs','send_email_task_broken']    )
schedule.add_transiction( source=Status.BROKEN    , destination=Status.REGISTERED , trigger='retry_all_jobs'                                                )
schedule.add_transiction( source=Status.TESTING   , destination=Status.RUNNING    , trigger=['test_job_pass','assigned_all_jobs']                           )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.DONE       , trigger=['all_jobs_are_done', 'send_email_task_done']                   )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.FINALIZED  , trigger=['all_jobs_ran','send_email_task_finalized']                    )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.KILL       , trigger='kill_all_jobs'                                                 )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.RUNNING    , trigger='check_not_allow_job_status_in_running_state'                   )
schedule.add_transiction( source=Status.FINALIZED , destination=Status.RUNNING    , trigger='retry_all_failed_jobs'                                         )
schedule.add_transiction( source=Status.KILL      , destination=Status.KILLED     , trigger=['all_jobs_were_killed','send_email_task_killed']               )
schedule.add_transiction( source=Status.KILLED    , destination=Status.REGISTERED , trigger='retry_all_jobs'                                                )





###########################################################################################################################
### Set everything to partitura private package

from partitura import lps

db  = OrchestraDB( postgres_url )

postman = Postman( email_login , email_password )



orchestrator = Orchestrator()


###########################################################################################################################


# create the pilot
pilot = Pilot(db, schedule, orchestrator, postman,
              timeout = None, # run forever
              cluster=Cluster.LPS,
              max_update_time = 0.1*MINUTE )


# Set all queues
pilot.add( Slots("CPU", Cluster.LPS, "cpu_large") )
pilot.add( Slots("GPU", Cluster.LPS, "nvidia", gpu=True) )




import traceback
try:
  pilot.run()
except Exception as e:
  print(e)
  subject = "[Cluster LPS] (ALARM) Orchestra stop"
  message=traceback.format_exc()
  postman.sendNotification('jodafons',subject,message)
  print(message)


