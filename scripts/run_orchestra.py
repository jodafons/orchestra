
from orchestra import *

schedule = Schedule()

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


postgres_url = 'postgres://ringer:12345678@ringer.cef2wazkyxso.us-east-1.rds.amazonaws.com:5432/postgres'
email_login = 'jodafons@lps.ufrj.br'
email_password = '6sJ09066sV1990;6'


db  = OrchestraDB( postgres_url )

postman = Postman( email_login , email_password , '/Users/jodafons/Desktop/orchestra/orchestra/mailing/templates')


backend = Subprocess()


############################################################################################################################


# create the pilot
pilot = Pilot('verdun', db, schedule, backend, postman)


# Set all queues
pilot+=Slots("verdun", "cpu_small")
pilot+=Slots("verdun", "nvidia", gpu=True)



pilot.run()

#import traceback
#try:
#
#except Exception as e:
#  print(e)
#  subject = "[Cluster LPS] (ALARM) Orchestra stop"
#  message=traceback.format_exc()
#  postman.sendNotification('jodafons',subject,message)
#  print(message)
#

