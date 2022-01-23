#!/usr/bin/env python3

from orchestra import *
import traceback
import argparse
import sys,os


parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()


config = getConfig()

parser.add_argument('-u', '--postgres', action='store',
        dest='postgres', required = False, default=config['postgres'],
            help = "The postgres url to access the database (format: postgres://username:password@machine.cef22bckysso.us-east-1.rds.amazonaws.com:5432/dbname).")

parser.add_argument('-e','--email', action='store',
        dest='email', required = False, default=config['email'],
        help = "The email used to send messages.")

parser.add_argument('-p','--password', action='store',
        dest='password', required = False, default=config['password'],
        help = "The email used to send messages.")

parser.add_argument('-n','--nodename', action='store',
        dest='nodename', required = True, default = None,
            help = "The node name registered into the daatabase.")


parser.add_argument('--cpu', action='store',
        dest='cpu', required = False, default='cpu_small',
            help = "All cpu queue (cpu_large,cpu_small,...) separated by commam." )


parser.add_argument('--gpu', action='store',
        dest='gpu', required = False, default='cpu_small',
            help = "All gpu queue (nvidia,rtx,...) separated by commam." )





if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)

args = parser.parse_args()


#
# Create the schedule
#
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
schedule.add_transiction( source=Status.DONE      , destination=Status.REGISTERED , trigger='retry_all_jobs'                                                )



# create the postman
postman = Postman( args.email, args.password , getEnv('ORCHESTRA_PATH')+'/orchestra/mailing/templates')

# create and connect to the database
db = OrchestraDB(args.postgres)

# create the pilot
pilot = Pilot(args.nodename, db, schedule, postman)


# Set all slots 
for queue in args.cpu.split(','):
  pilot+=Slots(args.nodename, queue)

for queue in args.cpu.split(','):
  pilot+=Slots(args.nodename, queue, gpu=True)

while True:
  try:
    pilot.run()
  except Exception as e:
    print(e)
    subject = "[Cluster LPS] (ALARM) Orchestra stop"
    message=traceback.format_exc()
    for user in db.getAllUsers():
      postman.send( user.email,subject,message)
    print(message)
    

