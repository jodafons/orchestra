
__all__ = ["PilotParser"]

from Gaugi.messenger import LoggingLevel, Logger
from Gaugi.messenger.macros import *
from Gaugi import StatusCode, Color, expandFolders
import orchestra
# Connect to DB
from orchestra import *
from sqlalchemy import and_, or_
from prettytable import PrettyTable

# common imports
import glob
import numpy as np
import argparse
import sys,os
import hashlib



config = getConfig()


class PilotParser( Logger ):

  def __init__(self, db, args=None):

    Logger.__init__(self)
    self.__db = db
    if args:
      
      run_parser = argparse.ArgumentParser(description = 'Run pilot command lines.' , add_help = False)

      run_parser.add_argument('-n','--node', action='store',
               dest='node', required = True, default = None,
               help = "The node name registered into the database.")

      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')
      subparser.add_parser('run', parents=[run_parser])
      args.add_parser( 'pilot', parents=[parent] )



  def compile( self, args ):
    # Dataset CLI
    if args.mode == 'pilot':
      if args.option == 'run':
        status, answer = self.run( args.node )
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      else:
        MSG_FATAL(self, "Not valid option.")




  #
  # List datasets
  #
  def run( self, nodename ):

    email = config['email']
    password = config['password']
    module_orchestra_path = os.path.dirname(orchestra.__file__)

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
    postman = Postman( email, password , module_orchestra_path+'/mailing/templates')
    
    # create the pilot
    pilot = Pilot(nodename, self.__db, schedule, postman)
   

    for node in self.__db.getAllNodes( nodename ):
      MSG_INFO( self, "Adding node(%s) for queue(%s). GPU is %s.", node.name, node.queueName, 'True' if node.isGPU else 'False' )
      pilot+=Slots(node.name, node.queueName , gpu=node.isGPU )
    
    
    try:
      pilot.run()
    except Exception as e:
      print(e)
      subject = "[Cluster LPS] (ALARM) Orchestra stop"
      message=traceback.format_exc()
      for user in self.__db.getAllUsers():
        postman.send( user.email,subject,message)
      print(message)
      return (StatusCode.FATAL, "fatal...")


    return (StatusCode.SUCCESS, "success..")


