
__all__ = ["PilotParser"]

import traceback, time, os, argparse

import orchestra
from orchestra import Schedule, Pilot
from orchestra.mailing import Postman
from orchestra.utils import *
from orchestra.utils import get_config
import socket

config = get_config()



class PilotParser:

  def __init__(self, db, args=None):

    self.__db = db
    if args:

      run_parser = argparse.ArgumentParser(description = 'Run pilot command lines.' , add_help = False)

      run_parser.add_argument('-n','--node', action='store',
               dest='node', required = False, default = socket.gethostname() ,
               help = "The node name registered into the database.")
      run_parser.add_argument('-m','--master', action='store_true',
               dest='master', required = False ,
               help = "This is a master branch. One node must be a master.")

      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')
      subparser.add_parser('run', parents=[run_parser])
      args.add_parser( 'pilot', parents=[parent] )



  def compile( self, args ):
    if args.mode == 'pilot':
      if args.option == 'run':
        self.run( args.node, args.master )
      else:
        MSG_FATAL("Not valid option.")




  #
  # List datasets
  #
  def run( self, nodename , master):
    
    fromEmail = config['from_email']
    toEmail   = config['to_email']
    password  = config['password']
    orch_path = os.path.dirname(orchestra.__file__)
    postman = Postman( fromEmail, password , toEmail, orch_path+'/mailing/templates')

    while True:
      try:

        self.__db.reconnect()
        # create the postman

        schedule = Schedule(self.__db, postman)
        from orchestra.Schedule import compile
        compile(schedule)

        # create the pilot
        pilot = Pilot(nodename, self.__db, schedule, postman, master=master )
        pilot.init()
        pilot.run()
        
      except Exception as e:
        traceback.print_exc()
        subject = "[Cluster LPS] (ALARM) Orchestra stop"
        message=traceback.format_exc()
        postman.send(subject,message)
        print(message)
        time.sleep(10)



