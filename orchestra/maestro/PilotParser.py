
__all__ = ["PilotParser"]

from Gaugi import LoggingLevel, Logger
from Gaugi.macros import *
from Gaugi import StatusCode, Color, expand_folders
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
import traceback
import socket
import time
  
from orchestra.utils import getConfig
config = getConfig()


class PilotParser( Logger ):

  def __init__(self, db, args=None):

    Logger.__init__(self)
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
    # Dataset CLI
    if args.mode == 'pilot':
      if args.option == 'run':
        status, answer = self.run( args.node, args.master )
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      else:
        MSG_FATAL(self, "Not valid option.")




  #
  # List datasets
  #
  def run( self, nodename , master):

    email = config['email']
    password = config['password']
    module_orchestra_path = os.path.dirname(orchestra.__file__)

    # create the postman
    postman = Postman( email, password , module_orchestra_path+'/mailing/templates')

    # get the standard schedule state machine
    from orchestra import schedule

    node = self.__db.getNode( nodename )

    if master:
      node.setThisNodeAsMaster()
    else:
      node.setThisNodeAsSlave()

    if node is None:
      return (StatusCode.FATAL, "Node (%s) is not available into the database"%nodename )


    # create the pilot
    pilot = Pilot(node, self.__db, schedule, postman, node.isMaster() )

    # create allways two slots (cpu and gpu) by default
    pilot+=Slots(node, 'cpu' , gpu=False )
    pilot+=Slots(node, 'gpu' , gpu=True  )



    while True:
      
      try:
        pilot.run()
      except Exception as e:
        print(e)
        subject = "[Cluster LPS] (ALARM) Orchestra stop"
        message=traceback.format_exc()

        #for user in self.__db.getAllUsers():
        #  postman.send( user.email,subject,message)
        print(message)
        time.sleep(10)
        from orchestra.db import OrchestraDB
        # create the database manager
        self.__db.close()
        self.__db  = OrchestraDB( config['postgres'] )


    return (StatusCode.SUCCESS, "success..")


