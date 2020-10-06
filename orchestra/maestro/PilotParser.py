
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
    
    # create the postman
    postman = Postman( email, password , module_orchestra_path+'/mailing/templates')
    
    # get the standard schedule state machine
    from orchestra import schedule

    # create the pilot
    pilot = Pilot(nodename, self.__db, schedule, postman)
   
    
    node = self.__db.getNode( nodename )
    
    if node is None:
      return (StatusCode.FATAL, "Node (%s) is not available into the database"%nodename )


    # create allways two slots (cpu and gpu) by default
    pilot+=Slots(node, 'cpu' , gpu=False )
    pilot+=Slots(node, 'gpu' , gpu=True  )
    

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


