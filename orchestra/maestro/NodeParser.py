
__all__ = ["NodeParser"]

from Gaugi.messenger import LoggingLevel, Logger
from Gaugi.messenger.macros import *
from Gaugi import StatusCode, Color, expandFolders

# Connect to DB
from orchestra.db import Task,Dataset,File,Job
from orchestra import Status, Signal, getStatus
from sqlalchemy import and_, or_
from prettytable import PrettyTable

# common imports
import glob
import numpy as np
import argparse
import sys,os
import hashlib




class NodeParser( Logger ):

  def __init__(self, db, args=None):

    Logger.__init__(self)
    self.__db = db
    if args:

      create_parser = argparse.ArgumentParser(description = 'Node create command lines.' , add_help = False)
      create_parser.add_argument('-n', '--name', action='store', dest='name', required=True,
                                  help = "The name of the node.")
      create_parser.add_argument('-ec','--enabledCPUSlots', action='store', dest='enabledCPUSlots', required=True,
                                  help = "The number of CPU enabled slots.")
      create_parser.add_argument('-mc','--maxNumberOfCPUSlots', action='store', dest='maxNumberOfCPUSlots', required=True,
                                  help = "The total number of CPU slots for this node.")

      create_parser.add_argument('-eg','--enabledGPUSlots', action='store', dest='enabledGPUSlots', required=True,
                                  help = "The number of GPU enabled slots.")
      create_parser.add_argument('-mg','--maxNumberOfGPUSlots', action='store', dest='maxNumberOfGPUSlots', required=True,
                                  help = "The total number of GPU  slots for this node.")



      delete_parser = argparse.ArgumentParser(description = 'Node remove command lines.', add_help = False)
      delete_parser.add_argument('-n', '--name', action='store', dest='name', required=True,
                                   help = "The node name to be removed")


      stop_parser = argparse.ArgumentParser(description = 'Node stop command lines.', add_help = False)
      stop_parser.add_argument('-n', '--name', action='store', dest='name', required=True,
                                   help = "The node name to be stop")


      # Delete dataset using the dataset CLI
      list_parser = argparse.ArgumentParser(description = 'List all users command lines.', add_help = False)


      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list', parents=[list_parser])
      subparser.add_parser('stop', parents=[stop_parser])
      args.add_parser( 'node', parents=[parent] )



  def compile( self, args ):
    # Dataset CLI
    if args.mode == 'node':
      if args.option == 'create':
        status, answer = self.create(args.name, args.enabledCPUSlots, args.maxNumberOfCPUSlots, args.enabledGPUSlots, args.maxNumberOfGPUSlots)
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      elif args.option == 'delete':
        status, answer = self.delete(args.name)
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      elif args.option == 'list':
        status, answer = self.list()
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          print(answer)

      else:
        MSG_FATAL(self, "Not valid option.")




  #
  # List datasets
  #
  def list( self ):

    t = PrettyTable([
                      Color.CGREEN2 + 'Node'      + Color.CEND,
                      Color.CGREEN2 + 'GPU Slots' + Color.CEND,
                      Color.CGREEN2 + 'CPU slots' + Color.CEND,
                      Color.CGREEN2 + 'Status'    + Color.CEND,
                      ])

    # Loop over all datasets inside of the username
    for node in self.__db.getAllNodes():

      t.add_row(  [node.name,
                   '%d/%d'%(node.enabledGPUSlots, node.maxNumberOfGPUSlots),
                   '%d/%d'%(node.enabledCPUSlots, node.maxNumberOfCPUSlots),
                   'online' if node.isAlive() else 'offline',
                  ] )

    return (StatusCode.SUCCESS, t)



  #
  # create a node
  #
  def create( self, nodename, enabledCPUSlots, maxNumberOfCPUSlots, enabledGPUSlots, maxNumberOfGPUSlots):

    if not self.__db.createNode( nodename, enabledCPUSlots, maxNumberOfCPUSlots, enabledGPUSlots, maxNumberOfGPUSlots ):
      return (StatusCode.FATAL, 'Failed to create the node into the database')

    return (StatusCode.SUCCESS, "Successfully created." )



  #
  # Delete node
  #
  def delete( self, nodename ):

    if self.__db.getNode( nodename ):
      try:
        self.session().query(Node).filter(Node.name==nodename).delete()
      except Exception as e:
        print(e)
        return (StatusCode.FATAL, "Failed to remove the node into the database." )
    else:
      return (StatusCode.FATAL, 'The node (%s) does not exist into the database'%nodename)

    return (StatusCode.SUCCESS, "Successfully removed." )



  def stop( self, nodename ):

    node = self.__db.getNode(nodename)
    if node:
      node.setSignal("stop")
    else:
      return (StatusCode.FATAL, 'The node (%s) does not exist into the database'%nodename)

    return (StatusCode.SUCCESS, "Successfully removed." )



