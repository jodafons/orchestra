
__all__ = ["DeviceParser"]

from orchestra.utils import *
import argparse
from prettytable import PrettyTable


class DeviceParser:

  def __init__(self, db, args=None):

    self.__db = db
    if args:

      create_parser = argparse.ArgumentParser(description = 'Device create command lines.' , add_help = False)
      create_parser.add_argument('-n', '--nodename', action='store', dest='nodename', required=True,
                                  help = "The name of the device.")
      create_parser.add_argument('-d','--device', action='store', dest='device', type=int, default=-1,
                                  help = "The device number")
      create_parser.add_argument('-s','--slots', action='store', dest='slots', type=int, default=1,
                                  help = "The number of slots for this device")
      create_parser.add_argument('-e','--enabled', action='store', dest='enabled', type=int, default=1,
                                  help = "The number of enabled slots for this device")

  

      # Delete dataset using the dataset CLI
      list_parser = argparse.ArgumentParser(description = 'List all users command lines.', add_help = False)


      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('list', parents=[list_parser])
      args.add_parser( 'device', parents=[parent] )



  def compile( self, args ):
    # Dataset CLI
    if args.mode == 'device':
      if args.option == 'create':
        ok, answer = self.create(args.nodename, args.device, args.slots, args.enabled)
        if not ok:
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer)

      elif args.option == 'list':
        ok, answer = self.list()
        if not ok:
          MSG_FATAL(answer)
        else:
          print(answer)

      else:
        MSG_FATAL( "Not valid option.")




  #
  # List datasets
  #
  def list( self ):

    t = PrettyTable([
                      'Node'     ,
                      'Device'   ,
                      'Type'     ,
                      'Slots'    ,
                      'Status'   , 
                      ])

    # Loop over all datasets inside of the username
    for device in self.__db.devices():

      t.add_row(  [device.nodename,
                   device.gpu,
                   'gpu' if device.gpu>=0 else 'cpu',
                   '%d/%d'%(device.enabled, device.slots),
                   'online' if device.is_alive() else 'offline',
                  ] )

    return (True, t)



  #
  # create a node
  #
  def create( self, nodename, device, slots, enabled):

    if not self.__db.create_device( nodename, enabled, slots, device=device ):
      return (False, 'Failed to create the node into the database')
    return (True, "Successfully created." )





