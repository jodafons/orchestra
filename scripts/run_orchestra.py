#!/usr/bin/env python3

from orchestra import *
import traceback
import argparse
import sys,os

from orchestra.mailing import Postman
from orchestra.db import Database
from orchestra import Schedule, compile, Pilot

from orchestra.utils import *

config = get_config()


parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()


config = get_config()


parser.add_argument('-u', '--postgres', action='store',
        dest='postgres', required = False, default=config['postgres'],
            help = "The postgres url to access the database (format: postgres://username:password@machine.cef22bckysso.us-east-1.rds.amazonaws.com:5432/dbname).")

parser.add_argument('-e','--from_email', action='store',
        dest='from_email', required = False, default=config['from_email'],
        help = "The email used to send messages.")

parser.add_argument('-t','--to_email', action='store',
        dest='to_email', required = False, default=config['to_email'],
        help = "The email used to receive messages.")

parser.add_argument('-p','--password', action='store',
        dest='password', required = False, default=config['password'],
        help = "The email used to send messages.")

parser.add_argument('-n','--nodename', action='store',
        dest='nodename', required = True, default = None,
            help = "The node name registered into the daatabase.")




if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)

args = parser.parse_args()


#
# Create the schedule
#

# Create the state machine


orch_path = os.environ['ORCH_PATH']



# create the postman
postman = Postman( args.fromemail, 
                   args.password , 
                   args.toemail, orch_path+'/orchestra/mailing/templates')

# create and connect to the database
db = Database(args.postgres)

# create the schedule
schedule = Schedule(db, postman)
compile(schedule)

# create the pilot
pilot = Pilot(args.nodename, db, schedule, postman, master=True )


while True:
  try:
    pilot.run()
  except Exception as e:
    print(e)
    subject = "[Cluster LPS] (ALARM) Orchestra stop"
    message=traceback.format_exc()
    postman.send(subject,message)
    print(message)
    

