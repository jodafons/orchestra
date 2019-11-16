#!/usr/bin/env python3

import argparse
from Gaugi.messenger import LoggingLevel, Logger
from Gaugi import load
import glob
import numpy as np
import argparse
import sys,os
import hashlib

logger = Logger.getModuleLogger("orchestra_delete")
parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()



parser.add_argument('-d','--dataset', action='store', dest='dataset', required=True,
                    help = "The dataset name")

parser.add_argument('-p','--path', action='store', dest='path', required=True,
                    help = "The absoluty path")



if len(sys.argv)==1:
  logger.info(parser.print_help())
  sys.exit(1)

args = parser.parse_args()



# Connect to DB
from orchestra.db import OrchestraDB
from orchestra.db import Dataset,File
from orchestra import Status
db = OrchestraDB( )



# check task policy
dataset = args.dataset
dataset = dataset.split('.')
if dataset[0] != 'user':
  logger.fatal('The task name must starts with: user.%USER.taskname.')
username = dataset[1]
if username in db.getAllUsers():
  logger.fatal('The username does not exist into the database. Please, report this to the db manager...')



# check if dataset exist into the database
if db.getDataset( username, args.dataset ):
  logger.fatal("The dataset exist into the database")


# Let's registry into the database
try:
  ds  = Dataset( username=username, dataset=args.dataset)
  # Loop over files
  from Gaugi import csvStr2List, expandFolders
  for path in expandFolders(args.path):
    print("Registry %s into %s"%(path,args.dataset))
    hash_object = hashlib.md5(str.encode(path))
    ds.addFile( File(path=path, hash=hash_object.hexdigest()) )
  db.createDataset(ds)
  db.commit()

except:
    logger.fatal("Impossible to registry the dataset(%s)", args.dataset)


db.close()















