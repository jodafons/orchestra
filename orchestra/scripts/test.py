
import numpy as np
import argparse
import sys,os


parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()


parser.add_argument('-c','--configFile', action='store',
            dest='configFile', required = True,
                        help = "The job config file that will be used to configure the job (sort and init).")

parser.add_argument('-o','--outputFile', action='store',
            dest='outputFile', required = False, default = None,
                        help = "The output tuning name.")

if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)

args = parser.parse_args()



import time

i=0
while True:
  print ("***********************************************************")
  i+=1
  if i >10000:
    break

