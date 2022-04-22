



import argparse
import sys,os
import traceback
import json
import time

parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()


parser.add_argument('-j','--job', action='store',
        dest='job', required = True,
            help = "The job config file that will be used to configure the job (sort and init).")

parser.add_argument('-v','--volume', action='store',
        dest='volume', required = False, default = None,
            help = "The volume output.")

if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)


args = parser.parse_args()

print('INICIOU....')
job  = json.load(open(args.job, 'r'))
sort = job['sort']

time.sleep(60)

print('ACABOU JOAOA')



