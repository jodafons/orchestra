#!/usr/bin/env python

import argparse
import sys,os


parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()


parser.add_argument('-c','--configFile', action='store',
        dest='configFile', required = True,
            help = "The job config file that will be used to configure the job (sort and init).")

parser.add_argument('-d','--dataFile', action='store',
        dest='dataFile', required = False, default = None,
            help = "The data/target file used to train the model.")

parser.add_argument('-r','--refFile', action='store',
        dest='refFile', required = False, default = None,
            help = "The reference file.")

parser.add_argument('-t', '--tag', action='store',
        dest='tag', required = True, default = None,
            help = "The tuning tag in the tuning branch in ringertunings repository")

parser.add_argument('-b', '--branch', action='store',
        dest='branch', required = True, default = None,
            help = "The tuning branch in ringetunings repository")

parser.add_argument('-v', '--volume', action='store',
        dest='volume', required = True, default = None,
            help = "The output path")



if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)

args = parser.parse_args()


def run(command):
  return True if os.system(command) == 0 else False


# we presume that this will be executed inside of the volume path given by the orchestra
if run("cd %s"%args.volume):

  # remove everything inside of the volume
  run("rm -rf *")

  # update saphyra
  run("pip install --upgrade saphyra")
  run("git clone https://github.com/jodafons/ringer.git && cd ringer && git checkout %s"%(args.branch))
  run("cd ..")
  run("python ringer/versions/%s/job_tuning.py -d %s -v %s -c %s -r %s"%(args.tag, args.dataFile, args.volume, args.configFile, args.refFile) )
  run('rm -rf ringer')

  sys.exit(0)
else:
  print('The volume (%s) path does not exist.', args.volume)
  sys.exit(1)









