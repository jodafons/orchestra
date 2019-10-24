#!/usr/bin/env python3

from Gaugi import Color
from orchestra.db import *
db = OrchestraDB()
tasks = db.session().query(TaskBoard).all()



def getStatus(status):
  if status == 'registered':
    return Color.CWHITE2+"REGISTERED"
  elif status == 'assigned':
    return Color.CWHITE2+"ASSIGNED"
  elif status == 'testing':
    return Color.CGREEN2+"TESTING"
  elif status == 'running':
    return Color.CGREEN2+"RUNNING"
  elif status == 'done':
    return Color.CGREEN2+"DONE"
  elif status == 'failed':
    return Color.CRED2+"FAILED"








# define the line template
line="+------------------+----------------------------------------------------------------------------------+----------+----------+----------+----------+----------+------------+"


print(line)
print ( "|     "+Color.CGREEN2+"username"+Color.CEND+
"     |                                     "+Color.CGREEN2+"taskname"+Color.CEND+
"                                     | "+Color.CGREEN2+"assigned"+Color.CEND+
" | "+Color.CGREEN2+"testing"+Color.CEND+
"  | "+Color.CGREEN2+"running"+Color.CEND+
"  | "+Color.CRED2+"failed"+Color.CEND+
"   |  "+Color.CGREEN2+"done"+Color.CEND+
"    | "+Color.CGREEN2+"status"+Color.CEND+"     |" )



print(line)
for task in tasks:
  print ( ("| {0:<16} | {1:<80} | {2:<8} | {3:<8} | {4:<8} | {5:<8} | {6:<8} | {7:<15}"+Color.CEND+" |" ).format( task.username, task.taskName, task.assigned, task.testing,
      task.running, task.failed, task.done, getStatus(task.status)))

print(line)














