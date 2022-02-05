
__all__ = ["Clock", "getStatus", "getEnv", "getConfig",
           "start", "complete", "fail", "is_test_job"]

from Gaugi import Color
import time, os


class Clock(object):

  def __init__( self , maxseconds ):
    self.__maxseconds=maxseconds
    self.__then = None


  def __call__( self ):

    # Always return false since we considere that the current
    # time never will go to the end (infinite)
    if self.__maxseconds is None:
      return False

    if not self.__then:
      self.__then = time.time()
      return False
    else:
      now = time.time()
      if (now-self.__then) > self.__maxseconds:
        # reset the time
        self.__then = None
        return True
    return False

  def reset(self):
    self.__then=None



def getStatus(status):
  if status == 'registered':
    return Color.CWHITE2+"REGISTERED"+Color.CEND
  elif status == 'assigned':
    return Color.CWHITE2+"ASSIGNED"+Color.CEND
  elif status == 'testing':
    return Color.CGREEN2+"TESTING"+Color.CEND
  elif status == 'running':
    return Color.CGREEN2+"RUNNING"+Color.CEND
  elif status == 'done':
    return Color.CGREEN2+"DONE"+Color.CEND
  elif status == 'failed':
    return Color.CGREEN2+"DONE"+Color.CEND
  elif status == 'killed':
    return Color.CRED2+"KILLED"+Color.CEND
  elif status == 'finalized':
    return Color.CRED2+"FINALIZED"+Color.CEND
  elif status == 'broken':
    return Color.CRED2+"BROKEN"+Color.CEND
  elif status == 'hold':
    return Color.CRED2+"HOLD"+Color.CEND
  elif status == 'removed':
    return Color.CRED2+"REMOVED"+Color.CEND
  elif status == 'to_be_removed':
    return Color.CRED2+"REMOVING"+Color.CEND
  elif status == 'to_be_removed_soon':
    return Color.CRED2+"REMOVING"+Color.CEND



def getEnv( name ):
  return os.environ[name]



def getConfig():

  # default
  fname = getEnv("HOME")+'/.orchestra.json'
  import json
  try:
    with open(fname,'r') as f:
      data = json.load(f)
      return data
  except OSError as e:
    print(e)
    print("Could not open/read file: %s" % fname)


def start():
  basepath = os.getcwd()
  if os.path.exists(basepath+'/.complete'):
    os.remove(basepath+'/.complete')
  if os.path.exists(basepath+'/.failed'):
    os.remove(basepath+'/.failed')
  
def complete():
  basepath = os.getcwd()
  with open(basepath+'/.complete','w') as f:
    f.write('complete')

def fail():
  basepath = os.getcwd()
  with open(output+'/.failed','w') as f:
    f.write('failed')

def is_test_job():
  return True if os.getenv('LOCAL_TEST') else False

