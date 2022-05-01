
__all__ = ['MSG_INFO',
           'MSG_WARNING',
           'MSG_ERROR',
           'MSG_FATAL',
           'get_config',
           'test_job_locally',
           ]

import time, os, json
from colorama import init, Back, Fore

init(autoreset=True)


def MSG_INFO(message):
  print(Fore.GREEN + message)

def MSG_WARNING(message):
  print(Fore.YELLOW + message)

def MSG_ERROR(message):
  print(Fore.RED + message)

def MSG_FATAL(message):
  print(Back.RED + Fore.WHITE + message)




def get_config():
  # default
  fname = os.environ['HOME']+'/.orchestra.json'

  try:
    with open(fname,'r') as f:
      data = json.load(f)
      return data
  except OSError as e:
    print(e)
    MSG_ERROR("Could not open/read file: %s" % fname)





def test_job_locally( job ):
  from orchestra.core import Slot, Consumer
  from orchestra.enums import State
  slot = Slot('test',device=1)
  slot.enable()
  consumer = Consumer( job, slot, extra_envs={'LOCAL_TEST':'1'})
  job.state = State.PENDING
  while True:
      if consumer.state() == State.PENDING:
          if not consumer.run():
            return False
      elif consumer.state() == State.FAILED:
          return False
      elif consumer.state() == State.RUNNING:
          continue
      elif consumer.state() == State.DONE:
          job.state='registered'
          return True
      else:
          continue