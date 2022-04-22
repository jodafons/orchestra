
__all__ = ["Consumer"]



import os, subprocess, traceback, psutil
from colorama import Back, Fore
from orchestra import State
import threading
import datetime
import time




class Clock(threading.Thread):

  def __init__(self, proc, tic):
    threading.Thread.__init__(self)
    self.__proc = proc
    self.__tic = tic
    self.__toc = None
    self.__stop = threading.Event()

  def is_alive(self):
    return True if self.__proc.poll() is None else False 
  
  def run(self):
    while not self.__stop.isSet():
      if not self.is_alive():
        self.__toc = datetime.datetime.now()
        self.__stop.set()

  def stop(self):
    self.__stop.set()

  def resume(self):
    if not self.__toc:
      return (datetime.datetime.now() - self.__tic).total_seconds()
    else:
      return (self.__toc - self.__tic).total_seconds()




class Consumer:

  #
  # Constructor
  #
  def __init__(self, job, slot, extra_envs={}):

    self.job = job
    self.slot = slot
    self.volume = job.volume()
    self.command = job.command
    self.pending=True
    self.broken=False
    self.killed=False

    self.env = os.environ.copy()
    self.env["VOLUME"] = self.volume
    self.env["CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"
    self.env["CUDA_VISIBLE_DEVICES"]=str(slot.device)
    self.env["TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'
   
    for key, value in extra_envs.items():
      self.env[key]=value

    # process
    self.__proc = None
    self.__proc_stat = None
    self.__clock = None
   



  def run(self):

    try:
      os.makedirs(self.volume, exist_ok=True)
      self.pending=False
      self.killed=False
      self.broken=False
      command = 'cd %s' % self.volume + ' && '
      command+= self.command

      print(Fore.YELLOW +'Lauching job...')
      print(Fore.GREEN + command)
      #tic = datetime.datetime.now()
      self.__proc = subprocess.Popen(command, env=self.env, shell=True)
      time.sleep(2)
      #self.__clock = Clock(self.__proc, tic)
      #self.__clock.start()
      self.__proc_stat = psutil.Process(self.__proc.pid)
      return True

    except Exception as e:
      traceback.print_exc()
      print(e)
      self.broken=True
      return False





  def is_alive(self):
    return True if (self.__proc and self.__proc.poll() is None) else False


  def resume(self):
    return self.__clock.resume() if self.__clock else 0


  def kill(self):

    if self.is_alive():
      children = self.__proc_stat.children(recursive=True)
      for child in children:
        print('kill child pid %d'%child.pid)
        p=psutil.Process(child.pid)
        p.kill()
      self.__proc.kill()
      self.killed=True
      return True
    else:
      return False


  def ping(self):
    self.job.ping()

  #
  # Get the consumer state
  #
  def state(self):

    if self.is_alive():
      return State.RUNNING

    elif self.pending:
      return State.PENDING
    
    elif self.killed:
      return State.KILLED

    elif self.broken:
      return State.BROKEN
    
    elif (self.__proc.returncode and  self.__proc.returncode>0):
      return State.FAILED
      
    else:
      return State.DONE

