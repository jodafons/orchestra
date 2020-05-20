
__all__ = ["Pilot"]


from Gaugi import Logger, NotSet, StatusCode, Color
from Gaugi.messenger.macros import *

from sqlalchemy import and_, or_
from orchestra.db.models import *
from orchestra.slots import *
from orchestra.constants import *
from orchestra.utilities import *
from orchestra.enumerations import *
from orchestra import Postman


class Pilot(Logger):

  def __init__(self, db, schedule, orchestrator,
               cluster    = Cluster.LPS,
               timeout    = None,
               max_update_time=MAX_UPDATE_TIME):

    Logger.__init__(self)
    self.__db = db
    self.__schedule = schedule
    self.__orchestrator = orchestrator
    self.__cluster = cluster
    self.__timeout_clock = Clock( timeout )
    self.__clock = Clock(max_update_time)
    self.__queue = {}

    try:
      self.__postman = Postman()
    except:
      MSG_FATAL( self, "It's not possible to create the Postman service." )


  def add( self, slots ):
    self.__queue[slots.getQueueName()] = slots


  def checkTimeout(self):
    return self.__timeout_clock()


  def db(self):
    return self.__db


  def postman(self):
    return self.__postman


  def schedule(self):
    return self.__schedule


  def orchestrator(self):
    return self.__orchestrator


  #
  # Initialize the schedule service
  #
  def initialize(self):

    # connect to the sql database (service)
    # Setup the kubernetes orchestrator (service)
    # link db to schedule
    self.schedule().setCluster( self.__cluster )
    self.schedule().setDatabase( self.db() )
    self.schedule().setPostman( self.postman() )

    if self.schedule().initialize().isFailure():
      MSG_FATAL( self, "Not possible to initialize the Schedule tool. abort" )


    for queue , slot in self.__queue.items():
      slot.setDatabase( self.db() )
      slot.setOrchestrator( self.orchestrator() )
      if slot.initialize().isFailure():
        MSG_FATAL( self, "Not possible to initialize the %s slot tool. abort", queue )

    return StatusCode.SUCCESS



  def execute(self):


    for queue, slot in self.__queue.items():
      MSG_INFO(self, "Put all running jobs to run for queue with name %s", queue)
      self.treatRunningJobsBeforeStart(slot)


    # Infinite loop
    while True:

      if self.__clock():

        self.checkNodesHealthy()

        # Calculate all priorities for all REGISTERED jobs for each 5 minutes
        self.schedule().execute()

        # If in standalone mode, these slots will not in running mode. Only schedule will run.
        for queue , slot in self.__queue.items():

          if slot.isAvailable():
            njobs = slot.size() - slot.allocated()
            MSG_DEBUG( self, "We have %d slots available in %s queue" , njobs, queue )
            MSG_DEBUG(self,"There are slots available. Retrieving the first %d jobs from the CPU queue",njobs )
            jobs = self.schedule().getQueue(njobs, queue)

            while (slot.isAvailable()) and len(jobs)>0:
              slot.push_back( jobs.pop() )

          slot.execute()


        # If in standalone mode, this can be calculated or note. Depend on demand.
        MSG_DEBUG(self, "Calculate all task boards...")
        self.updateAllBoards()


    return StatusCode.SUCCESS


  def finalize(self):

    self.db().finalize()
    self.schedule().finalize()
    for queue , slot in self.__queue:
      queue.finalize()
    self.orchestrator().finalize()
    return StatusCode.SUCCESS


  def run(self):
    self.initialize()
    self.execute()
    self.finalize()
    return StatusCode.SUCCESS


  def treatRunningJobsBeforeStart(self , slot):

    jobs = self.schedule().getAllRunningJobs(slot.getQueueName())
    if len(jobs) > 0:
      for job in jobs:
        job.setStatus( Status.ASSIGNED )
        job.getTask().setStatus( Status.RUNNING )
      i=0
      while (slot.isAvailable()) and i<len(jobs):
        slot.push_back( jobs[i] )
        i+=1
      self.db().commit()
      slot.execute()


  #
  # This is for monitoring purpose. Should be used to dashboard view
  #
  def updateAllBoards( self ):

    for user in self.db().getAllUsers():
      # Get the number of tasks
      tasks = user.getAllTasks( self.__cluster)

      for task in tasks:
        board = self.db().session().query(Board).filter( Board.taskName==task.taskName ).first()
        board.jobs          = len(task.getAllJobs())
        board.queueName     = task.getQueueName()

        board.registered    = len(self.db().session().query(Job).filter( and_( Job.status==Status.REGISTERED, Job.taskId==task.id )).all())
        board.assigned      = len(self.db().session().query(Job).filter( and_( Job.status==Status.ASSIGNED  , Job.taskId==task.id )).all())
        board.testing       = len(self.db().session().query(Job).filter( and_( Job.status==Status.TESTING   , Job.taskId==task.id )).all())
        board.running       = len(self.db().session().query(Job).filter( and_( Job.status==Status.RUNNING   , Job.taskId==task.id )).all())
        board.done          = len(self.db().session().query(Job).filter( and_( Job.status==Status.DONE      , Job.taskId==task.id )).all())
        board.failed        = len(self.db().session().query(Job).filter( and_( Job.status==Status.FAILED    , Job.taskId==task.id )).all())
        board.broken        = len(self.db().session().query(Job).filter( and_( Job.status==Status.BROKEN    , Job.taskId==task.id )).all())
        board.kill          = len(self.db().session().query(Job).filter( and_( Job.status==Status.KILL      , Job.taskId==task.id )).all())
        board.killed        = len(self.db().session().query(Job).filter( and_( Job.status==Status.KILLED    , Job.taskId==task.id )).all())
        board.status        = task.status
        board.priority      = self.db().session().query(Job).filter( Job.taskId==task.id ).order_by(Job.priority.desc()).first().priority
        self.db().commit()




  def checkNodesHealthy(self):

    for node in self.orchestrator().getNodeStatus():

      MSG_INFO(self, "Checking %s healthy...", node['name'])

      njobs=0
      for queue, slot in self.__queue.items():
        machine = self.db().getMachine(self.__cluster, queue, node['name'])
        if machine:  njobs+=machine.jobs


      machineIsUnderPressure = (node["MemoryPressure"] or node["DiskPressure"])
      machineIsReady = node['Ready']
      machineIsRunning = ( njobs > 0 )


      # Node is up and running but suffering with pressure
      if machineIsRunning and (not machineIsReady  or machineIsUnderPressure  ):

        MSG_WARNING( self, "The node %s is not healthy.", node['name']                   )
        MSG_WARNING( self, "    Ready               : %s", str(machineIsReady)              )
        MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
        MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )
        MSG_WARNING( self, "Shutting it down..."                   )


        for queue, slot in self.__queue.items():
          machine = self.db().getMachine(self.__cluster, queue, node['name'])
          if machine:
            machine.jobs = 0

        self.db().commit()

        # Send the email to the admin
        for user in self.db().getAllUsers():
          if user.isAdmin():
            try:
              subject = ("[LPS Cluster] FATAL - %s unready")%(machine.getName())
              message = ("Node with name {} in unhealthy. Further information below:<br><br>* "+\
                  "Ready={}<br>* MemoryPressure={}<br>DiskPressure={}<br><br> This node will be disabled until it's fixed".format(
                node['name'],
                str(machineIsReady),
                str(node['MemoryPressure']),
                str(node['DiskPressure']),
              ))
              self.__postman.sendNotification(user.getUserName(), subject, message)
            except Exception as e:
              MSG_ERROR(self, e)
              MSG_ERROR(self, "Couldn't send warning mail.")



      # Node is not running nor ready, do nothing
      else:
        MSG_INFO( self, "The node %s is healthy.", node['name']            )
        MSG_INFO( self, "    Ready               : %s", str(machineIsReady)              )
        MSG_INFO( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
        MSG_INFO( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )






  def checkNodesHealthyWithLoadBalancer(self):

    nodes = self.orchestrator().getNodeStatus()
    #for node in nodes:

    #  MSG_INFO(self, "Checking %s healthy...", node['name'])
    #  # Get the node database
    #  machine = self.db().getMachine(self.__cluster, self.__queue_name, node['name'])

    #  machineIsUnderPressure = (node["MemoryPressure"] or node["DiskPressure"])

    #  machineIsRunning = ( machine.CPUJobs > 0 ) or ( machine.GPUJobs > 0 )


    #  # If max(GPU/CPU)Jobs is zero than the node will be shut donw
    #  machineIsReady = (node['Ready'] and ( (machine.maxCPUJobs > 0) or (machine.maxGPUJobs > 0) ) )

    #  # Node is probably down (not ready and is running)
    #  if ((not machineIsReady) and (machineIsRunning)):
    #    MSG_WARNING( self, "The node %s is not healthy.", node['name']                   )
    #    MSG_WARNING( self, "    Ready               : %s", str(machineIsReady)               )
    #    MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
    #    MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )
    #    MSG_WARNING( self, "Shutting it down..."                   )
    #    # Disable the node
    #    machine.CPUJobs = 0
    #    machine.GPUJobs = 0
    #    self.db().commit()
    #    # Send the email to the admin
    #    for user in self.db().getAllUsers():
    #      if user.isAdmin():
    #        try:
    #          subject = ("[LPS Cluster] FATAL - %s unready")%(machine.getName())
    #          message = ("Node with name {} in unhealthy. Further information below:<br><br>* "+\
    #              "Ready={}<br>* MemoryPressure={}<br>DiskPressure={}<br><br> This node will be disabled until it's fixed".format(
    #            machine.getName(),
    #            str(machineIsReady),
    #            str(node['MemoryPressure']),
    #            str(node['DiskPressure']),
    #          ))
    #          self.__postman.sendNotification(user.getUserName(), subject, message)
    #        except Exception as e:
    #          MSG_ERROR(self, e)
    #          MSG_ERROR(self, "Couldn't send warning mail.")

    #  # Should restart adding load to the node (it's ready but not running)
    #  elif (not(machineIsRunning) and machineIsReady):
    #    MSG_WARNING( self, "The node %s has recovered health.", node['name']                   )
    #    MSG_WARNING( self, "    Ready               : %s", str(machineIsReady)               )
    #    MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
    #    MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )
    #    MSG_WARNING( self, "Restablishing workloads..."                   )


    #    if machine.maxCPUJobs>0:
    #      machine.CPUJobs = 1

    #    # GPU slots should be always maximum
    #    machine.GPUJobs = machine.maxGPUJobs
    #    self.db().commit()


    #    # Send the email to the admin
    #    for user in self.db().getAllUsers():
    #      if user.isAdmin():
    #        try:
    #          subject = ("[LPS Cluster] INFO - %s restablished")%(machine.getName())
    #          message = ("Node with name {} has recovered health. Further information "+\
    #              "below:<br><br>* Ready={}<br>* MemoryPressure={}<br>DiskPressure={}<br><br> Setting CPUJobs to {} and GPUJobs to {}".format(
    #            machine.getName(),
    #            str(machineIsReady),
    #            str(node['MemoryPressure']),
    #            str(node['DiskPressure']),
    #            machine.CPUJobs,
    #            machine.GPUJobs
    #          ))
    #          self.__postman.sendNotification(user.getUserName(), subject, message)
    #        except Exception as e:
    #          MSG_ERROR(self, e)
    #          MSG_ERROR(self, "Couldn't send warning mail.")

    #  # Node is up and running but suffering with pressure
    #  elif (machineIsReady and (machineIsUnderPressure) and (machineIsRunning)):
    #    MSG_WARNING( self, "The node %s is under pressure.", node['name']                   )
    #    MSG_WARNING( self, "    Ready               : %s", str(machineIsReady)              )
    #    MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
    #    MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )
    #    MSG_WARNING( self, "Reducing load..."                   )
    #    # Reduce CPU load on node
    #    if machine.CPUJobs > 0:
    #      machine.CPUJobs -= 1

    #    self.db().commit()

    #  # Node is up and running without any health issues
    #  elif (machineIsReady and (not(machineIsUnderPressure)) and (machineIsRunning)):
    #    MSG_WARNING( self, "The node %s is running and healthy.", node['name']              )
    #    MSG_WARNING( self, "    Ready               : %s", str(machineIsReady)              )
    #    MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
    #    MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )

    #    # Increase load on node
    #    if machine.CPUJobs == machine.maxCPUJobs:
    #      MSG_WARNING( self, "Load already at full capacity.")
    #    elif machine.CPUJobs <  machine.maxCPUJobs:
    #      MSG_WARNING( self, "Increasing load...")
    #      machine.CPUJobs += 1

    #    self.db().commit()

    #  # Node is not running nor ready, do nothing
    #  else:
    #    MSG_WARNING( self, "The node %s is not running nor ready.", node['name']                   )
    #    MSG_WARNING( self, "    Ready               : %s", str(machineIsReady)               )
    #    MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
    #    MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )

























