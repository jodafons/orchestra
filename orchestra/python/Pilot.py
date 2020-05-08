
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
               bypass_gpu_rule=False,
               cluster=Cluster.LPS,
               update_task_boards=True,
               timeout=None,
               run_slots = True,
               queue_name = Queue.LPS,
               max_update_time=MAX_UPDATE_TIME):

    Logger.__init__(self)
    self.__cpu_slots = Slots("CPU", cluster, queue_name)
    self.__gpu_slots = Slots("GPU", cluster, queue_name, gpu=True)
    self.__db = db
    self.__schedule = schedule
    self.__orchestrator = orchestrator
    self.__bypass_gpu_rule = bypass_gpu_rule
    self.__cluster = cluster
    self.__queue_name = queue_name
    self.__update_task_boards = update_task_boards
    self.__timeout_clock = Clock( timeout )
    self.__run_slots = run_slots
    self.__clock = Clock(max_update_time)

    try:
      self.__postman = Postman()
    except:
      MSG_FATAL( self, "It's not possible to create the Postman service." )


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


  def cpuSlots(self):
    return self.__cpu_slots


  def gpuSlots(self):
    return self.__gpu_slots


  def initialize(self):

    # connect to the sql database (service)
    # Setup the kubernetes orchestrator (service)
    # link db to schedule
    self.schedule().setCluster( self.__cluster )
    self.schedule().setDatabase( self.db() )
    self.schedule().setPostman( self.postman() )

    if self.schedule().initialize().isFailure():
      MSG_FATAL( self, "Not possible to initialize the Schedule tool. abort" )

    # link orchestrator/db to slots
    self.cpuSlots().setDatabase( self.db() )
    self.cpuSlots().setOrchestrator( self.orchestrator() )
    if self.cpuSlots().initialize().isFailure():
      MSG_FATAL( self, "Not possible to initialize the CPU slot tool. abort" )

    # link orchestrator/db to slots
    self.gpuSlots().setDatabase( self.db() )
    self.gpuSlots().setOrchestrator( self.orchestrator() )
    if self.gpuSlots().initialize().isFailure():
      MSG_FATAL( self, "Not possible to initialize the GPU slot tool. abort" )


    return StatusCode.SUCCESS



  def execute(self):

    if self.__run_slots:
      self.treatRunningJobsBeforeStart()

    # Infinite loop
    while True:

      if self.__clock():

        self.checkNodesHealthy()

        # Calculate all priorities for all REGISTERED jobs for each 5 minutes
        self.schedule().execute()

        # If in standalone mode, these slots will not in running mode. Only schedule will run.
        if self.__run_slots:

          if self.gpuSlots().isAvailable():
            njobs = self.gpuSlots().size() - self.gpuSlots().allocated()
            MSG_DEBUG( self, "We have %d GPU slots available" , njobs )
            if self.__bypass_gpu_rule:
              MSG_DEBUG(self,"There are GPU slots available. Retrieving the first %d jobs from the CPU queue since bypass gpu rule is True",njobs )
              jobs = self.schedule().getCPUQueue(njobs)
            else:
              MSG_DEBUG(self,"There are GPU slots available. Retrieving the first %d jobs from the GPU queue.",njobs )
              jobs = self.schedule().getGPUQueue(njobs)
            while (self.gpuSlots().isAvailable()) and len(jobs)>0:
              self.gpuSlots().push_back( jobs.pop() )
          else:
            MSG_DEBUG( self, "There is no GPU slots availale" )

          if self.cpuSlots().isAvailable():
            ## Prepare jobs for CPU slots only
            njobs = self.cpuSlots().size() - self.cpuSlots().allocated()
            MSG_DEBUG( self, "We have %d GPU slots available" , njobs )
            MSG_DEBUG(self,"There are slots available. Retrieving the first %d jobs from the CPU queue",njobs )
            jobs = self.schedule().getCPUQueue(njobs)

            while (self.cpuSlots().isAvailable()) and len(jobs)>0:
              self.cpuSlots().push_back( jobs.pop() )
          else:
            MSG_DEBUG( self, "There is no CPU slots availale" )

          ## Run the pilot for cpu queue
          self.cpuSlots().execute()
          ## Run the pilot for gpu queue
          self.gpuSlots().execute()

        # If in standalone mode, this can be calculated or note. Depend on demand.
        if self.__update_task_boards:
          MSG_DEBUG(self, "Calculate all task boards...")
          self.updateAllBoards()




    return StatusCode.SUCCESS


  def finalize(self):

    self.db().finalize()
    self.schedule().finalize()
    self.cpuSlots().finalize()
    self.gpuSlots().finalize()
    self.orchestrator().finalize()
    return StatusCode.SUCCESS


  def run(self):
    self.initialize()
    self.execute()
    self.finalize()
    return StatusCode.SUCCESS


  def treatRunningJobsBeforeStart(self):

    jobs = self.schedule().getAllRunningJobs()
    if len(jobs) > 0:
      for job in jobs:
        job.setStatus( Status.ASSIGNED )
        job.getTask().setStatus( Status.RUNNING )
      i=0
      while (self.cpuSlots().isAvailable()) and i<len(jobs):
        self.cpuSlots().push_back( jobs[i] )
        i+=1
      self.db().commit()
      self.cpuSlots().execute()


  #
  # This is for monitoring purpose. Should be used to dashboard view
  #
  def updateAllBoards( self ):

    for user in self.db().getAllUsers():
      # Get the number of tasks
      tasks = user.getAllTasks( self.__cluster)

      for task in tasks:
        board = self.db().session().query(Board).filter( Board.taskName==task.taskName ).first()
        board.jobs = len(task.getAllJobs())

        board.registered    = len(self.db().session().query(Job).filter( and_( Job.status==Status.REGISTERED, Job.taskId==task.id )).all())
        board.assigned      = len(self.db().session().query(Job).filter( and_( Job.status==Status.ASSIGNED  , Job.taskId==task.id )).all())
        board.testing       = len(self.db().session().query(Job).filter( and_( Job.status==Status.TESTING   , Job.taskId==task.id )).all())
        board.running       = len(self.db().session().query(Job).filter( and_( Job.status==Status.RUNNING   , Job.taskId==task.id )).all())
        board.done          = len(self.db().session().query(Job).filter( and_( Job.status==Status.DONE      , Job.taskId==task.id )).all())
        board.failed        = len(self.db().session().query(Job).filter( and_( Job.status==Status.FAILED    , Job.taskId==task.id )).all())
        board.kill          = len(self.db().session().query(Job).filter( and_( Job.status==Status.KILL      , Job.taskId==task.id )).all())
        board.killed        = len(self.db().session().query(Job).filter( and_( Job.status==Status.KILLED    , Job.taskId==task.id )).all())
        board.status        = task.status
        self.db().commit()


  def checkNodesHealthy(self):

    nodes = self.orchestrator().getNodeStatus()

    for node in nodes:

      MSG_INFO(self, "Checking %s healthy...", node['name'])
      # Get the node database
      machine = self.db().getMachine(self.__cluster, self.__queue_name, node['name'])
      machineIsRunning = (machine.CPUJobs + machine.GPUJobs) > 0
      machineIsUnderPressure = (node["MemoryPressure"] or node["DiskPressure"])

      # Node is probably down (not ready and is running)
      if ((not node['Ready']) and (machineIsRunning)):
        MSG_WARNING( self, "The node %s is not healthy.", node['name']                   )
        MSG_WARNING( self, "    Ready               : %s", str(node['Ready'])               )
        MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
        MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )
        MSG_WARNING( self, "Shutting it down..."                   )
        # Disable the node
        machine.CPUJobs = 0
        machine.GPUJobs = 0
        self.db().commit()
        # Send the email to the admin
        for user in self.db().getAllUsers():
          if user.isAdmin():
            try:
              subject = ("[LPS Cluster] FATAL - %s unready")%(machine.getName())
              message = ("Node with name {} in unhealthy. Further information below:<br><br>* Ready={}<br>* MemoryPressure={}<br>DiskPressure={}<br><br> This node will be disabled until it's fixed".format(
                machine.getName(),
                str(node['Ready']),
                str(node['MemoryPressure']),
                str(node['DiskPressure']),
              ))
              self.__postman.sendNotification(user.getUserName(), subject, message)
            except Exception as e:
              MSG_ERROR(self, e)
              MSG_ERROR(self, "Couldn't send warning mail.")

      # Should restart adding load to the node (it's ready but not running)
      elif (not(machineIsRunning) and node['Ready']):
        MSG_WARNING( self, "The node %s has recovered health.", node['name']                   )
        MSG_WARNING( self, "    Ready               : %s", str(node['Ready'])               )
        MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
        MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )
        MSG_WARNING( self, "Restablishing workloads..."                   )
        # Starting workload
        machine.CPUJobs = int(machine.maxCPUJobs / 2)
        machine.GPUJobs = machine.maxGPUJobs
        self.db().commit()
        # Send the email to the admin
        for user in self.db().getAllUsers():
          if user.isAdmin():
            try:
              subject = ("[LPS Cluster] INFO - %s restablished")%(machine.getName())
              message = ("Node with name {} has recovered health. Further information below:<br><br>* Ready={}<br>* MemoryPressure={}<br>DiskPressure={}<br><br> Setting CPUJobs to {} and GPUJobs to {}".format(
                machine.getName(),
                str(node['Ready']),
                str(node['MemoryPressure']),
                str(node['DiskPressure']),
                machine.CPUJobs,
                machine.GPUJobs
              ))
              self.__postman.sendNotification(user.getUserName(), subject, message)
            except Exception as e:
              MSG_ERROR(self, e)
              MSG_ERROR(self, "Couldn't send warning mail.")

      # Node is up and running but suffering with pressure
      elif (node['Ready'] and (machineIsUnderPressure) and (machineIsRunning)):
        MSG_WARNING( self, "The node %s is under pressure.", node['name']                   )
        MSG_WARNING( self, "    Ready               : %s", str(node['Ready'])               )
        MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
        MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )
        MSG_WARNING( self, "Reducing load..."                   )
        # Reduce load on node
        machine.CPUJobs = int(machine.CPUJobs / 2)
        machine.GPUJobs = int(machine.GPUJobs / 2)
        self.db().commit()

      # Node is up and running without any health issues
      elif (node['Ready'] and (not(machineIsUnderPressure)) and (machineIsRunning)):
        MSG_WARNING( self, "The node %s is under pressure.", node['name']                   )
        MSG_WARNING( self, "    Ready               : %s", str(node['Ready'])               )
        MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
        MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )
        MSG_WARNING( self, "Increasing load..."                   )
        # Increase load on node
        if (2 * machine.CPUJobs) > machine.maxCPUJobs:
          new_cpu_jobs_val = machine.maxCPUJobs
        else:
          new_cpu_jobs_val = 2 * machine.CPUJobs
        machine.CPUJobs = new_cpu_jobs_val
        machine.GPUJobs = machine.maxGPUJobs
        self.db().commit()

      # Node is not running nor ready, do nothing
      else:
        MSG_WARNING( self, "The node %s is not running nor ready.", node['name']                   )
        MSG_WARNING( self, "    Ready               : %s", str(node['Ready'])               )
        MSG_WARNING( self, "    MemoryPressure      : %s", str(node['MemoryPressure'])      )
        MSG_WARNING( self, "    DiskPressure        : %s", str(node['DiskPressure'])        )














