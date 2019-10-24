
__all__ = ["DashboardAPI"]

# Getting database models
from orchestra.db.models import *
# Getting orchestrator
from orchestra.kubernetes import Orchestrator
# Getting Akuanduba stuff
from Gaugi.messenger.macros import *
from Gaugi import StatusCode


# Getting some akuandubas threads services
from Akuanduba.core import AkuandubaService as Service
from Akuanduba.core.Watchdog import Watchdog
from Akuanduba.core.constants import Second
from Akuanduba.core import StatusThread


# My imports
import time
from flask import Flask, Response
from flask_cors import CORS
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

#
# Macros
#
DEFAULT_UPDATE_TIME = 1 * Second

class DashboardAPI (Service):

  #
  # Init
  #
  def __init__(self, name, update_time = DEFAULT_UPDATE_TIME):

    # Mandatory
    AkuandubaService.__init__(self, name)

    # My stuff
    self._name = name
    self._updateTime = update_time
    self._engine = create_engine('postgres://postgres:postgres@localhost:5432/postgres')
    self._session = sessionmaker(bind=self._engine)()
    self._orchestra = Orchestrator( "../data/job_template.yaml",  "../data/lps_cluster.yaml" )

  #
  # Initialize
  #
  def initialize(self):

    # Mandatory
    super().initialize()
    if self.start().isFailure():
      MSG_FATAL (self, "Unable to initialize service {}".format(self.name()))
      return StatusCode.FAILURE

    # Lock initialization
    self.init_lock()

    # Constructing Flask app
    self.__app = Flask(__name__)
    self.__cors = CORS(self.__app, supports_credentials = True)

    # Making route for getting queue data
    @self.__app.route('/queue')
    def get_queue():
        import json
        def get_data():
            while True:
                json_data = json.dumps(self._getDbData()) # Data goes here
                yield "data: {}\n\n".format(json_data)
                time.sleep(self._updateTime)
        return Response(get_data(), mimetype='text/event-stream')

    # Making route for getting usage data
    @self.__app.route('/usage')
    def get_usage():
        import json
        def get_data():
            while True:
                json_data = json.dumps(self._getUsage()) # Data goes here
                yield "data: {}\n\n".format(json_data)
                time.sleep(self._updateTime)
        return Response(get_data(), mimetype='text/event-stream')

    return StatusCode.SUCCESS


  #
  # Execute
  #
  def execute(self, context):
    return StatusCode.SUCCESS

  #
  # Finalize
  #
  def finalize(self):

    # Mandatory
    super().finalize()

    return StatusCode.SUCCESS

  #
  # Run
  #
  def run (self):

    # Imports
    import traceback

    # This is the main loop for the thread, do everything inside this
    while self.statusThread() == StatusThread.RUNNING:
      Watchdog.feed(self.name(), 'run')

      try:
        if (self.isInitialized()):
          self.__app.run(host = "0.0.0.0", port = 5000, debug = False)
      except Exception:
        print (traceback.format_exc())

    return StatusCode.FAILURE

  #
  # Method that gets data from the database
  #
  def _getDbData (self):

    data = {
      'queue' : [],
      'history' : []
    }

    for task in self._session.query(TaskBoard).all():
      if (task.status == 'done') or (task.status == 'failed'):
        data['history'].append({
          'name' : task.taskName,
          'status' : task.status,
        })
      else:
        data['queue'].append({
          'name' : task.taskName,
          'status' : task.status,
          'n_jobs' : task.jobs,
          'n_regs' : task.registered,
          'n_asgn' : task.assigned,
          'n_test' : task.testing,
          'n_runn' : task.running,
          'n_done' : task.done,
          'n_fail' : task.failed,
        })
    return data

  #
  # Method that gets data from orchestrator
  #
  def _getUsage (self):

    data = {
      'cpu' : self._orchestra.getCPUConsume(),
      'mem' : self._orchestra.getMemoryConsume()
    }
    return data
