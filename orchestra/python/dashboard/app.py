#!venv/bin/python
import os
import requests
from flask import Flask, url_for, redirect, render_template, request, abort, Response, session
from flask_sqlalchemy import SQLAlchemy
from flask_security import Security, SQLAlchemyUserDatastore, \
  UserMixin, RoleMixin, login_required, current_user
from flask_security.utils import encrypt_password
import flask_admin
from flask_admin.contrib import sqla
from flask_admin import helpers as admin_helpers
from flask_admin import BaseView, expose
from flask_cors import CORS
from sqlalchemy_utils import database_exists,create_database
from sqlalchemy import Column, Float, DateTime, Integer, String
from sqlalchemy.sql.expression import and_
from sqlalchemy import func
from flask_bootstrap import Bootstrap
from forms import ExtendedRegisterForm
import datetime
from flask import jsonify
import json
from config import light_db_endpoint, SESSION_TIMEOUT
from orchestra.db.models import *
from orchestra.db.models.Worker import db
from orchestra.kubernetes import Orchestrator
from flask_mail import Mail
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import docker
import re

from orchestra.constants import *

__all__ = [
  'app',
  'initial_exists'
]

# Create Flask application
app = Flask(__name__)
bootstrap = Bootstrap(app)
CORS(app)
app.config.from_pyfile('config.py')
db.init_app(app)

docker_client = docker.from_env()


#########################################################################
from partitura import lps

_orchestra  = Orchestrator( "/home/rancher/.cluster/orchestra/external/partitura/data/job_template.yaml",
                              "/home/rancher/.cluster/orchestra/external/partitura/data/lps_cluster.yaml" )

_engine = create_engine( lps.postgres_url )

_session = sessionmaker(bind=_engine)

#########################################################################
#
# Add session timeout
#
@app.before_request
def before_request():
    session.permanent = True
    app.permanent_session_lifetime = SESSION_TIMEOUT

#########################################################################
#
# Add X-Frame-Options
#
@app.after_request
def apply_caching(response):
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    return response

#########################################################################
#
# Utils
#
# ANSI to HTML stuff
COLOR_DICT = {
  '0' : [(255, 255, 255), (128, 128, 128)], # Reset
  '30': [(0, 0, 0),             (0, 0, 0)], # Black
  '31': [(255, 0, 0),         (128, 0, 0)], # Red
  '32': [(0, 255, 0),         (0, 128, 0)], # Green
  '33': [(255, 255, 0),     (128, 128, 0)], # Yellow
  '34': [(0, 0, 255),         (0, 0, 128)], # Blue
  '35': [(255, 0, 255),     (128, 0, 128)], # Magenta
  '36': [(0, 255, 255),     (0, 128, 128)], # Cyan
  '37': [(255, 255, 255), (128, 128, 128)], # White
}
COLOR_REGEX = re.compile(r'\[(?P<arg_1>\d+)(;(?P<arg_2>\d+)(;(?P<arg_3>\d+))?)?m')
BOLD_TEMPLATE = '<span style="color: rgb{}; font-weight: bolder">'
LIGHT_TEMPLATE = '<span style="color: rgb{}">'
def ansi_to_html(text):
  text = text.replace('[m', '</span>')
  def single_sub(match):
    argsdict = match.groupdict()
    if argsdict['arg_3'] is None:
      if argsdict['arg_2'] is None:
        color, bold = argsdict['arg_1'], 0
      else:
        color, bold = argsdict['arg_1'], int(argsdict['arg_2'])
    else:
      color, bold = argsdict['arg_2'], int(argsdict['arg_3'])
    if bold:
      return BOLD_TEMPLATE.format(COLOR_DICT[color][1])
    return LIGHT_TEMPLATE.format(COLOR_DICT[color][0])
  return COLOR_REGEX.sub(single_sub, text)

# Method that checks for modules health
def checkHealth ():
  health_dict = {
    'api' : False,
    'web' : False,
    'main': False,
  }
  for container in docker_client.containers.list():
    cname = container.attrs['Name']
    if 'orchestra-docker' in cname:
      if 'api' in cname:
        health_dict['api'] = True
      elif 'main' in cname:
        health_dict['main'] = True
      elif 'web' in cname:
        health_dict['web'] = True
  return health_dict

# Method that gets the log stream for a single module
def getLogStream (moduleName):
  for container in docker_client.containers.list():
    cname = container.attrs['Name']
    if 'orchestra-docker' in cname:
      if moduleName in cname:
        return container.logs(stream=True, stdout=True, stderr=True, tail=20)
  return False

# Method that gets data from the database
def _getDbData ():

  data = {
    'queue' : [],
    'history' : []
  }

  for task in _session().query(Board).order_by(Board.priority.desc()).all():
    if (task.status == 'done') or (task.status == 'failed') or (task.status == 'to_be_removed') or (task.status == 'to_be_removed_soon') or (task.status == 'removed'):
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

# Method that gets data from orchestrator
def _getUsage ():

  data = {
    'cpu' : _orchestra.getCPUConsume(),
    'mem' : _orchestra.getMemoryConsume()
  }
  return data

#########################################################################
#
# Stuff for Flask security
#
# Define models
user_datastore = SQLAlchemyUserDatastore(db, Worker, Role)
security = Security(app, user_datastore, register_form=ExtendedRegisterForm)

# Create customized model view class
class AdminAccessModelView(sqla.ModelView):

  def is_accessible(self):
    if not current_user.is_active or not current_user.is_authenticated:
      return False

    if current_user.has_role('superuser'):
      return True

    return False

  def _handle_view(self, name, **kwargs):
    """
    Override builtin _handle_view in order to redirect users when a view is not accessible.
    """
    if not self.is_accessible():
      if current_user.is_authenticated:
        # permission denied
        abort(403)
      else:
        # login
        return redirect(url_for('security.login', next=request.url))

# Create customized model view class
class AdminAccessView(BaseView):

  def is_accessible(self):
    if not current_user.is_active or not current_user.is_authenticated:
      return False

    if current_user.has_role('superuser'):
      return True

    return False

  def _handle_view(self, name, **kwargs):
    """
    Override builtin _handle_view in order to redirect users when a view is not accessible.
    """
    if not self.is_accessible():
      if current_user.is_authenticated:
        # permission denied
        abort(403)
      else:
        # login
        return redirect(url_for('security.login', next=request.url))

# Create customized model view class
class UserAccessView(BaseView):

  def is_accessible(self):
    if not current_user.is_active or not current_user.is_authenticated:
      return False

    if current_user.has_role('superuser') or current_user.has_role('user'):
      return True

    return False

  def _handle_view(self, name, **kwargs):
    """
    Override builtin _handle_view in order to redirect users when a view is not accessible.
    """
    if not self.is_accessible():
      if current_user.is_authenticated:
        # permission denied
        abort(403)
      else:
        # login
        return redirect(url_for('security.login', next=request.url))

class UserView(AdminAccessModelView):
  column_editable_list = ['email', 'username', 'maxPriority', 'active']
  column_searchable_list = column_editable_list
  column_exclude_list = ['password']
  # form_excluded_columns = column_exclude_list
  column_details_exclude_list = column_exclude_list
  column_filters = column_editable_list

  # can_edit = True
  edit_modal = True
  create_modal = True
  can_export = True
  can_view_details = True
  details_modal = True

class NodeView(AdminAccessModelView):
  column_editable_list = ['maxJobs']
  column_searchable_list = ['name', 'maxJobs', 'ip', 'cluster', 'queueName']
  column_exclude_list = ['jobs']
  # form_excluded_columns = column_exclude_list
  column_details_exclude_list = column_exclude_list
  column_filters = column_searchable_list

  # can_edit = True
  edit_modal = True
  create_modal = True
  can_export = True
  can_view_details = True
  details_modal = True

class QeA_Page (UserAccessView):
  @expose('/', methods=['GET'])
  def index(self):
    return self.render('admin/qea.html')

class Grafana_Page (AdminAccessView):
  @expose('/', methods=['GET'])
  def index(self):
    return self.render('admin/grafana.html')

class Rancher_Page (AdminAccessView):
  @expose('/', methods=['GET'])
  def index(self):
    return self.render('admin/rancher.html')

class Logs_Page (AdminAccessView):
  @expose('/', methods=['GET'])
  def index(self):
    return self.render('admin/logs.html')

#########################################################################
#
# Flask views
#
# Index
@app.route('/')
def index():
  return redirect(url_for('admin.index'))

# Get queue data
@app.route('/queue')
def get_queue():
  if current_user.is_authenticated:
    import json
    def get_data():
      while True:
        json_data = json.dumps(_getDbData()) # Data goes here
        yield "data: {}\n\n".format(json_data)
        time.sleep(1)
    return Response(get_data(), mimetype='text/event-stream')
  else:
    abort(404)

# Get usage data
@app.route('/usage')
def get_usage():
  if current_user.is_authenticated:
    import json
    def get_data():
      while True:
        json_data = json.dumps(_getUsage()) # Data goes here
        yield "data: {}\n\n".format(json_data)
        time.sleep(1)
    return Response(get_data(), mimetype='text/event-stream')
  else:
    abort(404)

# Get modules health
@app.route('/health')
def get_health():
  if current_user.is_authenticated:
    import json
    def get_data():
      while True:
        json_data = json.dumps(checkHealth()) # Data goes here
        yield "data: {}\n\n".format(json_data)
        time.sleep(1)
    return Response(get_data(), mimetype='text/event-stream')
  else:
    abort(404)

# Get modules health
@app.route('/logs/<name>')
def get_logs(name):
  if current_user.is_authenticated:
    msg_dict = {}
    msg_dict['message'] = ''
    import json
    def get_data():
      try:
        while True:
          for log in getLogStream(name):
            msg_ansi = log.decode()
            try:
              msg_html = ansi_to_html(msg_ansi)
              msg_dict['message'] = msg_html
            except:
              msg_dict['message'] = msg_ansi
            json_data = json.dumps(msg_dict)
            yield "data: {}\n\n".format(json_data)
          time.sleep(1)
      except:
        abort(404)
    return Response(get_data(), mimetype='text/event-stream')
  else:
    abort(404)

#########################################################################
#
# Create admin
admin = flask_admin.Admin(
  app,
  'LPS Cluster',
  base_template='my_master.html',
  template_mode='bootstrap3',
)

# Add model views
# admin.add_view(DashboardPage(name="Dashboard", endpoint='dashboard', menu_icon_type='fa', menu_icon_value='fa-dashboard',))
# admin.add_view(BalancoPage(name="Balanço", endpoint='balanco', menu_icon_type='fa', menu_icon_value='fa-balance-scale',))
# admin.add_view(AuthenticatedView(MainTableData, db.session, menu_icon_type='fa', menu_icon_value='fa-table', name="Tabela de dados"))
admin.add_view(AdminAccessModelView(Role, db.session, menu_icon_type='fa', menu_icon_value='fa-tags', name="Níveis de acesso"))
admin.add_view(UserView(Worker, db.session, menu_icon_type='fa', menu_icon_value='fa-users', name="Usuários"))
admin.add_view(NodeView(Node, db.session, menu_icon_type='fa', menu_icon_value='fa-desktop', name="Nodes"))
admin.add_view(Rancher_Page(name="Rancher", endpoint='rancher', menu_icon_type='fa', menu_icon_value='fa-cog'))
admin.add_view(Grafana_Page(name="Grafana", endpoint='grafana', menu_icon_type='fa', menu_icon_value='fa-line-chart'))
admin.add_view(Logs_Page(name="Logs", endpoint='logs', menu_icon_type='fa', menu_icon_value='fa-file-text'))
admin.add_view(QeA_Page(name="FAQ", endpoint='faq', menu_icon_type='fa', menu_icon_value='fa-question'))

# Views not in the menu
# admin.add_view(DetailsPage(name="Details", endpoint='details'))

# define a context processor for merging flask-admin's template context into the
# flask-security views.
@security.context_processor
def security_context_processor():
  return dict(
    admin_base_template=admin.base_template,
    admin_view=admin.index_view,
    h=admin_helpers,
    get_url=url_for
  )

def initial_exists ():
  if False:#db.session.query(Worker).filter('worker.username' == 'gabriel-milan').first():
    return True
  else:
    admin_role = Role(name='superuser')
    user_role = Role(name='user')
    db.session.add(admin_role)
    db.session.add(user_role)
    db.session.commit()
    gabriel = user_datastore.create_user(
        username='gabriel-milan',
        email='gabriel.milan@lps.ufrj.br',
        password=encrypt_password('Gazolaa1'),
        roles=[user_role, admin_role]
    )
    db.session.commit()


if __name__ == '__main__':

  # Start app
  app.run(host="0.0.0.0", port=8080, debug=True)
