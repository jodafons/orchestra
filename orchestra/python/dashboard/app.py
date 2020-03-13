#!venv/bin/python
import os
import requests
from flask import Flask, url_for, redirect, render_template, request, abort
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
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import and_
from sqlalchemy import func
from flask_bootstrap import Bootstrap
from forms import *
import datetime
from flask import jsonify
import json
from config import light_db_endpoint
from orchestra.db import OrchestraDB
from orchestra.db.models import *

__all__ = [
  'app',
  'db',
]

# Create Flask application
app = Flask(__name__)
bootstrap = Bootstrap(app)
CORS(app)
app.config.from_pyfile('config.py')
db = SQLAlchemy(app)

#########################################################################
#
# Stuff for Flask security
#
# Define models
user_datastore = SQLAlchemyUserDatastore(db, Worker, roles_workers)
security = Security(app, user_datastore, register_form=ExtendedRegisterForm)


# Create customized model view class
class AuthenticatedView(sqla.ModelView):

  def is_accessible(self):
    if not current_user.is_active or not current_user.is_authenticated:
      return False
    return True

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


  # can_edit = True
  edit_modal = True
  create_modal = True
  can_export = True
  can_view_details = True
  details_modal = True

class BalancoPage(BaseView):

  @expose('/', methods=['GET', 'POST'])
  def index(self):
    errors = []
    form = BalancoForm()
    if request.method == 'POST':
      if (form.cep.data and not form.trafo.data) or (not form.cep.data and form.trafo.data):
        if form.startDate.data and form.endDate.data:
          if form.endDate.data > form.startDate.data:
            return self.render('admin/balanco_resultado.html', data = computeBalance (cep = form.cep.data, trafo = form.trafo.data, start = form.startDate.data, end = form.endDate.data + datetime.timedelta(days=1)))
          else:
            errors.append("A data final deve ser maior que a data inicial")
        else:
          errors.append("Favor preencher datas inicial e final")
      else:
        errors.append("Favor preencher apenas um dos seguintes: identificador do transformador ou CEP")
    return self.render('admin/balanco_consulta.html', form = form, errors = errors)

  def is_accessible(self):
    """
    Overrides is_accessible method in order to allow users AND admins to access it
    """
    if not current_user.is_active or not current_user.is_authenticated:
      return False

    if current_user.has_role('superuser') or current_user.has_role('user'):
      return True

    return False

class DashboardPage (BaseView):

  @expose('/', methods=['GET'])
  def index(self):
    errors = []
    return self.render('admin/dashboard.html', list = getQualiList(), errors = errors, count = countEntries(), latest = getLatestEntryDelta())

  def is_accessible(self):
    """
    Overrides is_accessible method in order to allow users AND admins to access it
    """
    if not current_user.is_active or not current_user.is_authenticated:
      return False

    if current_user.has_role('superuser') or current_user.has_role('user'):
      return True

    return False

class DetailsPage (BaseView):

  @expose('/', methods=['GET'])
  def default (self):
    return self.render('admin/details.html', equip = "Default")

  @expose('/<equipment>', methods=['GET'])
  def index(self, equipment):
    errors = [],
    consumptionA, consumptionB, consumptionC, accumulatedA, accumulatedB, accumulatedC, results, timestamps = getDetailsData(equipment)
    return self.render(
      'admin/details.html',
      equip = equipment,
      consumption = readableWatts(consumptionA + consumptionB + consumptionC),
      accumulatedA = accumulatedA,
      accumulatedB = accumulatedB,
      accumulatedC = accumulatedC,
      registers = results,
      latest = getLatestEntryDelta(equipment),
      timestamps = timestamps
    )

  def is_accessible(self):
    """
    Overrides is_accessible method in order to allow users AND admins to access it
    """
    if not current_user.is_active or not current_user.is_authenticated:
      return False

    if current_user.has_role('superuser') or current_user.has_role('user'):
      return True

    return False

  def is_visible (self):
    return False

#########################################################################
# Flask views
@app.route('/')
def index():
  return redirect(url_for('admin.index'))

# Create admin
admin = flask_admin.Admin(
  app,
  'Qualimeter',
  base_template='my_master.html',
  template_mode='bootstrap3',
)

# Add model views
# admin.add_view(DashboardPage(name="Dashboard", endpoint='dashboard', menu_icon_type='fa', menu_icon_value='fa-dashboard',))
# admin.add_view(BalancoPage(name="Balanço", endpoint='balanco', menu_icon_type='fa', menu_icon_value='fa-balance-scale',))
# admin.add_view(AuthenticatedView(MainTableData, db.session, menu_icon_type='fa', menu_icon_value='fa-table', name="Tabela de dados"))

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

if __name__ == '__main__':

  # Start app
  app.run(host="0.0.0.0", port=8080, debug=True)
