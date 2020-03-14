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
from sqlalchemy.sql.expression import and_
from sqlalchemy import func
from flask_bootstrap import Bootstrap
from forms import ExtendedRegisterForm
import datetime
from flask import jsonify
import json
from config import light_db_endpoint
from orchestra.db.models import *
from orchestra.db.models.Worker import db
from flask_mail import Mail

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

class UserView(AdminAccessModelView):
  column_editable_list = ['email', 'username', 'maxPriority']
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
admin.add_view(AdminAccessModelView(Role, db.session, menu_icon_type='fa', menu_icon_value='fa-tags', name="Níveis de acesso"))
admin.add_view(UserView(Worker, db.session, menu_icon_type='fa', menu_icon_value='fa-users', name="Usuários"))

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
