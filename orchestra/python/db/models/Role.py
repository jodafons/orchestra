__all__=['Role']

from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import relationship
from orchestra.db.models import Base
from flask_login import UserMixin
from flask_security import RoleMixin
from orchestra.db.models.Worker import db

class Role(Base, db.Model, RoleMixin):

  __tablename__ = 'role'

  id = Column(Integer(), primary_key=True)
  name = Column(String(80), unique=True)
  description = Column(String(255))

  def __str__(self):
    return self.name
