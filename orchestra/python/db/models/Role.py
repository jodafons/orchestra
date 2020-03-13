__all__=['Role']

from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import relationship
from orchestra.db.models import Base
from flask_login import UserMixin
from flask_security import RoleMixin
from flask_sqlalchemy import Model

class Role(Model, Base, RoleMixin):

  __tablename__ = 'role'

  id = Column(Integer(), primary_key=True)
  name = Column(String(80), unique=True)
  description = Column(String(255))

  # Foreign
  users = relationship("Worker", back_populates="roles")

  def __str__(self):
    return self.name
