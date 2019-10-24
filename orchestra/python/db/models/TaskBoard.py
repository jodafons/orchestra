__all__=['TaskBoard']


from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship
from orchestra.db.models import Base


#
#   Tasks Table
#
class TaskBoard (Base):
  __tablename__ = 'taskboard'

  # Local
  id = Column(Integer, primary_key = True)
  username = Column(String)


  taskName      = Column(String, unique=True)
  taskId        = Column(Integer)
  status        = Column(String)


  jobs           = Column( Integer )
  registered     = Column( Integer )
  assigned       = Column( Integer )
  testing        = Column( Integer )
  running        = Column( Integer )
  done           = Column( Integer )
  failed         = Column( Integer )





  def __repr__ (self):
    return "<Orchestra (taskName='{}')>".format(self.taskName)

