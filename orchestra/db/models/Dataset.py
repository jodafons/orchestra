__all__ = ["Dataset","File"]

from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from orchestra.db.models import Base

#
#   Jobs Table
#
class Dataset (Base):
  __tablename__ = 'dataset'

  # Local
  id = Column(Integer, primary_key = True)
  username = Column(String)
  dataset = Column(String)
  files = relationship("File", order_by="File.id", back_populates="dataset")


  def getDatasetName(self):
    return self.datasetName

  def getAbsPath(self):
    return self.abspath

  def getUsername(self):
    return self.username

  def getAllFiles(self):
    return self.files

  def addFile(self, file):
    self.files.append(file)




class File(Base):

  __tablename__ = 'file'

  # Local
  id = Column(Integer, primary_key = True)
  path = Column(String)

  # Foreign
  dataset = relationship("Dataset", back_populates='files')
  datasetId = Column(Integer, ForeignKey('dataset.id'))


  def getPath(self):
    return self.path

