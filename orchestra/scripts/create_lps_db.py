from orchestra.db.models import *

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.enumerations import *

engine = create_engine('postgres://postgres:postgres@localhost:5432/postgres')

Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

machines = ['node02','node03','node04','node05','node06','node07','node08','cessy','marselha','verdun']

for name in machines:
  obj = Node(name=name, CPUJobs=1, maxCPUJobs=30, GPUJobs=0, maxGPUJobs=0, queueName='lps', cluster=Cluster.LPS)
  session.add(obj)


session.commit()
session.close()

