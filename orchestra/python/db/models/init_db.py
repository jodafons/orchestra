
#from sqlalchemy.ext.declarative import declarative_base
#Base = declarative_base()
from orchestra.db.models import *

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
#engine = create_enginepostgres://ringer:6sJ09066sV1990;6@postgres-ringer-db.cahhufxxnnnr.us-east-2.rds.amazonaws.com/ringer')



#engine = create_engine('postgres://lps:DuQDYsBP@postgres-lps-cluster-db.cahhufxxnnnr.us-east-2.rds.amazonaws.com/postgres')
engine = create_engine('postgres://postgres:postgres@localhost:5432/postgres')

Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

users = ["jodafons", "mverissimo", "gabriel.milan","wsfreund","cadu.covas"]


for user in users:
  obj = Worker( username = user, maxPriority = 1000 )
  session.add(obj)



machines = ['node02','node03','node04','node05','node06','node07','node08','cessy','marselha','verdun']

for name in machines:
  obj = Node(name=name, CPUJobs=1, maxCPUJobs=30, GPUJobs=0, maxGPUJobs=0)
  session.add(obj)


session.commit()
session.close()







