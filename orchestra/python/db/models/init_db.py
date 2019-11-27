
#from sqlalchemy.ext.declarative import declarative_base
#Base = declarative_base()
from orchestra.db.models import *

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


#engine = create_engine('postgres://postgres:postgres@localhost:5432/postgres')
engine = create_engine('postgres://postgres:postgres@postgres.cahhufxxnnnr.us-east-2.rds.amazonaws.com:5432/postgres')

Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

users = ["joao.pinto", "micael.araujo","werner.freund"]


for user in users:
  obj = Worker( username = user, maxPriority = 1000 )
  session.add(obj)



machines = ['sdummont']

for name in machines:
  obj = Node(name=name, CPUJobs=4, maxCPUJobs=10, GPUJobs=0, maxGPUJobs=2)
  session.add(obj)


session.commit()
session.close()







