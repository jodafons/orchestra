from orchestra.db.models import *

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.enumerations import *

#engine = create_engine('postgres://postgres:postgres@localhost:5432/postgres')
engine = create_engine('postgres://postgres:postgres@localhost:5432/lps_dev')

Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

users = ["jodafons", "gabriel.milan"]


obj = Worker( username = "jodafons", maxPriority = 1000, email="jodafons@lps.ufrj.br", active=True  )
session.add(obj)




nvidia_machines    = ['node04','node05','cessy','marselha','verdun']
cpu_small_machines = ['node02','node03','node04','node05','node06','cessy','marselha','verdun']
cpu_large_machines = ['node07','node08']


for name in nvidia_machines:
  obj = Node(queueName='nvidia', name=name, jobs=0, maxJobs=0, cluster=Cluster.LPS)
  session.add(obj)


for name in cpu_small_machines:
  obj = Node(queueName='cpu_small', name=name, jobs=0, maxJobs=10, cluster=Cluster.LPS)
  session.add(obj)

for name in cpu_large_machines:
  obj = Node(queueName='cpu_large', name=name, jobs=0, maxJobs=1, cluster=Cluster.LPS)
  session.add(obj)




session.commit()
session.close()

