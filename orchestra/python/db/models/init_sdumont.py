
#from sqlalchemy.ext.declarative import declarative_base
#Base = declarative_base()
from orchestra.db.models import *

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from orchestra.enumerations import *
from orchestra.constants import DEFAULT_URL_SDUMONT
engine = create_engine(DEFAULT_URL_SDUMONT)

Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

users = ["jodafons","mverissimo"]


for user in users:
  obj = Worker( username = user, maxPriority = 1000 )
  session.add(obj)




obj = Node(name="sdumont_cpu", CPUJobs=10, maxCPUJobs=10, GPUJobs=0, maxGPUJobs=8, queueName = "cpu", cluster= Cluster.SDUMONT)
session.add(obj)

obj = Node(name="sdumont_cpu_small", CPUJobs=10, maxCPUJobs=10, GPUJobs=0, maxGPUJobs=8, queueName = "cpu_small", cluster= Cluster.SDUMONT)
session.add(obj)

obj = Node(name="sdumont_nvidia", CPUJobs=4, maxCPUJobs=4, GPUJobs=2, maxGPUJobs=2, queueName = "nvidia", cluster = Cluster.SDUMONT)
session.add(obj)

obj = Node(name="sdumont_gdl", CPUJobs=10, maxCPUJobs=10, GPUJobs=8, maxGPUJobs=8, queueName = "gdl", cluster = Cluster.SDUMONT)
session.add(obj)


session.commit()
session.close()







