
from orchestra.utils import get_config
from orchestra.db import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

config = get_config()


engine = create_engine(config["postgres"])
Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)
session.commit()
session.close()


