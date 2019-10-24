# Akuanduba imports
from Akuanduba.core import Akuanduba, LoggingLevel, AkuandubaTrigger
from Akuanduba.services import StoreGateSvc
from Akuanduba.tools import DataLog
from Akuanduba import ServiceManager, ToolManager, DataframeManager
from Akuanduba.core.constants import Second
from Akuanduba.triggers import TimerCondition
from Akuanduba.core.Watchdog import Watchdog

# Importing DashboardAPI service
from orchestra import DashboardAPI 

# Creating Akuanduba
manager = Akuanduba ("Akuanduba", level=LoggingLevel.INFO)

# Creating services
svc = DashboardAPI("Dashboard API")

# Adding services
ServiceManager += svc

# Initializing 
manager.initialize()
manager.execute()
manager.finalize()
