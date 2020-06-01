# Importing API
from orchestra.db import OrchestraDB
from orchestra import MaestroAPI
from orchestra.enumerations import Cluster
from partitura import lps

db  = OrchestraDB( lps.postgres_url, lps.cluster_volume , cluster=Cluster.LPS )

# Run!
api = MaestroAPI( db, lps.maestro_api_port )
api.run()
