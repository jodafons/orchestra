

from orchestra.slots import *
from orchestra import Orchestrator


containerImage="jodafons/gpu-base"
task = 'user.wsfreund.dev'
data = 'data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_V97_et0_eta0.npz'
config = 'job_config.ID_0000.ml0000.mu0001_s0000_il0000.iu0001.18-Oct-2019-01.36.14.pic.gz'
ref = 'data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_v97_et0_eta0.ref.pic.gz'
output = 'output'





obj = Orchestrator('../../data/job_template.yaml','../../data/lps_cluster.yaml')





execArgs = "python3 -c 'import tensorflow' && . /setup_envs.sh && python3 /code/saphyra/Analysis/RingerNote_2018/tunings/v10/job_tuning.py -c /volume/{TASK}/{CONFIG} -o /volume/{TASK}/{OUTPUT} -d /volume/{TASK}/{DATA} -r /volume/{TASK}/{REF} --user wsfreund --task dev".format( TASK=task,CONFIG=config,DATA=data,REF=ref,OUTPUT=output )




node = GPUNode( "marselha", 0 )

name = obj.create('user.wsfreund.test2', 'wsfreund',containerImage,execArgs, gpu_node=node)

print(name)





