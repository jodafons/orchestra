

from orchestra.slots import *
from orchestra import Orchestrator


containerImage="jodafons/gpu-base"
task = 'user.jodafons.task.data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_V97_et2_eta0.v8'
data = 'data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_V97_et2_eta0.npz'
config = 'user.jodafons.job_config.ringer.v8.mlp2to15.10sorts.10inits/job_config.ID_0000.ml0000.mu0004_s0000_il0000.iu0001.17-Oct-2019-16.14.20.pic.gz'
ref = 'data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_v97_et2_eta0.ref.pic.gz'
output = 'output'





obj = Orchestrator('../../data/job_template.yaml','../../data/lps_cluster.yaml')





execArgs = "ls /volume/{TASK} && python3 -c 'import tensorflow' && . /setup_envs.sh && python3 /code/saphyra/Analysis/RingerNote_2018/tunings/v8/job_tuning.py -c /volume/{TASK}/{CONFIG} -o /volume/{TASK}/{OUTPUT} -d /volume/{TASK}/{DATA} -r /volume/{TASK}/{REF} --user jodafons --task dev".format( TASK=task,CONFIG=config,DATA=data,REF=ref,OUTPUT=output )



#node = GPUNode( "verdun", 1 )
node = CPUNode( "node07" )

name = obj.create('user.wsfreund.test2', 'jodafons',containerImage,execArgs, node)

print(name)





