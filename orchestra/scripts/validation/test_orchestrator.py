

from orchestra import Orchestrator

kube = Orchestrator('../../data/job_template.yaml','../../data/lps_cluster.yaml')



containerImage="jodafons/gpu-base"

execArgs = "python3 -c 'import tensorflow' && . /setup_envs.sh && python3 /code/saphyra/Analysis/Tutorial/job_tuning.py -c /volume/jodafons/test/user.jodafons.cnn_short_test/job_config_m0000_s0000_i0000.10-Oct-2019-17.37.23.pic.gz -o /volume/jodafons/test/output/test01 -d /volume/jodafons/test/user.jodafons.data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_V97_et0_eta0.npz/data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_V97_et0_eta0.npz -r /volume/jodafons/test/user.jodafons.data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_v97_et0_eta0.ref.pic.gz/data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_v97_et0_eta0.ref.pic.gz"


#execArgs = "python3 --version ./setup_envs.sh"
node=6



name = kube.create('test', containerImage,execArgs,node='node04',gpu=True)
import time





