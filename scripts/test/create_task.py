


import os
basepath = os.getcwd()

path = basepath



# execute my job!
exec_cmd = "python {PATH}/run.py -j %IN".format(PATH=path)

# if complete, remove some dirs...
#exec_cmd+= "rm -rf rxwgan && rm -rf rxcore && "
#exec_cmd+= "(rm -rf wandb || true)" # some protections

command = """maestro.py task create \
  -v {PATH} \
  -i {PATH}/jobs \
  -t task.test.18 \
  --exec "{EXEC}" \
  """

cmd = command.format(PATH=path,EXEC=exec_cmd)
print(cmd)
os.system(cmd)
