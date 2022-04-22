import os, json


output_path = 'jobs'

os.makedirs(output_path, exist_ok=True)

for sort in range(10):

        d = {   
                'sort'   : sort,
            }

        o = output_path + '/job.sort_%d.json'%(sort)
        with open(o, 'w') as f:
            json.dump(d, f)
