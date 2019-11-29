


from orchestra.subprocess.Orchestrator import Orchestrator

sub = Orchestrator( )

sub.create( 'a', 'b', 'c','python3 test.py -c myfile.npz -o myoutput.npz' ,'node' )


while True:
  print(sub.status('a','b',0))







