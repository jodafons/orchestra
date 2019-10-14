
__all__ = ["CPUSlots"]


from lpsgrid.engine.slots import Slots

class CPUSlots( Slots ):
  def __init__( self,name,  maxLength ):
    Slots.__init__(self,name,maxLength)


