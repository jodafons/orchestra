__all__ = ["Slot"]


#
# Slot class
#
class Slot:

  def __init__(self, node, device=-1):
    self.node = node
    self.device = device
    self.__enable = False
    self.__available = True


  def available( self ):
    return (self.__available and self.__enable)

  def lock( self ):
    self.__available = False

  def unlock( self ):
    self.__available = True

  def enable(self):
    self.__enable=True

  def disable(self):
    self.__enable=False

  def is_enable(self):
    return self.__enable