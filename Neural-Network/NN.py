import numpy as np
from .Components.Layers import InputLayer

class ANN(object):
    def __init__(self, **kwargs):
        self.data = kwargs.get("data", None)
        self.layers = kwargs.get("layers", [])
        
