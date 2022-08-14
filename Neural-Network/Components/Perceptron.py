from cmath import exp


class Perceptron(object):
    def __init__(self, **kwargs):
        """
        Parameters:
        - features: List(float)
        - weights: List(float) 
        - bias: float
        - activation: function
        """
        
        self.features = kwargs.get("features", [])
        self.weights = kwargs.get("weights", [1 for _ in self.features])
        self.bias = kwargs.get("bias", 0)
        self.activation = kwargs.get("activation", None)
        self.output = None

    def activate(self) -> None:
        dot_prod = self.bias \
                    + sum([f * w for f,w in zip(self.features, self.weights)])

        if self.activation == "sigmoid":
            self.output = 1.0 / (1.0 + exp(-dot_prod))