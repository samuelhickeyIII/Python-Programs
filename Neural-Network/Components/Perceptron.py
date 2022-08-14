import numpy as np
from scipy import rand


class Perceptron(object):
    def __init__(self, **kwargs):
        """
        Parameters:
        - features:     numpy.array
        - weights:      numpy.array of shape self.features
        - bias:         numpy.float
        - activation:   function
        - input_layer:  boolean
        """
        
        self.features = kwargs.get("features", np.empty((1,1)))
        self.weights = kwargs.get("weights", np.random.rand(*self.features.shape))
        self.bias = kwargs.get("bias", 0)
        self.activation = kwargs.get("activation", None)
        self.input_layer = kwargs.get("input_layer", False)
        self.output = None

    def value(self):
        v = self.bias \
            + np.sum(np.multiply(self.features, self.weights))

        if self.activation == "input":
            self.output = v
        if self.activation == "sigmoid":
            self.output = 1.0 / (1.0 + np.exp(-v))
        return self.output

def main():
    mlr = Perceptron(
        features=np.random.rand(1,5),
        weights=np.random.rand(1,5),
        bias=0,
        activation="sigmoid"
    )

    print(mlr.value())

if __name__ == "__main__":
    main()