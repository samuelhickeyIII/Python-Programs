import numpy as np
from numpy.typing import DTypeLike

class Perceptron(object):
    def __init__(self, **kwargs):
        """
        Parameters:
        - weights:      numpy.array
        - bias:         numpy.float
        - activation:   function

        """
        
        self.weights = kwargs.get("weights", None)
        self.bias = kwargs.get("bias", 0.0)
        self.activation = kwargs.get("activation", None)
        self.output = None

    def generate(self, feature_vec:np.dtype('float64'), weights=None):
        # Parameter validation
        try:
            feature_vec = feature_vec.astype(np.float64)
        except ValueError:
            "Cannot covert string to float"
    
        if weights and weights.dtype == np.dtype('float64') \
        and weights.shape == feature_vec.shape:
            self.weights = weights
        else:
            self.weights = np.random.rand(feature_vec.shape[0])

        # Multiply the vectors
        result = self.bias \
            + np.sum(np.multiply(feature_vec, self.weights))

        if self.activation == "input_layer":
            self.output = result
        if self.activation == "sigmoid":
            self.output = 1.0 / (1.0 + np.exp(-result))
        return self.output

    def __str__(self) -> str:
        return f"weights={self.weights} \n" + \
            f"bias={self.bias} \n" + \
            f"activation=\"{self.activation}\"\n" + \
            f"output={self.output}\n\n"

def main():
    mlr = Perceptron(
        weights=np.random.rand(1,5),
        bias=np.random.rand(1)[0],
        activation="sigmoid"
    )
    
    mlr.generate(feature_vec=np.random.rand(1,5))
    print(mlr)

if __name__ == "__main__":
    main()