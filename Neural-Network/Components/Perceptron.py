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
        elif self.weights is None:
            self.weights = np.random.rand(feature_vec.shape[0])

        result = self.bias \
            + np.sum(np.multiply(feature_vec, self.weights))

        if self.activation == "input_layer":
            self.output = result
        if self.activation == "sigmoid":
            self.output = 1.0 / (1.0 + np.exp(-result))
        if self.activation == "relu":
            self.output = max(0, result)
        return self.output


    def __str__(self) -> str:
        return f"\nweights={self.weights} \n" + \
            f"bias={self.bias} \n" + \
            f"activation=\"{self.activation}\"\n" + \
            f"output={self.output}\n"


def main():
    input_ = np.array([1, 1], dtype=np.float64)
    print(f"\ninput = {input_}\n")

    and_ = Perceptron(
        weights=np.array([1, 1], dtype=np.float64),
        bias=np.float64(-1),
        activation="sigmoid"
    )
    aand = np.round(and_.generate(feature_vec=input_))
    or_ = Perceptron(
        weights=np.array([.25, .25], dtype=np.float64),
        bias=np.float64(0),
        activation="sigmoid"
    )
    oor = np.round(or_.generate(feature_vec=input_))
    not_ = Perceptron(
        weights = np.array([-1, -1], dtype=np.float64),
        bias=np.float64(1),
        activation="sigmoid"
    )
    nnot = np.round(not_.generate(feature_vec=input_))

    nand = Perceptron(
        weights = np.array([-2, 0], dtype=np.float64),
        bias=np.float64(1),
        activation="sigmoid"
    )
    nnand = np.round(nand.generate(
        feature_vec=np.array([aand, oor], dtype=np.float64)
    ))

    nor_ = Perceptron(
        weights = np.array([-2, -2], dtype=np.float64),
        bias=np.float64(1),
        activation="sigmoid"
    )
    nnor = np.round(nor_.generate(
        feature_vec=np.array([aand, oor], dtype=np.float64)
    ))

    xor_ = Perceptron(
        weights = np.array([-1, 1], dtype=np.float64),
        bias=np.float64(0),
        activation="sigmoid"
    )
    xxor = np.round(xor_.generate(
        feature_vec=np.array([aand, oor], dtype=np.float64)
    ))

    # xnor here
    
    print(f"and_ node: {and_}  and={aand}\n")
    print(f"or_  node: {or_}  or={oor}\n")
    print(f"not_ node: {not_}  not={nnot}\n")
    print(f"nand node: {nand}  not={nnand}\n")
    print(f"nor_ node: {nor_}  not={nnor}\n")
    print(f"xor_ node: {xor_}  xor={xxor}")


if __name__ == "__main__":
    main()