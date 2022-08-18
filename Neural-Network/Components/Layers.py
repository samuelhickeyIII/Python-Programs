from inspect import Attribute
import numpy as np
from Perceptron import Perceptron

class InputLayer(object):
    """
    Creates a layer of Perceptrons with a bias=0, weights=np.ones(*attributes.shape) and an activation function of the form lambda x: x
    """
    def __init__(self, units=0, **kwargs):
        if units <= 0:
            ValueError("Units specifies the number of nodes in a layer. Must be a positive integer")
        self.units = units
        self.perceptrons = []
        for _ in range(units):
            self.perceptrons.append(
                Perceptron(
                    weights=np.ones((1,1)),
                    bias=0,
                    activation="input_layer"
                )
            )

    def __str__(self) -> str:
        layer = ""
        for perceptron in self.perceptrons:
            layer += str(perceptron)
        return layer


def main():
    input_layer = InputLayer(units=10)
    print(input_layer)



if __name__ == "__main__":
    main()