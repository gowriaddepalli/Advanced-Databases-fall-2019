import time
from collections import OrderedDict
import copy

"""
Authors : Sree Lakshmi Addepalli (sla410)

"""

"""
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

"""


# This is the class for every data variable.

class Variable:

    """
    Args: i : variable index
          v : variable value

    """
    # The constructor
    def __init__(self, i, v):
        self.index = i
        self.value = v
        self.c = -1
        self.c = self.c+1
        self.LastSetTime = time.monotonic_ns()
        self.timeVersionsValues = OrderedDict()
        self.timeVersionsValues[self.LastSetTime] = self.value

    # Get the variable number.
    # Returns an integer index.
    def getVariableIndex(self):
        return self.index

    # Get the variable value.
    # Returns an integer variable value.
    def getVariableValue(self):
        return self.value

    # Set the variable value and hence change the last committed time.
    def setVariableValue(self, v):
        self.value = v
        self.c = self.c + 1
        self.LastSetTime = time.monotonic_ns()+self.c
        self.timeVersionsValues[self.LastSetTime] = self.value

    # Get the latest committed value below the given input time.
    # returns an integer variable value before the given time.
    def getLatestCommittedValue(self,inputTime):
        #print("The latest Time: ")
        #print(inputTime)
        minTime =0
        #inputTime = inputTime-1
        for t,v in self.timeVersionsValues.items():
            if inputTime > t >= minTime:
                minTime = t
        #print(minTime)
        #print(self.timeVersionsValues)
        return self.timeVersionsValues[minTime]

    # Creates a deep copy of the variable.
    # Returns an object of class variable.
    def replicateVariable(self):
        return copy.deepcopy(self)

    # Returns whether two objects of this class are equal or not.
    # Overrides the method.
    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __repr__(self):
        return str(self.__dict__)




