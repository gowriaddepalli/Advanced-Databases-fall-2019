"""
Authors : Sree Gowri Addepalli (sga297)
"""
import time
"""
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

"""

# This is a class for the lock table present at every site..

from enums import *


class LockTable:

    def __init__(self, type, transactionNumber, variableNumber):
        # Lock type
        self.type = type
        self.transactionNumber = transactionNumber
        self.variableNumber = variableNumber
        self.LastSetTime = time.monotonic_ns()

    # Returns the lock type.
    def getType(self):
        return self.type

    # Returns the Transaction Number of the lock.
    def getTransactionNumber(self):
        return self.transactionNumber

    # Returns the variable on which the lock is.
    def getVariableNumber(self):
        return self.variableNumber

    def getTime(self):
        return self.LastSetTime

    # Returns whether two objects of this class are equal or not.
    # Overrides the method.
    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False
    
    # String Representation.
    def __repr__(self):
        return str(self.__dict__)
