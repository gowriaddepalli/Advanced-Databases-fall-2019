from enums import *

"""
Authors : Sree Gowri Addepalli (sga297)
"""

"""
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

"""


# This is a class for handling other operations of a transaction.

class OperationHandler:

    def __init__(self, operationType, transactionNum, timestamp, variableNumber, variableValue=0):
        self.operationType = operationType
        self.transactionNum = transactionNum
        self.timestamp = timestamp
        self.variableNumber = variableNumber
        self.variableValue = variableValue

    # get the type of operation.
    def getOperationType(self):
        return self.operationType

    # get the Transaction Number.
    def getTransactionNum(self):
        return self.transactionNum

    # get the timestamp of the operation.
    def getTimestamp(self):
        return self.timestamp

    # get the variable number regarding the timestamp.
    def getVariableNumber(self):
        return self.variableNumber

    # get the variable value of the variable.
    def getVariableValue(self):
        return self.variableValue

    # Returns whether two objects of this class are equal or not.
    # Overrides the method.
    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __repr__(self):
        return str(self.__dict__)
