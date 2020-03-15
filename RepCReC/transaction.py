from enums import *

"""
Authors : Sree Gowri Addepalli (sga297) 
"""

"""
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

"""

# This is a class for managing all transactions


class Transaction:

    def __init__(self, transactionNumber, timestamp, type):
        self.transactionNumber = transactionNumber
        self.type = type
        self.timestamp = timestamp
        self.operations = []

    # Return the transaction Number.
    def getTransactionNumber(self):
        return self.transactionNumber

    # Return the Timestamp.
    def getTimeStamp(self):
        return self.timestamp

    # Return the type of Transaction.
    def getTransactionType(self):
        return self.type

    # Return get the list of operations associated with this Transaction.
    def getOperations(self):
        return self.operations

    # Add operation to the list of Operations.
    def addOperation(self, op):
        self.operations.append(op)

    # Remove all the operations related to the set of Transactions.
    def removeOperations(self):
        self.operations = []

    # Returns whether two objects of this class are equal or not.
    # Overrides the method.
    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __repr__(self):
        return str(self.__dict__)
