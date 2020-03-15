import enum

"""
Authors : Sree Lakshmi Addepalli (sla410) 
"""

"""
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

"""


# Enum for telling the health signal of a particular site.
class SiteHealthSignal(enum.Enum):
    UP = 1
    DOWN = 2
    RECOVER = 3


# Enum for telling the type of transaction operations.
class TransactionOperationType(enum.Enum):
    Read = 1
    Write = 2


# Enum for telling the type of Lock.
class LockType(enum.Enum):
    Read = 1
    Write = 2


# Enum for telling the type of Transaction.
class TransactionType(enum.Enum):
    ReadOnly = 1
    ReadWrite = 2


# Enum for telling the health signal of any particular transaction.
class TransactionHealthSignal(enum.Enum):
    Active = 1
    Committed = 2
    Failed = 3
    Aborted = 4
    PartiallyCommitted = 5  # waiting
    Deadlocked = 6
