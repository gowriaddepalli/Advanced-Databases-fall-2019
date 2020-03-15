import queue

from enums import *
from variable import Variable
import time

"""
Authors : Sree Lakshmi Addepalli (sla410)

"""

"""
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

"""


# This is a class for each particular site.

class Site:

    def __init__(self, siteIndex):
        self.siteIndex = siteIndex
        self.SiteHealthSignal = SiteHealthSignal.UP
        # the variable list is variable id and variable.
        self.variableList = dict()
        # the lock list is variable id and locks on that.
        self.LockList = dict()
        # the transactionID and the the list of operations to be committed.
        self.transactionList = dict()
        for i in range(1, 21):
            if i % 2 == 0 or int(siteIndex) == int(1 + i % 10):
                self.variableList[i] = Variable(i, 10 * i)
                self.LockList[i] = []

        #print("kkkghg\n")
        #print(self.variableList)
        #print("SiteIndex:" + str(self.siteIndex) +" \n")
        #for i in range(1,21):
            #if self.variableList.get(i) is not None:
                #print(str(self.variableList)+" \n")

    # return site status.
    def getSiteSignalHealth(self):
        #print("Health ->")
        #print(self.SiteHealthSignal)
        return self.SiteHealthSignal

    # get the list of locks on a certain variable.
    def getLockTable(self, variableIndex):
        #for s in self.LockList[variableIndex]:
            #print("Lock->: ")
            #print(s)
            #print("\n")
        return self.LockList[variableIndex]

     # recover the current site.
    def recoverSite(self):
        print("\n The site got recovered: \n")
        self.SiteHealthSignal = SiteHealthSignal.RECOVER

     # fail the current site.
    def failSite(self):
        # remove all sites for this site
        print("\n The site got failed.")
        for i in self.variableList:
            self.LockList[i] = []

        self.SiteHealthSignal = SiteHealthSignal.DOWN

        # removing all the transactions of the corresponding site
        listOfTransactions = self.transactionList.keys()

        self.transactionList = dict()

        return listOfTransactions

        # Get the value of every variable at every site.
    def dump(self):
        print("The site health is: -> " + str(self.SiteHealthSignal))
        print("site number ->" + str(self.siteIndex) + "\n_________________________________________\n")
        for i, v in self.variableList.items():
            print("Variable x" + str(i) + "->" + str(v.getVariableValue()))
        print("*************")

       # Get the value of particular given variable at every site.
    def dumpNum(self, index):
        print("The site health is: -> " + str(self.SiteHealthSignal))
        print("site number ->" + self.siteIndex + "\n")
        print("Variable x" + index + "->" + self.variableList[index].getVariableValue())

    '''
     This method takes in an transaction number and the Operation.
    '''

    def addOperationList(self, transactionNumber, op):
        #print("Adding to operation List: \n")
        if self.transactionList.get(transactionNumber) is None:
            self.transactionList[transactionNumber] = queue.Queue(maxsize=0)
        else:
            self.transactionList[transactionNumber].put(op)
        #print(self.transactionList.get(transactionNumber))

    """
        drop the lock on the given variable with a specific transaction.
     """

    def dropLock(self, variableNumber, transactionNumber):
        print("\n Dropping lock on transaction "+ str(transactionNumber) + "and variable number" + str(variableNumber)
              + " for site "+ str(self.siteIndex) +" \n")
        locks = self.LockList[variableNumber]
        n = len(locks)
        for i in range(0, n):
            if locks[i].getTransactionNumber == transactionNumber:
                locks.remove(i)
                break
        self.LockList[variableNumber] = locks

    '''
    add the lock on the given variable with a specific transaction.
    '''

    def addLock(self, variableNumber, lock):
        #print("%%%%%%%%%%5")
        #print(self.SiteHealthSignal)
        #print(lock.getType())
        print("\n Adding lock on transaction T:" + str(lock.getTransactionNumber()) + " and variable number " + str(
            variableNumber) + " for site "+ str(self.siteIndex) + " \n")
        if self.SiteHealthSignal == SiteHealthSignal.DOWN:
            return False
        elif (
                self.SiteHealthSignal == SiteHealthSignal.RECOVER) and lock.getType() == LockType.Read and variableNumber % 2 == 0:
            return False

        totalLocks = self.LockList[variableNumber]
        #print("<<<<")
        #print(totalLocks)

        if lock.getType() == LockType.Read:
            # list of locks before is zero.
            #print(" Trying to add Read Lock \n")
            if len(totalLocks) == 0:
                totalLocks.append(lock)
                self.LockList[variableNumber] = totalLocks
                return True
            else:
                # comment below lines for testcase 24  and lock transaction Number.
                for l in totalLocks:
                    if l.getType() == LockType.Write and l.getTransactionNumber() != lock.getTransactionNumber():
                        return False
                totalLocks.append(lock)
                self.LockList[variableNumber] = totalLocks
                return True

        if lock.getType() == LockType.Write:
            #print("@@@@")
            #print(totalLocks)
            #print(lock)
            #print(" Trying to add write Lock \n")
            if len(totalLocks) > 1:
                return False
            elif len(totalLocks) == 1:
                #print("adding lock")
                if totalLocks[0].getTransactionNumber() == lock.getTransactionNumber():
                    print("added same transaction lock")
                    totalLocks.append(lock)
                    self.LockList[variableNumber] = totalLocks
                    return True
                else:
                    return False
            else:
                totalLocks.append(lock)
                self.LockList[variableNumber] = totalLocks
                return True

    # Returns whether two objects of this class are equal or not.
    # Overrides the method.
    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    # commit a given operation of a transaction.
    def isCommit(self, transaction, op):

        print("\n Committing a given transaction " + str(transaction.getTransactionNumber()) + " for operation type " + str(op.getOperationType())+ " for variable "+ str(op.getVariableNumber()) + "\n")
        variableIndex = int(op.getVariableNumber())
        #print(variableIndex)
        #print("Reached start here")
        #print(self.LockList)

        #print("values of variables: \n")
        #print(self.variableList.get(variableIndex))
         
        # if a transaction is Read only and operation is read for a non replicable variable while the site is down.
        if ((transaction.getTransactionType() == TransactionType.ReadOnly) and (
                op.getOperationType() == TransactionOperationType.Read)
                and (variableIndex % 2 == 1) and (self.SiteHealthSignal == SiteHealthSignal.DOWN)):
            return False
        
        # if a transaction is Read only and operation is read for a non replicable variable while the site is in recover or up state.
        if (transaction.getTransactionType() == TransactionType.ReadOnly) and (
                op.getOperationType() == TransactionOperationType.Read) and (
                variableIndex % 2 == 1):
            print("Variable x: " + str(self.variableList.get(
                variableIndex).getVariableIndex()) + " Transaction T:" + str(
                transaction.getTransactionNumber()) + " Operation Read: " + str(
                self.variableList.get(variableIndex).getLatestCommittedValue(op.getTimestamp())) + " Site:"
                  + str(self.siteIndex) + "\n")
            return True
        
        # if a transaction is Read only and operation is read for a replicable variable while the site is in up state.
        if ((transaction.getTransactionType() == TransactionType.ReadOnly) and (
                op.getOperationType() == TransactionOperationType.Read)
                and (variableIndex % 2 == 0) and (self.SiteHealthSignal == SiteHealthSignal.UP)):
            print(
                "Variable x: " + str(self.variableList.get(variableIndex).getVariableIndex()) + " Transaction T:" + str(
                    transaction.getTransactionNumber()) + " Operation Read: " + str(
                    self.variableList.get(variableIndex).getLatestCommittedValue(op.getTimestamp()))
                + " Site:" + str(self.siteIndex) + "\n")
            return True
        
        # if a transaction is Read only and operation is read for a non replicable variable while the site is in down or recover state.
        if ((transaction.getTransactionType() == TransactionType.ReadOnly) and (
                op.getOperationType() == TransactionOperationType.Read)
                and (variableIndex % 2 == 0)):
            return False

        if transaction.getTransactionType() == TransactionType.ReadWrite:
            #print("Reached middle up here")
            locks = self.LockList[variableIndex]
            #print(self.LockList)
            #print(locks)
            
            # if a transaction is ReadWrite and operation is read for both non replicable and replicable variable.
            if op.getOperationType() == TransactionOperationType.Read:
                #print("Reached middle here")
                flag = False
                for l in locks:
                    if l.getType() == LockType.Write and (
                            l.getTransactionNumber() != transaction.getTransactionNumber()):
                        flag = True
                        break

                if flag:
                    return False

                if (variableIndex % 2 == 1) and (self.SiteHealthSignal == SiteHealthSignal.DOWN):
                    return False
                # changed here for testcase 24 -> getLatestCommittedValue and getVariableValue.
                if variableIndex % 2 == 1:
                    print("Variable x: " + str(self.variableList.get(
                        variableIndex).getVariableIndex()) + " Transaction T:" + str(
                        transaction.getTransactionNumber()) + " Operation Read: " + str(
                        self.variableList.get(variableIndex).getVariableValue()) + " Site: "
                          + str(self.siteIndex) + "\n")
                    return True

                if variableIndex % 2 == 0 and self.SiteHealthSignal == SiteHealthSignal.UP:
                    print("Variable x: " + str(self.variableList.get(
                        variableIndex).getVariableIndex()) + " Transaction T:" + str(
                        transaction.getTransactionNumber()) + " Operation Read: " + str(
                        self.variableList.get(variableIndex).getVariableValue()) + " Site:"
                          + str(self.siteIndex) + "\n")
                    return True

                if variableIndex % 2 == 0:
                    return False
            
            # if a transaction is ReadWrite and operation is write for both non replicable and replicable variable.
            if op.getOperationType() == TransactionOperationType.Write:
                #print("Reached middle write here")
                flag = True

                #print(locks)


                for l in locks:
                    #print(l)
                    if l.getType() == LockType.Write and (
                            l.getTransactionNumber() == transaction.getTransactionNumber()):
                        flag = False
                        break

                if flag:
                    return False

                if self.SiteHealthSignal == SiteHealthSignal.DOWN:
                    return False
                if self.SiteHealthSignal == SiteHealthSignal.RECOVER:
                    self.SiteHealthSignal = SiteHealthSignal.UP
                #print("Reached here")
                v = self.variableList[variableIndex]
                v.setVariableValue(op.getVariableValue())
                self.variableList[variableIndex] = v
                print("Variable x: " + str(
                    self.variableList.get(variableIndex).getVariableIndex()) + " Transaction T:" + str(
                    transaction.getTransactionNumber()) + " Operation Write: " + str(v.getVariableValue()) + " Site:"
                      + str(self.siteIndex) + "\n")
                self.dropLock(variableIndex, transaction.getTransactionNumber())
                return True
        return False

    ''' 
     Remove all the locks held on this site for a corresponding transaction and remove corresponding transaction.
    '''

    def removeTransaction(self, transactionNumber):
        #print("Lock checking remove Transaction: \n")
        #print(self.getLockTable(1))
        #print(" Remove all the locks held on this site for a corresponding transaction and remove corresponding transaction. \n")

        if self.transactionList.get(transactionNumber) is not None:
            del self.transactionList[transactionNumber]
            #print("::::::;")
            #print(self.LockList)
            for lockID in self.LockList.keys():
                l = []
                n = len(self.LockList.get(lockID))
                #print(n)
                #print(transactionNumber)
                for s in self.LockList.get(lockID):
                    if s.getTransactionNumber() == transactionNumber:
                        l.append(s)
                t = self.LockList.get(lockID)
                #print(t)
                for i in l:
                    #print(i)
                    t.remove(i)

                self.LockList[lockID] = t
            return True
        return False

    def __repr__(self):
        return str(self.__dict__)