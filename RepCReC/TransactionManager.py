from Graph import DeadlockDetector
from Site import Site
from transaction import Transaction
import time
from enums import *
from OperationHandler import OperationHandler
from LockTable import LockTable
import copy

'''
Authors : Sree Gowri Addepalli (sga297)
'''

'''
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

'''


# This is the class for a transaction manager for our distributed database system.

class TransactionManager:

    def __init__(self):
        # Map of variables with the list of site they are in.
        self.variableSiteMap = {}
        self.it = -1

        for i in range(1, 21):
            self.variableSiteMap[i] = []
        for i in range(1, 21):
            if i % 2 == 0:
                for p in range(1, 11):
                    self.variableSiteMap[i].append(p)
            else:
                self.variableSiteMap[i].append(i % 10 + 1)
        print(" Set up the sites and variables at each site \n")
        #print("[[[[[[[[[")
        #print(self.variableSiteMap)
        # Map of site numbers with site objects.
        self.siteMap = {}

        for i in range(1, 11):
            self.siteMap[i] = Site(i)

        # Map of transactionIds to transactions.
        self.transactionMap = {}
        # Map of Transaction numbers to set of site numbers.
        self.transactionSiteMap = {}
        # List of pending operations waiting to be completed.
        # Copy on Write
        self.pendOperations = []
        self.graph = DeadlockDetector()
    
    # Transaction manager fails this site and aborts the corresponding transactions.
    def siteFail(self, siteNumber):
        print("\n Site Failing: ")
        #print(self.siteMap.get(int(siteNumber)))
        siteNumber = int(siteNumber)
        if self.siteMap.get(siteNumber) is not None:
            print(" The site: " + str(siteNumber) + " got failed. Transactions running in this site would be aborted. \n")
            transOnSiteId = self.siteMap[siteNumber].failSite()
            if len(transOnSiteId) != 0:
                for tno in transOnSiteId:
                    print(" Abort Transaction: T" + tno +" \n")
                    self.abort(tno)
    
    # Transaction Manager recovers this site by running pending operation.
    def siteRecover(self, siteNumber):
        siteNumber = int(siteNumber)
        if self.siteMap.get(siteNumber) is not None:
            print(" The Site: " + str(siteNumber) + " is recovered.")
            self.siteMap[siteNumber].recoverSite()
            self.runPendOp()
    
    # function to get variable values at all sites. 
    def dumpAll(self):
        for i in range(1, 11):
            self.dumpSite(i)
    
    # Print the value of the respective variable at every site. 
    def dump(self, variableNumber):
        varSite = self.variableSiteMap.get(variableNumber)
        print("Dump of variable:" + variableNumber)
        for i in varSite:
            print("Site Number:" + i)
            self.siteMap[i].dumpNum(variableNumber)
            print("\n")
    
    # print the value of variable at every site.
    def dumpSite(self, siteNumber):
        if self.siteMap.get(siteNumber) is not None:
            print("Dump of Site:" + str(siteNumber))
            self.siteMap.get(siteNumber).dump()
            print("\n")
    
    # add particular transaction with it corresponds site number.
    def addTransactionSiteMap(self, transactionNumber, variableNumber):
        #print("$$$$$$$$$$")
        #print(transactionNumber)
        #print(variableNumber)
        variableNumber = int(variableNumber)
        if variableNumber % 2 == 0:
            if self.transactionSiteMap.get(transactionNumber) is None:
                self.transactionSiteMap[transactionNumber] = set()
            for i in range(1, 11):
                self.transactionSiteMap[transactionNumber].add(i)
        else:
            if self.transactionSiteMap.get(transactionNumber) is None:
                self.transactionSiteMap[transactionNumber] = set()
            for i in range(1, 11):
                self.transactionSiteMap[transactionNumber].add(variableNumber % 10 + 1)
        #print(self.transactionSiteMap)

    # starts the corresponding transaction based on it being ReadOnly or ReadWrite.
    def startTransaction(self, type, transactionNumber):
        self.it = self.it + 1
        print("Start Transaction: " + transactionNumber + "\n")
        if type == "ReadOnly":# add self.it here
            self.transactionMap[transactionNumber] = Transaction(transactionNumber, time.monotonic_ns()+ self.it,
                                                                 TransactionType.ReadOnly)
            self.graph.addTransactionVertex(transactionNumber)
        if type == "ReadWrite": #add self.it here
            self.transactionMap[transactionNumber] = Transaction(transactionNumber, time.monotonic_ns()+self.it,
                                                                 TransactionType.ReadWrite)
            self.graph.addTransactionVertex(transactionNumber)
    
    # abort the corresponding transaction number.
    def abort(self, transaction):
        print("\n Trying to abort the following transaction T: " + str(transaction))
        self.graph.removeTransactionVertex(transaction)
        #print("Post abort transaction state: \n ")
        #print(self.graph)
        del self.transactionMap[transaction]

        #print(self.siteMap.get(2).getLockTable(1))
        #print(self.transactionSiteMap[transaction])

        for s in self.transactionSiteMap[transaction]:
            self.siteMap[s].removeTransaction(transaction)
        #print("Lock checking 1")
        #print(self.siteMap.get(2).getLockTable(1))

        del self.transactionSiteMap[transaction]

        #print("Lock checking 2")
        #print(self.siteMap.get(2).getLockTable(1))

        removeL = []
        for i in range(0, len(self.pendOperations)):
            if self.pendOperations[i].getTransactionNum() == transaction:
                removeL.append(i)
        #print("----")
        #print(removeL)
        #print(self.pendOperations)

        # changed here to get new copy while rem.
        """
            for i in removeL:
            gg.append(self.pendOperations[i])
            del self.pendOperations[i]
        """
        gg = []
        for i in removeL:
            gg.append(self.pendOperations[i])
        pp = [x for x in self.pendOperations if x not in gg]
        self.pendOperations = pp
        #print(":::::")
        #print(removeL)
        #print(self.pendOperations)
        #print("Lock checking 3")
        #print(self.siteMap.get(2).getLockTable(1))
        self.runPendOp()
    
     # execute the corresponding operations in the pending operation list.
    def runPendOp(self):
        print("\n Running Pending Operations. ")
        #print(self.pendOperations)
        #print(self.transactionMap)
        # made changes here
        copyPendOp = copy.deepcopy(self.pendOperations)
        for op in copyPendOp:
            #print("Current Op: \n" + str(op))
            t = self.transactionMap.get(op.getTransactionNum())
            #print("Transaction Num:" + str(t))
            self.pendOperations.remove(op)
            #print("Pending ops pos removing op: \n")
            #print(self.pendOperations)
            if op.getOperationType() == TransactionOperationType.Write:
                if self.writeOp(op.getTransactionNum(), op.getVariableNumber(), op.getVariableValue()):
                    print("\n Operation with Transaction T: " + str(op.getTransactionNum()) + " Got Lock" +
                          str(op.getOperationType()) + " X " + str(op.getVariableNumber()) + " " + str(
                        op.getVariableValue()) + "\n")
                    if len(t.getOperations()) != 0:
                        continue
                    print(" Waiting operation starting for committing.")
                    self.endTransaction(op.getTransactionNum())
                    print(" Waiting operation got committed. ")

            if op.getOperationType() == TransactionOperationType.Read:
                if self.readOp(op.getTransactionNum(), op.getVariableNumber()):
                    print("\n Operation with Transaction T: " + str(op.getTransactionNum()) + " Got Lock " +
                          str(op.getOperationType()) + " X " + str(op.getVariableNumber())+ " \n")
                    if len(t.getOperations()) != 0:
                        continue
                    print(" Waiting operation starting for committing. ")
                    self.endTransaction(op.getTransactionNum())
                    print(" Waiting operation got committed.")
    
    # detect deadlock in the graph.
    def deadlockDetection(self):
        deadlockTids = self.graph.detectCycleUtil()
        #print("Locks Latest: \n")
        '''
        if self.graph.isSelfLock:
            print("self lock deadlock \n")
            deadlockTids = self.graph.getNeighboursOfATrans(deadlockTids[0])
            print(deadlockTids)
            print(self.transactionMap)
            ll = []
            for i in range(0, len(deadlockTids)):
                ll.append(self.transactionMap.get(str(deadlockTids[i])))
            print(ll)
            df = sorted(ll, key=lambda x: x.getTimeStamp(), reverse=False)
            print(df)
            df1 = [x.getTransactionNumber() for x in df]
            print(df1)
            deadlockTids= df1[1:]
            self.graph.isSelfLock = False
        #print(self.siteMap.get(2).getLockTable(1))
        print(";;;;;;;;;")
        print(deadlockTids)
        '''
        while len(deadlockTids) != 0:
            #print("nnnnnnn")
            #print(self.transactionMap)
            tr = self.transactionMap[str(deadlockTids[0])]
            #print(tr)
            for i in range(1, len(deadlockTids)):
                t1 = self.transactionMap.get(str(deadlockTids[i]))
                #print(t1)
                if tr.getTimeStamp() < t1.getTimeStamp():
                    tr = t1
            deadlockTrans = []
            for i in deadlockTids:
                deadlockTrans.append("T" + str(i))

            print("\n Deadlock detection: " + "".join(deadlockTrans))
            print("\n Aborting youngest transaction T: " + tr.getTransactionNumber())

            #print("Locks LatestNew: \n")
            #print(self.siteMap.get(2).getLockTable(1))
            self.abort(tr.getTransactionNumber())
            #print("Locks LatestNewUp: \n")
            #print(self.siteMap.get(2).getLockTable(1))
            #if not self.graph.isSelfLock:
            deadlockTids = self.graph.detectCycleUtil()
            #print("^^^^OO")
            #print(deadlockTids)
    
    # whether the transaction has committed or not.
    def isCommitOp(self, op):
        print("\n Committing operation with transaction number T:" + str(op.getTransactionNum()) + "and variable number "+ str(op.getVariableNumber()))
        print("\n")
        Trans = self.transactionMap.get(op.getTransactionNum())
        if op.getVariableNumber() % 2 == 0:
            flag = True
            for i in range(1, 11):
                s = self.siteMap[i]
                if s.getSiteSignalHealth() != SiteHealthSignal.DOWN and not s.isCommit(Trans, op):
                    flag = False
            return flag
        else:
            s = self.siteMap.get(op.getVariableNumber() % 10 + 1)
            #print(s)
            #print(s.getLockTable(op.getVariableNumber()))
            if s.isCommit(Trans, op):
                return True
            else:
                return False
    
    # whether the transaction has committed or not.
    def endTransaction(self, transactionNumber):
        print("\n end Transaction T:" + str(transactionNumber))
        print("\n")
        #print(self.siteMap.get(2).getLockTable(1))
        if self.transactionMap.get(transactionNumber) is None:
            self.graph.removeTransactionVertex(transactionNumber)
            return

        flag = True

        indexNum = -1

        for i in range(0, len(self.pendOperations)):
            if self.pendOperations[i].getTransactionNum() == transactionNumber:
                indexNum = i
        #print(indexNum)
        #print(self.pendOperations)
        if indexNum < 0:
            tr = self.transactionMap.get(transactionNumber)
            opList = tr.getOperations()
            #print("ccccccccc")
            #for o in opList:
                #print(o)
            tr.removeOperations()

            for op in opList:
                varNum = op.getVariableNumber()
                #print("YYYYY")
                #print(varNum)
                #print(op.getOperationType())
                #print(tr.getTransactionType())
                if op.getOperationType() == TransactionOperationType.Read and tr.getTransactionType() == TransactionType.ReadWrite:
                    if varNum % 2 == 1:
                        print("Even variable Lock got dropped.")
                        self.siteMap.get(varNum % 10 + 1).dropLock(varNum, transactionNumber)
                    else:
                        print("Odd variable Lock got dropped.")
                        #print(self.siteMap.get(3))
                        for j in range(1, 11):
                            self.siteMap.get(j).dropLock(varNum, transactionNumber)

                if op.getOperationType() == TransactionOperationType.Write and tr.getTransactionType() == TransactionType.ReadWrite:
                    p = self.isCommitOp(op)
                    #print("+++===")
                    #print(p)

        else:
            opL = self.transactionMap[transactionNumber].getOperations()
            #print("operation list: \t")
            #print(opL)
            tp = self.transactionMap.get(transactionNumber)
            #print("Transaction List: \n")
            #print(tp)
            self.transactionMap[transactionNumber].removeOperations()

            for o in opL:
                vnum = o.getVariableNumber()
                #print("!!")
                #print(vnum)
                #print(o.getOperationType())
                #print(tp.getTransactionType())

                if o.getOperationType() == TransactionOperationType.Read and tp.getTransactionType() == TransactionType.ReadWrite:
                # testcase 24 changes.
                    if vnum % 2 == 1:
                        print("See the read value while transaction is going.")
                        self.isCommitOp(o)
                        self.siteMap.get(vnum % 10 + 1).dropLock(vnum, transactionNumber)
                    else:
                        print("See the read value while transaction is going.")
                        self.isCommitOp(o)
                        for j in range(1, 11):
                            self.siteMap.get(j).dropLock(vnum, transactionNumber)

                if o.getOperationType() == TransactionOperationType.Write and tp.getTransactionType() == TransactionType.ReadWrite:
                    #print("!!!!!!!!!")
                    self.isCommitOp(o)

            ot = self.pendOperations[indexNum]
            #print("ops pen: lat \n")
            #print(indexNum)
            #for i in self.pendOperations:
                #print(i)
            if not self.isCommitOp(ot):
                print("Not committed ")
                print("Transaction Number: \n" + ot.getTransactionNum())
                flag = False
            else:
                #print(":::::")
                #for i in self.pendOperations:
                    #print(i)
                del self.pendOperations[indexNum]

        if flag:
            f = self.transactionMap.get(transactionNumber).getTransactionType() == TransactionType.ReadWrite
            if f:
                self.graph.removeTransactionVertex(transactionNumber)
            if indexNum == -1:
                print("\n T" + transactionNumber + " Normal Commit \n")
            else:
                print("\n T" + transactionNumber + " unblocked and run \n")
            del self.transactionMap[transactionNumber]
            #print("GGG")
            #print(transactionNumber)
            #print(self.transactionSiteMap)
            if(self.transactionSiteMap.get(transactionNumber)) is not None:
                for d in self.transactionSiteMap.get(transactionNumber):
                    #print("HHHH")
                    #print(d)
                    #print(transactionNumber)
                    self.siteMap.get(d).removeTransaction(transactionNumber)
                del self.transactionSiteMap[transactionNumber]
            #print(f)
            #print(self.pendOperations)
            if f:
                self.runPendOp()
    
    # the command to read a particular variable of a transaction.
    def readOp(self, transactionNum, variableNum):
        print("\n Performing a read operation for Transaction "+ str(transactionNum) + " for variable number " + str(variableNum)+ "\n")
        variableNum = int(variableNum)
        rt = True
        if self.transactionMap.get(transactionNum) is None:
            print("\n Aborted Transaction while read operation. \n")
            return rt

        trans = self.transactionMap.get(transactionNum)

        if trans.getTransactionType() == TransactionType.ReadOnly:
            opTmstmp = trans.getTimeStamp()
            if variableNum % 2 == 0:
                ListSiteId = self.variableSiteMap.get(variableNum)
                for siteNum in ListSiteId:
                    s = self.siteMap.get(siteNum)
                    Op = OperationHandler(TransactionOperationType.Read, transactionNum, opTmstmp, variableNum)
                    s.addOperationList(transactionNum, Op)
                    if s.isCommit(trans, Op):
                        break
                opl = OperationHandler(TransactionOperationType.Read, transactionNum, opTmstmp, variableNum)
                self.transactionMap.get(transactionNum).addOperation(opl)
                print("\n")
            else:
                s1 = self.siteMap.get(variableNum % 10 + 1)
                Op = OperationHandler(TransactionOperationType.Read, transactionNum, opTmstmp, variableNum)
                s1.addOperationList(transactionNum, Op)
                Opo = OperationHandler(TransactionOperationType.Read, transactionNum, opTmstmp, variableNum)
                self.transactionMap.get(transactionNum).addOperation(Opo)
                s1.isCommit(trans, Opo)
                print("\n")
        else:
            opTmstmp = trans.getTimeStamp()
            if variableNum % 2 == 0:
                ListSiteId = self.variableSiteMap.get(variableNum)
                flag = False
                opl = OperationHandler(TransactionOperationType.Read, transactionNum, opTmstmp, variableNum)
                for siteNum in ListSiteId:
                    s = self.siteMap.get(siteNum)
                    Op = OperationHandler(TransactionOperationType.Read, transactionNum, opTmstmp, variableNum)
                    if s.addLock(variableNum, LockTable(LockType.Read, transactionNum, variableNum)):
                        s.addOperationList(transactionNum, Op)
                        flag = True
                        s.isCommit(trans, Op)
                if not flag:
                    self.pendOperations.append(opl)
                    rt = False
                    s = self.siteMap.get(1)
                    for sid in self.siteMap.keys():
                        if self.siteMap.get(sid).getSiteSignalHealth() == SiteHealthSignal.UP:
                            s = self.siteMap.get(sid)
                            break

                    lt = s.getLockTable(variableNum)
                    for l in lt:
                        if l.getType() == LockType.Write and l.getTransactionNumber() != transactionNum:
                            self.graph.addTransactionEdge(l.getTransactionNumber(), transactionNum)
                    self.deadlockDetection()
                else:
                    print("\n")
                    opr = OperationHandler(TransactionOperationType.Read, transactionNum, opTmstmp, variableNum)
                    self.transactionMap.get(transactionNum).addOperation(opr)

                self.transactionMap.get(transactionNum).addOperation(opl)
                print("\n")
            else:
                s1 = self.siteMap.get(variableNum % 10 + 1)
                Op = OperationHandler(TransactionOperationType.Read, transactionNum, opTmstmp, variableNum)
                if s1.addLock(variableNum, LockTable(LockType.Read,transactionNum, variableNum)):
                    #print(" Read Lock got added.")
                    s1.addOperationList(transactionNum, Op)
                    Opo = OperationHandler(TransactionOperationType.Read, transactionNum, opTmstmp, variableNum)
                    self.transactionMap.get(transactionNum).addOperation(Opo)
                    s1.isCommit(trans, Opo)
                    #print("Operation got committed.")
                    print("\n")
                else:
                    #print("inside pend op--.")
                    self.pendOperations.append(Op)
                    #print(self.pendOperations)
                    rt = False
                    ltn = s1.getLockTable(variableNum)
                    for ls in ltn:
                        if ls.getType() == LockType.Write and ls.getTransactionNumber() != transactionNum:
                            self.graph.addTransactionEdge(ls.getTransactionNumber(), transactionNum)
                    self.deadlockDetection()

        return rt

    # The command to write a particular variable of a transaction with a particular value.
    def writeOp(self, transactionNum, variableNum, value):
        print("\n Performing a write operation for Transaction " + str(transactionNum) + " for variable number " + str(variableNum) + "\n")
        rt = True
        opTmstmp = time.monotonic_ns()
        variableNum = int(variableNum)
        #print("GHGHGH")
        #print(transactionNum)
        #print(variableNum)
        #print(value)
        if variableNum % 2 == 0:
            sids = self.variableSiteMap.get(variableNum)
            #print(sids)
            flag = False
            op1 = OperationHandler(TransactionOperationType.Write, transactionNum, opTmstmp, variableNum, value)
            for si in sids:
                #print("OOOOOO")
                #print(si)
                s = self.siteMap.get(si)
                op = OperationHandler(TransactionOperationType.Write, transactionNum, opTmstmp, variableNum, value)
                flad = False
                if s.getSiteSignalHealth() == SiteHealthSignal.UP or s.getSiteSignalHealth() == SiteHealthSignal.RECOVER:
                    # code added.
                    sLock = s.getLockTable(variableNum)
                    #print(sLock)
                    for p in self.pendOperations:
                        #print("bhbb")
                        #print(p)
                        if p.getVariableNumber() == op.getVariableNumber() and p.getOperationType() == TransactionOperationType.Write\
                                and p.getTransactionNum() != op.getTransactionNum() and len(sLock) == 1 and sLock[0].getType() == LockType.Read\
                               and sLock[0].getTime() <= p.getTimestamp() <= op.getTimestamp() :
                            self.graph.addTransactionEdge(sLock[0].getTransactionNumber(), transactionNum)
                            print("Checking for self locked operations. \n")
                            flad = True
                            break
                    if not flad and not s.addLock(variableNum, LockTable(LockType.Write, transactionNum, variableNum)):
                        #print("Lock not added")
                        break
                    elif not flad:
                        s.addOperationList(transactionNum, op)
                        flag = True
            #print("Fllag Val ->" + str(flag))
            if flag:
                for i in sids:
                    oo = OperationHandler(TransactionOperationType.Write, transactionNum, opTmstmp, variableNum, value)
                    s = self.siteMap.get(i)
                    if s.getSiteSignalHealth() == SiteHealthSignal.RECOVER:
                        print("write operation in recover state")
                        s.addOperationList(transactionNum, oo)
                oy = OperationHandler(TransactionOperationType.Write, transactionNum, opTmstmp, variableNum, value)
                self.transactionMap.get(transactionNum).addOperation(oy)
                #print("check transaction Map:")
                #print(self.transactionMap)
            else:
                self.pendOperations.append(op1)
                rt = False
                s = self.siteMap.get(1)
                #print("pendOps ->" + str(self.pendOperations))
                for si in self.siteMap.keys():
                    if self.siteMap.get(si).getSiteSignalHealth() == SiteHealthSignal.UP:
                        s = self.siteMap.get(si)
                        break
                lsLo = s.getLockTable(variableNum)
                #print("Lock Table \n")
                #print(lsLo)
                for l in lsLo:
                    if l.getTransactionNumber() != transactionNum:
                        self.graph.addTransactionEdge(l.getTransactionNumber(), transactionNum)
                self.deadlockDetection()
        else:
            #print("KKKKKKKKKKKKk")
            #print(transactionNum)
            op = OperationHandler(TransactionOperationType.Write, transactionNum, opTmstmp, variableNum, value)
            s1 = self.siteMap.get(variableNum % 10 + 1)
            #print(s1.getLockTable(1))
            if s1.addLock(variableNum, LockTable(LockType.Write, transactionNum, variableNum)):
                s1.addOperationList(transactionNum, op)
                #print(s1.getLockTable(variableNum))
                op1 = OperationHandler(TransactionOperationType.Write, transactionNum, opTmstmp, variableNum, value)
                #print(self.transactionMap)
                self.transactionMap.get(transactionNum).addOperation(op1)
                #print("^^^^^^^^^^^^")
                #print(self.transactionMap)
            else:
                #print("&&&&")
                #print(transactionNum)
                self.pendOperations.append(op)
                rt = False
                lt = s1.getLockTable(variableNum)
                #print(self.graph)
                for l in lt:
                    if l.getTransactionNumber() != transactionNum:
                        self.graph.addTransactionEdge(l.getTransactionNumber(), transactionNum)
                self.deadlockDetection()
        return rt
