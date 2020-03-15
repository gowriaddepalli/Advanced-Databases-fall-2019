from TransactionManager import TransactionManager

"""
Authors : Sree Lakshmi Addepalli (sla410)

"""

"""
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

"""


# This is a script to parse input file and perform events according to the action.
class IOFileParser:

    def __init__(self):
        self.listofInstructions = []
        self.tm = TransactionManager()

    def fileRead(self, file):
        try:
            inpFile = open(file, 'r')
            listOfOperations = inpFile.readlines()
            if len(listOfOperations) != 0:
                for i in range(0, len(listOfOperations)):
                    # print(listOfOperations[i])
                    if listOfOperations[i].find("//") == -1 and (listOfOperations[i].strip()):
                        self.listofInstructions.append(listOfOperations[i])
                for i in range(0, len(self.listofInstructions)):
                    # print(self.listofInstructions[i])
                    self.executeOperation(self.listofInstructions[i])
        except IOError:
            print("Could not read file:")

    def executeOperation(self, operationLine):
        """
            List of transactions in the transaction file - Begin, BeginRO, R, W, end, fail, dump, recover.
        """
        #print("All Set")
        switcher = {
            "beginRO": operationLine[operationLine.find("T") + 1],
            "begin": operationLine[operationLine.find("T") + 1],
            "R": operationLine,  # print(operationLine[operationLine.find("x")+1]),
            "W": operationLine,
            # print(operationLine[operationLine.find("x")+1])#print(operationLine.split(',')[2]),
            "end": operationLine[operationLine.find("T") + 1],
            "fail": operationLine[operationLine.find("(") + 1 : operationLine.find(")")],
            "dump": operationLine,
            "recover": operationLine[operationLine.find("(") + 1 : operationLine.find(")")],
            "nothing": "Unable to parse"
        }

        if operationLine.startswith("beginRO"):
            #print(switcher.get("beginRO", "nothing"))
            TransactionNumber = switcher.get("beginRO", "nothing")
            #print(TransactionNumber)
            self.tm.startTransaction("ReadOnly", TransactionNumber)
            print("\n *** Operation Ends *** \n")
        elif operationLine.startswith("begin"):
            #print(switcher.get("begin", "nothing"))
            TransactionNumber = switcher.get("begin", "nothing")
            #print(TransactionNumber)
            self.tm.startTransaction("ReadWrite", TransactionNumber)
            print("\n *** Operation Ends *** \n")
        elif operationLine.startswith("R"):
            print(switcher.get("R", "nothing"))
            operationLine = switcher.get("R", "nothing")
            transNum = operationLine[operationLine.find("T") + 1: operationLine.find(",")]
            varNum = operationLine[operationLine.find("x") + 1: operationLine.find(")")]
            #print(transNum)
            #print(varNum)
            if self.tm.transactionMap.get(transNum) is not None:
                self.tm.addTransactionSiteMap(transNum, varNum)
                self.tm.readOp(transNum, varNum)
            print("\n *** Operation Ends *** \n")
        elif operationLine.startswith("W"):
            print(switcher.get("W", "nothing"))
            operationLine = switcher.get("W", "nothing")
            StringParse = operationLine[operationLine.find("(") + 1: operationLine.find(")")]
            result = [x.strip() for x in StringParse.split(',')]
            varNum = result[1][result[1].find("x") + 1:]
            TransNum = result[0][result[0].find("T") + 1:]
            val = result[2]
            #print(TransNum)
            #print(varNum)
            #print(val)
            if self.tm.transactionMap.get(TransNum) is not None:
                self.tm.addTransactionSiteMap(TransNum, varNum)
                self.tm.writeOp(TransNum, varNum, val)
            print("\n *** Operation Ends *** \n")
        elif operationLine.startswith("end"):
            #print(switcher.get("end", "nothing"))
            TransactionNumber = switcher.get("end", "nothing")
            #print(TransactionNumber)
            #print("\n ****** \n")
            #print("gowri")
            #print(self.tm.siteMap.get(2).getLockTable(1))
            if self.tm.transactionMap.get(TransactionNumber) is not None:
                self.tm.endTransaction(TransactionNumber)
            print("\n *** Operation Ends *** \n")
        # the site id.
        elif operationLine.startswith("fail"):
            print(switcher.get("fail", "nothing"))
            siteNumber = switcher.get("fail", "nothing")
            #print(siteNumber)
            #print("\n ****** \n")
            self.tm.siteFail(siteNumber)
            print("\n *** Operation Ends *** \n")
        elif operationLine.startswith("dump"):
            op = switcher.get("dump", "nothing")
            print(op)
            print("\n")
            #.getTransactionNumber(iprint("\n ****** \n")
            if op[op.find("(") + 1] == ")":
                self.tm.dumpAll()
            elif op[op.find("(") + 1] == "x":
                var = op[(op.find("x") + 1): (op.find(")"))]
                self.tm.dump(var)
            else:
                var = op[(op.find("(") + 1): (op.find(")"))]
                self.tm.dumpSite(var)
            print("\n *** Operation Ends *** \n")
        # the site id.
        elif operationLine.startswith("recover"):
            print(switcher.get("recover", "nothing"))
            siteNumber = switcher.get("recover", "nothing")
            print(siteNumber)
            #print("\n ****** \n")
            self.tm.siteRecover(siteNumber)
            print("\n *** Operation Ends *** \n")
        else:
            print("There is error in file format")
