import argparse
from IOFileParser import IOFileParser

""""
Authors : Sree Lakshmi Addepalli (sla410)

"""

"""
References: 

All status notations have been taken from: https://www.tutorialspoint.com/distributed_dbms/distributed_dbms_transaction_processing_systems.htm

"""

parser = argparse.ArgumentParser(description='Replicated Concurrency Control and Recovery')
parser.add_argument('--FileInputPath', type=str, default='.\inputFiles', metavar='path',
                    help="folder where input data is located.")
parser.add_argument('--FileName', type=str, default='inp1.txt', metavar='pathFile',
                    help="file of input data is located.")
args = parser.parse_args()


class Main:
    """

    This is the main class for the start of the program.

    """

    def __init__(self, fileInputPath, fileName):
        self.fileInputPath = fileInputPath
        self.fileName = fileName
        self.ioFileParser = IOFileParser()


if __name__ == "__main__":
    main = Main(args.FileInputPath, args.FileName)
    finalPath = args.FileName
    # print(finalPath)
    main.ioFileParser.fileRead(finalPath)
    #code for autogeneration of emails.
    #for i in range(1,24):
        #filR = args.FileInputPath+"\\"+ "inp"+str(i)+".txt"
        #print(filR)
