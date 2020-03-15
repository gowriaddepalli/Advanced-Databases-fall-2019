# RepCRep - Advanced Databases, fall 2019.

- Built a distributed database complete with multi-version concurrency control, deadlock detection, replication, and failure recovery using object oriented python.

- Authors: 
     Sree Gowri Addepalli (sga297)
	 Sree Lakshmi Addepalli (sla410)


## Documentation
 Design documentation has been added as a design file in the folder.

## Reprozip file
Current folder contains adb_final.rpz that has all the code in folder

- ~/reprounzip_directory_name/root/home/vagrant/ADB-master/folder name to you have given while extracting/RepCReC

Example for unzipping the adb_final.rpz file.

pip install reprounzip
reprounzip showfiles adb_final.rpz
reprounzip info  adb_final.rpz
reprounzip directory setup adb_final.rpz ./check
reprounzip directory run  ./gowri
cd check/
cd root/
cd home/
cd vagrant/
cd ADB-master/
cd gowri/
cd RepCReC/
python Main.py

- warning - the input file in inputFiles folder can contain many text files, but each single text file should be a single testcase and not have multiple testcases.


## Running without reprozip
#### To execute source code without reprozip, navigate to the directory that contains the code:

- /adbmsSGA297_SLA410/RepCReC


- warning - the input file in inputFiles folder can contain many text files, but each single text file should be a single testcase and not have multiple testcases.

#### And run the below command with the testcase test file within the RepCReC folder:
- python Main.py --FileName inp1.txt

### To change the folder location of the testcase file use the folder and place the test file within:
- python Main.py --FileName inputFiles\inp1.txt
(change the inputFiles to the folder name or absolute path of your choice).


### If you want to put the contents to an output file you may use the following command and the console output will be written to file name specified 
- python Main.py --FileName inp1.txt > output1.txt


### if you want to run all the testcases at one attempt, placed in the inputFiles folder in windows:
copy the run.bat file within the RepCReC folder post unzipping and open the windows command line there itself and type run.bat to get the output in the output folder while changing
the number of files to the number of files in the inputFiles folder i.e change (1,1,36) -> (1,1,number of files in the inputFiles folder). Also copy the inputFiles folder/ add testcases within this folder within the RepCReC folder to run the bat file.

## Running with reprounzip
To unzip:
- reprounzip directory setup adb_final.rpz ./gowri

To run:
- reprounzip directory run ./gowri

## Test cases
Test cases and comments have been submitted in the folder inputFiles/  for original testcases and extra ones to be tested.

Incase of queries please contact sga297@nyu.edu or sla410@nyu.edu



