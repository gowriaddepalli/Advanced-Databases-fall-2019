SETLOCAL ENABLEDELAYEDEXPANSION
FOR /L %%A IN (1,1,36) DO (
  set "@var=.\inputFiles\inp%%A.txt"
  set "@varop=.\outputFiles\out%%A.txt"
  ECHO !@var!
  ECHO !@varop!
  python Main.py --FileName !@var! > !@varop!
)