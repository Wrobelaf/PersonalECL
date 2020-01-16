//HPCC Systems KEL Compiler Version 0.11.2
IMPORT KEL011 AS KEL;
IMPORT B_Person FROM Wrobel;
IMPORT * FROM KEL011.Null;
EXPORT B_BuildAll_FirstQuery := MODULE
  EXPORT BuildAll := PARALLEL(B_Person.BuildAll);
END;
