IMPORT STD
      ,DOPS_Common
      ,lib_system;


////////////////////////////////////////////////////////////////////////////////////////
// Daisy Chain WUs Unit Test. The worlds most inefficent method of working out 6!
////////////////////////////////////////////////////////////////////////////////////////

Factorial := 6;

#WORKUNIT('name','UNIT TEST UKServices_Utilities.DaisyChainWU');
dcWU := UKServices_Utilities.DaisyChainWU('DaisyChainUTFire','DaisyChainUTReply');
d := DATASET([{'#WORKUNIT(\'name\',\'Calculate Factorial XY1YX\');\n'
           +'doIt := FUNCTION\n'
           +'     RRes := {INTEGER itm};\n'
           +'     BodyFunc(DATASET(RRes) ds,UNSIGNED c) := PROJECT(ds,TRANSFORM(RRes;SELF.itm := LEFT.itm * c));\n'
           +'     RETURN OUTPUT(\'XY1YX! = \'+(STRING) LOOP(DATASET([{1}],RRes),XY1YX,BodyFunc(ROWS(LEFT),COUNTER),FEW)[1].itm,NAMED(\'FACTORIAL\'));\n'
           +'END;\n'}],{STRING txt});
o := OUTPUT(d,,dcWU.PersistECLName,THOR,OVERWRITE);


GotAnEvent := FUNCTION

    STD.System.Log.dbglog('EVENTEXTRA: '+EVENTEXTRA('Param'));
    SET OF STRING param := dcWU.XMLToParams(EVENTEXTRA('Param'));
    STRING Result := param[1];
    STRING WUID   := param[2];
    INTEGER iNextParam := (INTEGER)Param[3] + 1;
    STRING NextParam := IF(Result = 'FAILURE' OR iNextParam = 6,'END',(STRING) iNextParam);

    STRING ErrMess := IF(Result = 'FAILURE','WU '+WUID+' Failed.','6! not calculated as 720.');
    
    fs := fileservices.sendemail('uki-hpccapplications@lexisnexisrisk.com',
                                 'DaisyChain UNIT TEST Stage has completed: '+Result+' ('+WUID+')',
                                 'This Notify got a parameters of: '+param[3],
                                 lib_system.smtpserver,
                                 lib_system.smtpport,
                                 lib_system.emailAddress);
    Chk    := ASSERT(Result = 'SUCCESS','DaisyChainWU_UT FAILED because '+ErrMess,FAIL);
    RETURN PARALLEL(Chk
                   ,fs
                   ,NOTHOR(NOTIFY(EVENT('DaisyChainUTReply',dcWU.ParamsToXML(['END']))))
                   ); 
END;

GotAnEvent : WHEN(EVENT('DaisyChainUTFire','*'),COUNT(Factorial));

//ORDERED(o,dcWU.Initiate(['1']));

