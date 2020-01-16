IMPORT STD;
IMPORT UKServices_Utilities;

EXPORT ExersizeWorkman := MODULE

  EXPORT RunECL(STRING eclStr) := FUNCTION
      Cluster := STD.System.Job.Target();
      RETURN OUTPUT(UKServices_Utilities.WorkUnitManagement.fSubmitNewWorkunit(eclStr,Cluster, Cluster+'.thor'),NAMED('NEXT_WU'));
  END;

  EXPORT DoBuild(UNSIGNED4 date,BOOLEAN Die) := FUNCTION
      RETURN ORDERED(ASSERT(NOT Die,'Born to Die!',FAIL),OUTPUT((STRING) date,NAMED('Date_of_build')));
  END;

  EXPORT SimpleTestUseOfWorkman(Integer Iteration,STRING BaseBuildDate) := FUNCTION

    Date := STD.Date.AdjustDate((STD.Date.date_t)BaseBuildDate,0,Iteration-1,0);
    RETURN CASE(Iteration, 1 => DoBuild(Date,FALSE)
                         , 2 => DoBuild(Date,FALSE)
                         , 3 => Wrobel.ExamplePARSE
                         ,      DoBuild(Date,TRUE)
               );
  END;

  EXPORT TestUseOfWorkman(Integer Iteration) := FUNCTION

   StdEcl(STRING task) := 'wk_ut.mac_ChainWuids(pECL:=\''
             +Task
             +'\n,pIterationStartValue:=1\n'
             +',pNumIterations:=1\n'
             +',pversion:=\'XXXX\'\n'
             +',pcluster:=\'thordev3\'\n'
             +',pNotifyEmails:=\'allan.wrobel@lexisnexisrisk.com\'\n'
             +',pOutputEcl:=false);\n';

    EclToRun := CASE(Iteration, 1 => StdEcl('Wrobel.ExersizeWorkman.DoBuild(20180727,FALSE);\'')
                              , 2 => StdEcl('Wrobel.ExersizeWorkman.DoBuild(20180727,FALSE);\'')
                              , 3 => StdEcl('Wrobel.ExamplePARSE;\'')
                              ,      StdEcl('Wrobel.ExersizeWorkman.DoBuild(20190104,TRUE);\''));
    RETURN RunECL(EclToRun);
  END;

END;
