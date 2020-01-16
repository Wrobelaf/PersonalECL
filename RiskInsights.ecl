IMPORT STD
      ,RiskInsights_KEL_Thor
      ,UKServices_Utilities;

EXPORT RiskInsights := MODULE

  SHARED STRING SetupECL(UNSIGNED4 Date,UNSIGNED Iteration) := FUNCTION
      idOfBuild := (STRING)Date+', '+(STRING)Iteration;
      RETURN '#WORKUNIT(\'name\',\'RiskInsights Build: '+idOfBuild+'\');\n'
             +'Wrobel.RiskInsights.Run('+idOfBuild+')';
  END;

  SHARED RunECL(STRING eclStr) := FUNCTION
      Cluster := STD.System.Job.Target();
      RETURN OUTPUT(UKServices_Utilities.WorkUnitManagement.fSubmitNewWorkunit(eclStr,Cluster, Cluster+'.thor'),NAMED('NEXT_BUILD_WU'));
  END;
  
  EXPORT Run(UNSIGNED4 Date,UNSIGNED Iteration) := FUNCTION
      DaisyChain := IF(Iteration > 1,RunECL(SetupECL(STD.Date.AdjustCalendar(Date,,3),Iteration-1)));
      RETURN ORDERED(RiskInsights_KEL_Thor.Actions.BuildAll(Date),DaisyChain);
  END;

  EXPORT Initiate(UNSIGNED4 Date,UNSIGNED NumBuilds) := FUNCTION
      AQuarterDate  := ASSERT(Date % 10000 IN [0101,0401,0701,1001],'Input Date ('+(STRING) Date+') must start on a quarter.',FAIL);
      NoFutureBuild := ASSERT(STD.Date.AdjustCalendar(Date,,NumBuilds*3) <= STD.Date.Today(),'Input parameters cannot initiate a build in the future.',FAIL);
      Checks := ORDERED(AQuarterDate,NoFutureBuild);
      RETURN ORDERED(Checks,RunECL(SetupECL(Date,NumBuilds)));
  END;
    
END;
