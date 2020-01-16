IMPORT STD
      ,_Control
      ,DOPS_Common
      ,UKServices_Utilities
      ,wk_ut;

#if(_Control.ThisEnvironment.Name = 'Prod')
  export	string					EmailErrorTarget	:=	DOPS_Common.EmailAddress.DataOps;
  export	string					TargetWUCluster		:=	'thorprod3';
#elseif(_Control.ThisEnvironment.Name = 'QA')	
  export	string					EmailErrorTarget	:=	DOPS_Common.EmailAddress.QA;
  export	string					TargetWUCluster		:=	'thorqa1';
#else
  export	string					EmailErrorTarget	:=	'uki-hpccapplications@lexisnexisrisk.com';
  export	string					TargetWUCluster		:=	'thordev3';
#end

EXPORT RiskBuildInitiate(STD.Date.Date_t Date,INTEGER numiters = 20,STRING RunCluster = TargetWUCluster) := FUNCTION
/*
    Note 'RunCluster' defiens the queue the builds will run on. Currently the HTHOR that the controlling WU's run on is hard coded in
    wk_ut._Constants, which is a pain.
*/
    ControlFileRoot := '~wk_ut::';
   // StartDate := (STRING)STD.Date.AdjustDate((STD.Date.Date_t) UKServices_Utilities.DateUtils.GetQuarterDate((STRING8)Date),,-numiters*3);
    StartDate := '20190101';
    startiter := 1;
    o := OUTPUT(StartDate,NAMED('StartDate'));
    dsf := STD.File.DeleteSuperFile(ControlFileRoot+'workunit_history',TRUE);
    PollingFrequency := '1';
    OutputEcl := false;

    ecltext :=  'iteration         := \'@iteration@\';\n'
               +'pStartDate          := \'@StartDate@\';\n'
               +'PreClusterCount   := \'1234567\';\n'
               +'PostClusterCount  := \'213456\';\n'
               +'MatchesPerformed  := \'77849594\';\n'
               +'myset             := [100,80,40,30,20,10,1];\n\n'
               +'#workunit(\'name\',\'PIRnnnn \' + pStartDate + \' \' + iteration);\n\n'
               +'output(iteration        ,named(\'iteration\'       ));\n'
               +'output(pStartDate         ,named(\'pStartDate\'        ));\n'
               +'output(PreClusterCount  ,named(\'PreClusterCount\' ));\n'
               +'output(PostClusterCount ,named(\'PostClusterCount\'));\n'
               +'output(myset[(unsigned)iteration] ,named(\'MatchesPerformed\'));';

    kickiters := wk_ut.mac_ChainWuids(ecltext
                                     ,startiter
                                     ,numiters
                                     ,StartDate
                                     ,['PreClusterCount','PostClusterCount','MatchesPerformed']
                                     ,RunCluster
                                     ,pNotifyEmails := EmailErrorTarget
                                     ,pOutputEcl := OutputEcl
                                     ,pPollingFrequency := PollingFrequency
                                     ,pOutputFilename   := ControlFileRoot + StartDate + '_@iteration@::workunit_history::test_att'
                                     ,pOutputSuperfile  := ControlFileRoot + 'workunit_history' );

    RETURN ORDERED(o,dsf,kickiters);
END;
