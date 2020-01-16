IMPORT STD
      ,UKServices_Utilities;



EXPORT DaisyChainWU(STRING RaiseEventName,STRING ReplyEventName) := MODULE

  EXPORT PersistECLName := '~'+RaiseEventName+'::'+ReplyEventName;

  EXPORT STRING ParamsToXML(SET OF STRING Params) := FUNCTION
      /*
          Input a SET OF STRING ['Parameter 1','Parameter 2','Parameter 3']
          Output a single STRING of XML. e.g.
          Str := '<Row>'
                +'<Param>Parameter 1</Param>'
                +'<Param>Parameter 2</Param>'
                +'<Param>Parameter 3</Param>'
                +'</Row>';
      */
      ParamDS := DATASET(COUNT(Params),TRANSFORM({STRING Param};SELF.Param := Params[COUNTER]));
      p       := PROJECT(ParamDS,TRANSFORM({STRING Param};SELF.Param := '<Param>'+LEFT.Param+'</Param>'));
      RETURN '<Row>'+UKServices_Utilities.ConcatenateStringFields(p,Param,'')+'</Row>';
  END;

  EXPORT SET OF STRING XMLToParams(STRING ParameterStr) := FUNCTION
      /*
          Input a single STRING of XML. e.g.
          Str := '<Row>'
                +'<Param>Parameter 1</Param>'
                +'<Param>Parameter 2</Param>'
                +'<Param>Parameter 3</Param>'
                +'</Row>';

          Output a SET OF STRING ['Parameter 1','Parameter 2','Parameter 3']
      */
      RParams := RECORD
          SET OF STRING Params{XPATH('Param')};
      END;
        //  STD.System.Log.dbglog('ParameterStr: '+ParameterStr);
      RETURN FROMXML(RParams,ParameterStr).Params;
  END;

  EXPORT STRING SetupECL(SET OF STRING Params) := FUNCTION

      ParamDS     := DATASET(COUNT(Params),TRANSFORM({STRING Param};SELF.Param := Params[COUNTER]));
      EclTemplate := DATASET(PersistECLName,{STRING txt},THOR)[1].txt;

      /* Populate the ECL with all the parameters supplied in 'Params' 
            substitution is of the form  XY<n>YX   => Params[<n>]
            So the 1st parameter (Params[1]) replaces static text XY1YX, and Params[34] replaces static text XY34YX.
            The same parameter can replace multiple occurrences of its place holder.
      */

      RECORDOF(ParamDS) Replace(RECORDOF(ParamDS) L,RECORDOF(ParamDS) R,UNSIGNED Cnt) := TRANSFORM
          Ecl := IF(L.Param = '',EclTemplate,L.Param);
          SELF.Param := REGEXREPLACE('XY'+(STRING)Cnt+'YX',ECL,R.Param);
      END;

      STRING EclToRun := IF(EXISTS(ParamDS),ITERATE(ParamDS,Replace(LEFT,RIGHT,COUNTER))[COUNT(ParamDS)].Param,EclTemplate);

      RETURN 'IMPORT * FROM UKServices_Utilities;\n'                          // Leave the #WORKUNIT('name'...) for the caller to insert.
            +'NotifyCompletion(BOOLEAN Continue,STRING Params) := FUNCTION\n'
            +'    ReFire := FUNCTION\n'
            +'        M := DaisyChainWU(\''+RaiseEventName+'\',\''+ReplyEventName+'\');\n'
            +'        P := M.XMLToParams(EVENTEXTRA(\'Param\'));                 // From the \'Reply\' Event.\n'
            +'        RETURN IF(Continue AND P[1] != \'END\',M.RunECL(M.SetupECL(P)));\n'
            +'    END;\n'
            +'    ReFire : WHEN(EVENT(\''+ReplyEventName+'\',\'*\'),COUNT(1));\n'
            +'    RETURN NOTIFY(EVENT(\''+RaiseEventName+'\',Params));\n'
            +'END : GLOBAL;\n'
            +EclToRun
            +'DoIt : FAILURE(NotifyCompletion(FALSE,\''+REGEXREPLACE('(<Row>)',ParamsToXML(Params),'\\1<Param>FAILURE</Param><Param>\'+WORKUNIT+\'</Param>')+'\'))\n'
            +'     , SUCCESS(NotifyCompletion(TRUE, \''+REGEXREPLACE('(<Row>)',ParamsToXML(Params),'\\1<Param>SUCCESS</Param><Param>\'+WORKUNIT+\'</Param>')+'\'));\n';
  END;

  EXPORT RunECL(STRING eclStr) := FUNCTION
      Cluster := STD.System.Job.Target();
      RETURN OUTPUT(UKServices_Utilities.WorkUnitManagement.fSubmitNewWorkunit(eclStr,Cluster, Cluster+'.thor'),NAMED('NEXT_WU'));
  END;

  ////////////////////////////////////////////////////////////////////////////////////////
  // Initiate a sequence of WU's. Run sequentually
  ////////////////////////////////////////////////////////////////////////////////////////
  
  EXPORT Initiate(SET OF STRING Params) := FUNCTION
      RETURN RunECL(SetupECL(Params));
  END;

  EXPORT DoBuild(UNSIGNED4 date,BOOLEAN Die) := FUNCTION
      RETURN ORDERED(ASSERT(NOT Die,'Born to Die!',FAIL),OUTPUT((STRING) date,NAMED('Date_of_build')));
  END;

  EXPORT SimpleTestUseOfWorkman(Integer Iteration,STRING BuildDate) := FUNCTION

    RETURN CASE(Iteration, 1 => DoBuild(20180701,FALSE)
                         , 2 => DoBuild(20180727,FALSE)
                         , 3 => Wrobel.ExamplePARSE
                         ,      DoBuild(20190727,TRUE)
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

    EclToRun := CASE(Iteration, 1 => StdEcl('Wrobel.DaisyChainWU(\\\'a\\\',\\\'b\\\').DoBuild(20180727,FALSE);\'')
                              , 2 => StdEcl('Wrobel.DaisyChainWU(\\\'a\\\',\\\'b\\\').DoBuild(20180727,FALSE);\'')
                              , 3 => StdEcl('Wrobel.ExamplePARSE;\'')
                              ,      StdEcl('Wrobel.DaisyChainWU(\\\'a\\\',\\\'b\\\').DoBuild(20190104,TRUE);\''));
    RETURN RunECL(EclToRun);
  END;
  
END;