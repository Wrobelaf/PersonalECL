IMPORT STD;
IMPORT _Control;
IMPORT ProjectUK_Deltas;
IMPORT UKServices_Utilities;

EXPORT make := MODULE

    EXPORT A := FUNCTION
      #workunit('name','A');
      RETURN OUTPUT(TRUE,NAMED('RESULT'),OVERWRITE);
    END;    

    EXPORT B := FUNCTION
      #workunit('name','B');
      RETURN OUTPUT(TRUE,NAMED('RESULT'),OVERWRITE);
    END;    

    EXPORT C := FUNCTION
      #workunit('name','C');
      RETURN OUTPUT(TRUE,NAMED('RESULT'),OVERWRITE);
    END;    

    EXPORT D := FUNCTION
      #workunit('name','D');
      RETURN OUTPUT(TRUE,NAMED('RESULT'),OVERWRITE);
    END;    


    SHARED MakeConfig := '~afw::make::config';

    SHARED LayoutConfig := RECORD
        STRING Target;
        STRING Dependencies;
    END;

    SHARED Config := DATASET([{'Wrobel.make.D',''}
                             ,{'Wrobel.make.C',''}
                             ,{'Wrobel.make.B',''}
                             ,{'Wrobel.make.A','Wrobel.make.B Wrobel.make.C Wrobel.make.D'}],LayoutConfig);
    
    // EXPORT SetUp := FUNCTION

      // RETURN OUTPUT(DATASET([{'Wrobel.Job.D',''}
                            // ,{'Wrobel.Job.C',''}
                            // ,{'Wrobel.Job.B',''}
                            // ,{'Wrobel.Job.A','Wrobel.Job.B|Wrobel.Job.C|Wrobel.Job.D'}],LayoutConfig),,MakeConfig,THOR,OVERWRITE);
    // END;

		SHARED Cluster := NOTHOR(STD.System.Workunit.WorkunitList(WORKUNIT,WORKUNIT)[1].cluster);

    EXPORT make(STRING tgt) := FUNCTION
    
      tgtrow    := Config(Target = tgt);
      tgtExists := EXISTS(tgtrow);
      Badtgt    := COUNT(tgtrow) != 1;


      RDeps := RECORD
          STRING Dependency;
				  BOOLEAN result;
			END;

      Deps := PROJECT(DATASET(STD.Str.SplitWords(tgtrow[1].Dependencies,' '),{STRING Dependency}),TRANSFORM(RDeps,SELF.result := TRUE;SELF := LEFT));

      BOOLEAN RunJob(STRING ecl) := FUNCTION

          BOOLEAN GetResult(STRING wuid) := FUNCTION      // Note DATASET(WORKUNIT...  to get the result form a WU does not work.

              rWURequest	:= RECORD
                string										Wuid{XPATH('Wuid')}		:=	wuid;
                string										ResultName{XPATH('ResultName')}		:=	'RESULT';
                boolean										SuppressXmlSchema{XPATH('SuppressXmlSchema')}		:=	true;
              end;

              rWUResponse := RECORD
                STRING										result{XPATH('Result')};
              END;

              rWUResponse genResult() := TRANSFORM
                SELF.result := '<Row><RESULT>false</RESULT></Row>';     // Default to FALSE to stop the 'make' in its tracks.
              END;

              res := 	SOAPCALL(_Control.ThisEnvironment.ESP_ServerIPPort + '/WsWorkunits',
                                           'WUResult',
                                           rWURequest,
                                           dataset(rWUResponse),
                                           LITERAL,
                                           XPATH('WUResultResponse'),
                                           ONFAIL(genResult()),
                                           TIMEOUT(0)
                                          );
               RETURN REGEXFIND('true',REGEXFIND('<RESULT>(.+)</RESULT>',res[1].result,1),NOCASE);
          END;

          wuid := UKServices_Utilities.WorkUnitManagement.fSubmitNewWorkunit(ecl, Cluster, Cluster+'.thor');
          info := UKServices_Utilities.WorkUnitManagement.fWaitComplete(wuid);
          RETURN info.result = 3 AND GetResult(info.wuid);    // 3 = successful wait
      END;
      
      RDeps RunDependency(RDeps L,RDeps R) := TRANSFORM
          
          nexttsk := Config(Target = R.Dependency);
          
          tsk := IF(nexttsk[1].Dependencies != '','Wrobel.make.make(\''+R.Dependency+'\');',R.Dependency+';');
          SELF.result := IF(L.Dependency = '' OR (L.Dependency != '' AND L.result),RunJob(tsk),FALSE);
          SELF := R;

      END;
      
      p := ITERATE(Deps,RunDependency(LEFT,RIGHT));

      // Can only run the 'tgt' itself if every dependency has returned TRUE.

      tgtres := IF(NOT EXISTS(p(NOT result)),RunJob(tgt+';'),FALSE);
      
      RETURN MAP(NOT tgtExists    => SEQUENTIAL(OUTPUT(FALSE,NAMED('RESULT'),OVERWRITE)
                                               ,ASSERT(FALSE,'Bad Target Supplied: '+tgt,FAIL))
                ,Badtgt           => SEQUENTIAL(OUTPUT(FALSE,NAMED('RESULT'),OVERWRITE)
                                               ,ASSERT(FALSE,'Bad make file',FAIL))
                ,OUTPUT(tgtres,NAMED('RESULT'),OVERWRITE)
                );
    END;
    
END;
