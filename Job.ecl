IMPORT STD;
IMPORT _Control;
IMPORT ProjectUK_Deltas;

EXPORT Job := MODULE

    SHARED STRING rundate := (STRING)stringlib.getDateYYYYMMDD();

    EXPORT LZ(STRING fn) := FUNCTION        // e.g. fn = 'transaction_log.txt'
      #workunit('name','make LZ '+fn);
      
      dirname := TRIM(ProjectUK_Deltas.delta_Constants(fn).dirname,LEFT,RIGHT);
      RootLZ  := REGEXREPLACE('/([^/.]+)/$',dirname,'/');      // Strip off one level of sub-dir to get to dir MBS exports to.
                
      FileExported  := EXISTS(STD.File.RemoteDirectory(_Control.ThisEnvironment.LandingZone,RootLZ+rundate+'/',fn+'.flag'));
      FileProcessed := EXISTS(STD.File.RemoteDirectory(_Control.ThisEnvironment.LandingZone,dirname+rundate+'/',fn));
      
      RETURN MAP(NOT FileExported                    => OUTPUT(FALSE,NAMED('RESULT'),OVERWRITE)
                ,FileExported AND NOT FileProcessed  => SEQUENTIAL(STD.File.CreateExternalDirectory(_Control.ThisEnvironment.LandingZone
                                                                                                   ,dirname+rundate)
                                                                  ,STD.File.MoveExternalFile(_Control.ThisEnvironment.LandingZone
                                                                                            ,RootLZ+rundate+'/'+fn
                                                                                            ,dirname+rundate+'/'+fn)
                                                                  ,OUTPUT(TRUE,NAMED('RESULT'),OVERWRITE))
                ,FileExported AND FileProcessed      => OUTPUT(TRUE,NAMED('RESULT'),OVERWRITE)
                ,ASSERT(FALSE,'Inconsistant state of files on LZ, Fileexported: '+FileExported+', FileProcessed: '+FileProcessed,FAIL));
     END;


    EXPORT LF(STRING fn) := FUNCTION
      #workunit('name','make LF '+fn);

      dirname     := TRIM(ProjectUK_Deltas.delta_Constants(fn).dirname,LEFT,RIGHT);
      FileToSpray := EXISTS(STD.File.RemoteDirectory(_Control.ThisEnvironment.LandingZone,dirname+rundate+'/',fn));

      lfn         := MAP(fn = ProjectUK_Deltas.delta_Constants().Intlog   => '~thor::base::deltauk::intermediate_log::current::'+rundate+'::001'
                        ,fn = ProjectUK_Deltas.delta_Constants().Translog => '~thor::base::deltauk::transaction_log::current::'+rundate+'::001'
                        ,ERROR('SUPPLIED INVALID FEILNAME: '+fn));
      LFExists    := STD.File.FileExists(lfn);

      RETURN MAP(NOT FileToSpray               => OUTPUT(FALSE,NAMED('RESULT'),OVERWRITE)
                ,FileToSpray AND NOT LFExists  => SEQUENTIAL(ProjectUK_Deltas.BuildDeltaExports(rundate,fn).SprayBuildBaseAndKeys
                                                            ,OUTPUT(TRUE,NAMED('RESULT'),OVERWRITE))
                ,FileToSpray AND LFExists      => OUTPUT(TRUE,NAMED('RESULT'),OVERWRITE)
                ,ASSERT(FALSE,'Inconsistant state of files on HPCC, FileToSpray: '+FileToSpray+', LFExists: '+LFExists,FAIL));
    END;


    EXPORT DW := FUNCTION
      #workunit('name','make DW');
      RETURN OUTPUT(TRUE,NAMED('RESULT'),OVERWRITE);
    END;

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

END;
