// Ru = Rollup
// Make copy of Fathers state in grandfather and Current state in Father.
// Done prior to any bulid. The build process Aggrigates into months that's all.

IMPORT * FROM UKServices_utilities;

EXPORT Ru2(STRING fType) := MODULE

    SHARED CURRENT     := 1;
    SHARED FATHER      := 2;
    SHARED GRANDFATHER := 3;
           GenerationName               := ['Current','Father','GrandFather'];
    EXPORT SF(INTEGER generation)       := '~'+ftype+'::'+GenerationName[generation];

     // 1st component of regex is file type, e.g. Contributor::Pol01::Current.
     // 2nd component of regex is Year. 
     // 3rd component of regex is month. 
     // 4th component of regex is day.
     // 5th component of regex is '::' seperator between date and build number.
     // 6th component of regex is the build number.
     // 7th component of regex is any trailing suffix in the filename.
    SHARED PREFIX      := 1;
    SHARED YEAR        := 2;
    SHARED MONTH       := 3;
    SHARED DAY         := 4;
    SHARED SEPERATOR   := 5;
    SHARED BUILDNUMBER := 6;
    SHARED SUFFIX      := 7;
    SHARED Component(STRING fn,integer itm) := REGEXFIND('^(.*'+fType+'.*)::([0-9]{4})([0-9]{2}|$)([0-9]{2}|$)(::|$)([0-9]{2}|$)(.*$)',fn,itm,NOCASE);

    SHARED SfExists(INTEGER generation)     := fileservices.FileExists(SF(generation));
    SHARED CreateSF(INTEGER generation)     := IF(NOT SfExists(generation),fileservices.CreateSuperFile(SF(generation)));
    SHARED SFContents(INTEGER generation)   := IF(SfExists(generation),
                                                  fileservices.SuperFileContents(SF(generation)),
                                                  DATASET([],FsLogicalFileNameRecord)
                                                  );
    SHARED SortedDailyLFsCurrent            := SORT(SFContents(CURRENT)(Component(name,DAY) != ''),-name);             // Filter to get all 'Daily' build LF, exclude the Monthly LFs.
    SHARED LatestLFCurrent                  := IF (EXISTS(SortedDailyLFsCurrent),SortedDailyLFsCurrent[1].name,'');

           FileRunDate(STRING fn)           := Component(fn,YEAR)+Component(fn,MONTH)+Component(fn,DAY);

    // Return the next free logical filename for 'current', given a 'rundate' of YYYYMMDD
    // Requires that the SF be populated with all previous builds LFs. (Uses contents of SF to workout the next build number.)
    EXPORT LF(STRING8 rundate)              := SF(CURRENT)+'::'+rundate+'::'+IF(FileRunDate(LatestLFCurrent)=rundate,INTFORMAT(((INTEGER)Component(LatestLFCurrent,BUILDNUMBER)+1),2,1),'01');

    SHARED CleanOutCurrentDailys(BOOLEAN MonthRollOver) := FUNCTION
         delFiles  := GLOBAL(SortedDailyLFsCurrent,FEW);
         RETURN NOTHOR(IF(GLOBAL(MonthRollOver) AND EXISTS(delFiles),
                          APPLY(delFiles,fileservices.RemoveSuperFile(SF(CURRENT),'~'+name,TRUE))
                          )
                       );
    END;

    // Adds built logical file named 'lf' to the 'Current' superfile. Requires a callback to perform the actual MERGE of datasets into a monthly dataset.
    // Use of a callback allows this code to work with any type of dataset.
    // Example of a callback function:
    // Bundleupfiles(BOOLEAN RollOver,STRING dailyfiles,STRING monthlyfile) := FUNCTION
          // f := DISTRIBUTE(DATASET('{'+dailyfiles+'}',R,FLAT),HASH32(typ));
          // IF(RollOver,OUTPUT(f,,monthlyfile,OVERWRITE));
    // END;
    
    EXPORT AddFile(STRING lf,BOOLEAN R(BOOLEAN a,STRING b,STRING c)) := FUNCTION
    
      GetMonthComponentOfLF(STRING fn)  := Component(fn,MONTH);
      PreviousLFExists                  := SfExists(CURRENT) AND LatestLFCurrent != '';
      MonthRollOver                     := PreviousLFExists AND GetMonthComponentOfLF(LatestLFCurrent) != GetMonthComponentOfLF(lf);
      MonthLFname                       := '~'+Component(LatestLFCurrent,PREFIX)+'::'+Component(LatestLFCurrent,YEAR)+Component(LatestLFCurrent,MONTH);
      DailysToBeMerged                  := '~'+ConcatenateStringFields(SortedDailyLFsCurrent,name,',~');
      TargetMonthlyFile                 := IF(MonthRollOver,MonthLFname,'');
      AddMonthlyFileToCurrentSF         := IF(MonthRollOver,fileservices.AddSuperFile(SF(CURRENT),TargetMonthlyFile));

      SEQUENTIAL(fileservices.StartSuperFileTransaction(),
                 CreateSF(CURRENT),                                   // Should not need as 'PromoteLogicalFiles' must have been done prior to any build and would have created the superfiles.
                 R(MonthRollOver,DailysToBeMerged,TargetMonthlyFile), // Merge a months 'daily' builds into one 'monthly' logical file.
                 CleanOutCurrentDailys(MonthRollOver),                // Delete the 'daily' files that have been moved into the 'monthly' file.
                 AddMonthlyFileToCurrentSF,                           // Add any newly created 'monthly' logical file into the 'current' superfile.
                 fileservices.AddSuperFile(SF(CURRENT),lf),           // Add current builds daily logical file 'lf' into the superfile.
                 fileservices.FinishSuperFileTransaction()
                );
      RETURN TRUE;
    END;
    
    // As filenames between generations differ, to identify identical files one must only use the rundate and build number in the comparison. (plus any suffix)
    // Note for aggrigate 'month' files only compare year and month components of the filename. 
    Comparable(STRING fn)        :=  Component(fn,YEAR)+Component(fn,MONTH)
                                    +IF(LENGTH(Component(fn,DAY))>0,        Component(fn,DAY),'DD')
                                    +IF(LENGTH(Component(fn,BUILDNUMBER))>0,Component(fn,BUILDNUMBER),'BN')
                                    +IF(LENGTH(Component(fn,SUFFIX))>0,     Component(fn,SUFFIX),'SFX');   

    // Target filename for whatever process is being done.
    DestFile(INTEGER generation,STRING SrcFile)
                                 :=  SF(generation)+'::'+Component(SrcFile,YEAR)+Component(SrcFile,MONTH)
                                    +IF(LENGTH(Component(SrcFile,DAY))>0,        Component(SrcFile,DAY),'')
                                    +IF(LENGTH(Component(SrcFile,SEPERATOR))>0,  Component(SrcFile,SEPERATOR),'')
                                    +IF(LENGTH(Component(SrcFile,BUILDNUMBER))>0,Component(SrcFile,BUILDNUMBER),'')
                                    +IF(LENGTH(Component(SrcFile,SUFFIX))>0,     Component(SrcFile,SUFFIX),'');
    
    // Copy all files in 'generation' to 'generation+1' that are not already in 'generation+1'.
    // Note that 'Copy' retains the DISTRIBUTED state of the source file.
    CopyLF(INTEGER generation) := FUNCTION
         FilesToCopy  := JOIN(SFContents(generation),SFContents(generation+1),Comparable(LEFT.name) = Comparable(RIGHT.name),LEFT ONLY);
         myFiles      := GLOBAL(FilesToCopy,FEW);
         RETURN NOTHOR(IF(EXISTS(myFiles),
                          APPLY(myFiles,fileservices.Copy('~'+name,'thor5_64_development',DestFile(generation+1,name)),
                                        fileservices.AddSuperFile(SF(generation+1),DestFile(generation+1,name))
                                )
                          )
                       );
    END;

    // Delete all files in 'generation' that are not in 'generation-1'.
    // This kicks in when a months worth of daily logical files get aggrigated into a monthly logical file on the 1st build of the month.
    // The new monthly aggrigated file once promoted up a generation replaces any daily files for that month still in the target superfile.
    // These daily logical files must me removed.
    CleanUp(INTEGER generation)  := FUNCTION
         FilesToDelete := JOIN(SFContents(generation),SFContents(generation-1),Comparable(LEFT.name) = Comparable(RIGHT.name),LEFT ONLY);
         myFiles       := GLOBAL(FilesToDelete,FEW);
         RETURN NOTHOR(IF(EXISTS(myFiles),
                          APPLY(myFiles,fileservices.RemoveSuperFile(SF(generation),DestFile(generation,name),TRUE))
                          )
                       );
    END;

    // Copy the logical file state from superfile 'generation' to 'generation+1'.
    // Do this prior to any build of logical files into the 'current' superfile.
           iPromoteLogicalFiles(INTEGER generation)
                                        := SEQUENTIAL(CreateSF(generation),
                                                      CreateSF(generation+1),
                                                      CopyLF(generation),
                                                      CleanUp(generation+1)
                                                      );

    // Promote 'Father' to 'Grandfather' followed by 'Current' to 'Father' prior to any build.
    EXPORT PromoteLogicalFiles          := SEQUENTIAL(iPromoteLogicalFiles(FATHER),
                                                      iPromoteLogicalFiles(CURRENT)
                                                      );
END;
