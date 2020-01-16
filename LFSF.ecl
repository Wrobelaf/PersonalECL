IMPORT * FROM STD;

EXPORT LFSF(STRING5 ftype,UNSIGNED generation) := MODULE

    SHARED GenerationName := ['Current','Father','GrandFather','Delete'];
    
    EXPORT SF     := '~afw::'+ftype+'::'+GenerationName[generation];
    EXPORT LF(STRING8 rundate,STRING2 buildNumber) := SF+'::'+rundate+'::'+buildnumber;
    
    SHARED NextSF := '~afw::'+ftype+'::'+GenerationName[IF(generation=4,4,generation+1)];
    
    // 1st component of regex is file type, e.g. Contributor::Pol01::Current, 2nd component of Regex is Year, 3rd component of regex is month, 4th component of regex is day.
    SHARED Component(STRING fn,integer itm) := REGEXFIND('^(.*'+ftype+'::'+GenerationName[generation]+')::([0-9]{4})([0-9]{2})([0-9]{2})::(.*)$',fn,itm,NOCASE);

    SHARED SfExists    := File.SuperFileExists(SF);            
           AllSortedDS := IF (SfExists,File.SuperFileContents(SF),DATASET([],File.FsLogicalFileNameRecord));
    SHARED SortedDS    := SORT(AllSortedDS(Component(name,4) != ''),{name});                     // Filter to get all 'Daily' build LF, exclude the Monthly LFs.
    SHARED GetLatestLF := IF (EXISTS(SortedDS),SortedDS[COUNT(SortedDS)].name,'');
 
    SHARED GetMonthComponentOfLF(STRING fn)  := Component(fn,3);
    SHARED LFExists(STRING lf) := SfExists AND GetMonthComponentOfLF(lf) != '';
    SHARED MonthRollOver(STRING lf) := LFExists(GetLatestLF) AND GetMonthComponentOfLF(GetLatestLF) != GetMonthComponentOfLF(lf);

    SHARED MonthLFname(STRING lf) := '~'+Component(lf,1)+'::'+Component(lf,2)+Component(lf,3);
    
    // Add this builds LF to this Modules SF, returning the name of the previous builds LF in the SF
    // (said file can then be copied into the father/grandfather)

    SHARED iAddFile(STRING lf) := FUNCTION
  
        mro       := MonthRollOver(lf);
        mlf       := IF(LFExists(GetLatestLF),MonthLFname(GetLatestLF),'');
        Agg       := IF(mro,MERGE(SortedDS,SORTED(name)));
        OutAgg    := IF(mro,OUTPUT(Agg,,mlf));
        AggAdd    := IF(mro,File.AddSuperFile(SF,mlf));
        SrcCopyLF := IF (COUNT(SortedDS) > 1,'~'+SortedDS[COUNT(SortedDS)-1].name,'');        // Get previous 'latest' LF to move into the previous generation.
        TgtCopyLF := NextSF+'::'+Component(SrcCopyLF,2)+Component(SrcCopyLF,3)+Component(SrcCopyLF,4)+'::'+Component(SrcCopyLF,5);
        CopyLF    := IF(SrcCopyLF != '' AND generation < 3,File.Copy(SrcCopyLF,'thor5_64_development',TgtCopyLF));
        //DeleteLF  := IF(mro,APPLY(SortedDS,File.RemoveSuperFile(SF,name,TRUE)));
        DeleteLF  := IF(mro,File.RemoveSuperFile(SF,SortedDS[1].name,TRUE));

        SEQUENTIAL(OutAgg,
                   File.StartSuperFileTransaction(),
                   IF(NOT SfExists,
                      SEQUENTIAL(File.CreateSuperFile(SF),
                                 File.AddSuperFile(SF,lf)),
                      SEQUENTIAL(File.AddSuperFile(SF,lf),
                                 AggAdd)),
                   File.FinishSuperFileTransaction(),
                   CopyLF
                   //DeleteLF
                  );

        RETURN IF(COUNT(SortedDS) > 1,TgtCopyLF,'');    
    END;
    
    EXPORT AddFile(STRING lf) := FUNCTION
        RETURN IF(lf = '','',iAddFile(lf));    
    END;

END;
