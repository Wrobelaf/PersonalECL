EXPORT JobSequencer := MODULE

    // Used by MAC_EventActionSetup
    EXPORT noteReceived(string text) := FUNCTION
      logRecord := { string msg };
      RETURN output(dataset([text], logRecord),NAMED('Received'), extend);
    END;

    // EXPORT MAC_EventActionSetup(InXmlText) := MACRO
        // LOADXML(InXmlText);
        // #FOR(Job)
          // #UNIQUENAME(fname)
          // %fname%() := FUNCTION
          // logRecord := { string msg };
          // msgs := DATASET(WORKUNIT('Received'),logRecord);
          // RETURN IF (count(msgs) = COUNT(%Events%), %Action%);
          // END;
          // #UNIQUENAME(ename)
          // %ename%(STRING name) := FUNCTION
             // RETURN [Wrobel.JobSequencer.noteReceived('Received '+name); %fname%()];
          // END;
          // #FOR(Events)
              // %ename%(%'Name'%) : WHEN(%'Name'%,COUNT(1))
          // #END
        // #END
     // ENDMACRO;

    checkComplete(cntEvents,Action) := FUNCTIONMACRO
      logRecord := { string msg };
      msgs := DATASET(WORKUNIT('Received'),logRecord);
      RETURN IF (count(msgs) = cntEvents, Action);
    ENDMACRO;
   
    processReceived(cntEvents,Action,Name) := FUNCTIONMACRO
       RETURN [Wrobel.JobSequencer.noteReceived('Received '+Name); checkComplete(cntEvents,Action)];
    ENDMACRO;
    
    EXPORT MAC_EventActionSetup(InXmlText) := MACRO
        LOADXML(InXmlText);
        #DECLARE(cntEvents);
        #FOR(Job)
          #SET(cntEvents,0);
          #FOR(Events)
              #SET(cntEvents,%cntEvents%+1);
          #END
          #FOR(Events)
              processReceived(%'cntEvents'%,%Action%,%'Name'%) : WHEN(%'Name'%,COUNT(1));
          #END
        #END
     ENDMACRO;

    // checkComplete() := FUNCTION
      // logRecord := { string msg };
      // msgs := DATASET(WORKUNIT('Received'),logRecord);
      // RETURN IF (count(msgs) = 2, OUTPUT('Dependent Job Completed'));
    // END;
        

    // processReceived(string name) := FUNCTION
      // RETURN [Wrobel.JobSequencer.noteReceived('Received '+name); checkComplete()];
    // END;

    // rd := '20130111';
    // processReceived('Prerequisite_1') : WHEN('Prerequisite_1'+rd, COUNT(1));
    // processReceived('Prerequisite_2') : WHEN('Prerequisite_2'+rd, COUNT(1));

      rName := record
        STRING        Name  {xpath('Name')};
      end;

      RJob := record
        dataset(rName)Events {xpath('Events')};
        STRING        Action {xpath('Action')};
      end;

      EXPORT RJobList := record
        dataset(rJob) Job		{xpath('Job')};
      end;

END;
