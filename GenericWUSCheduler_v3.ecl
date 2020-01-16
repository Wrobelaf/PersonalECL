/////////////////////////////////////////////////////////////////////
// Harness to run multiple workunits in parallel and/or in sequence.
//
// To Install:
//    1.   Set the STRING HOME_FOLDER  to the name of the folder that will
//         hold the WorkUnitManagement MODULE
//    2.   Three further STRING variables are available to configure this workunit scheduler:
//            ESP_service_IP_and_PORT :  the URL to your own ESP service.
//            CLUSTER                 :  Identifies the default cluster the workunits will run on.
//            QUEUE                   :  Identifies the default queue the workunits will run on.
//         The shipped settings may suffice.
//    3.   Copy HOME_FOLDER plus the entire MODULE (exclude the example BWR code at the end)
//         into a empty file named WorkUnitManagement under the folder references at step 1 above.
//    4.   Save the WorkUnitManagement file.
//
// Below this MODULE there is example BWR code that will run a demonstration
// set of workunits.
// Copy the example code into a builder window, set HOME_FOLDER as above
// and just execute either on THOR or HTHOR.
/////////////////////////////////////////////////////////////////////

IMPORT STD;
    
STRING HOME_FOLDER := '';
STRING ESP_service_IP_and_PORT := STD.File.GetEspURL();
STRING CLUSTER := STD.System.Job.Target();
STRING QUEUE   := CLUSTER+'.thor';
 
EXPORT WorkUnitManagement := MODULE

    EXPORT STRING fSubmitNewWorkunit(STRING pECLText, STRING pESPURL = ESP_service_IP_and_PORT,STRING pCluster = CLUSTER,STRING pQueue = QUEUE) := FUNCTION

        STRING fWUCreateAndUpdate := FUNCTION

            rWUCreateAndUpdateRequest := RECORD
              STRING    QueryText{XPATH('QueryText'),maxlength(20000)}  := pECLText;
              BOOLEAN   Protected{XPATH('Protected')}                   := FALSE;
            END;

            rESPExceptions  := RECORD
              STRING    Code{XPATH('Code'),maxlength(10)};
              STRING    Audience{XPATH('Audience'),maxlength(50)};
              STRING    Source{XPATH('Source'),maxlength(30)};
              STRING    Message{XPATH('Message'),maxlength(200)};
            END;

            rWUCreateAndUpdateResponse  := RECORD
              STRING                    Wuid{XPATH('Workunit/Wuid'),maxlength(20)};
              DATASET(rESPExceptions)   Exceptions{XPATH('Exceptions/ESPException'),maxcount(110)};
            END;

            dWUCreateAndUpdateResult  := SOAPCALL( pESPURL + '/WsWorkunits',
                                                   'WUUpdate',
                                                   rWUCreateAndUpdateRequest,
                                                   rWUCreateAndUpdateResponse,
                                                   XPATH('WUUpdateResponse')
                                                  );

            RETURN  dWUCreateAndUpdateResult.WUID;

        END;

        fWUSubmit(STRING pWUID) := FUNCTION

            rWUSubmitRequest  := RECORD
              STRING                    WUID{XPATH('Wuid'),maxlength(20)}                   :=  pWUID;
              STRING                    Cluster{XPATH('Cluster'),maxlength(30)}             :=  pCluster;
              STRING                    Queue{XPATH('Queue'),maxlength(30)}                 :=  pQueue;
              STRING                    Snapshot{XPATH('Snapshot'),maxlength(10)}           :=  '';
              STRING                    MaxRunTime{XPATH('MaxRunTime'),maxlength(10)}       :=  '0';
              STRING                    Block{XPATH('BlockTillFinishTimer'),maxlength(10)}  :=  '0';
            END;

            rWUSubmitResponse := RECORD
              STRING                    Code{XPATH('Code'),maxlength(10)};
              STRING                    Audience{XPATH('Audience'),maxlength(50)};
              STRING                    Source{XPATH('Source'),maxlength(30)};
              STRING                    Message{XPATH('Message'),maxlength(200)};
            END;

            RETURN SOAPCALL( ESP_service_IP_and_PORT + '/WsWorkunits',
                             'WUSubmit',
                             rWUSubmitRequest,
                             rWUSubmitResponse,//dataset(rWUSubmitResponse),
                             XPATH('WUSubmitResponse/Exceptions/Exception'));
        END;

        STRING  lWUIDCreated  :=  fWUCreateAndUpdate;
        dExceptions           :=  fWUSubmit(lWUIDCreated);

        RETURN if(dExceptions.Code = '', lWUIDCreated,dExceptions.Code);
    END;

    ///////////////////////////////////////////////////////////////////////
    // Work unit Daisy Chain Functionality.
    // Any number of parallel streams of sequentially running WU's
    ///////////////////////////////////////////////////////////////////////

    /*
        Define one workunit:
        WUName      : The operand used in the #WORKUNIT('name',...); construct of the workunit. 
        eclFUNCTION : Full path reference to the ECL FUNCTION to run. (Include parameters
                      that are described below)
    */
    EXPORT JobDefinitionLayout := {STRING WUName; STRING eclFUNCTION};
    /*
        Here 'JobNumber' defines what target ECL FUNCTION is run (with #WORKUNIT('name'))
        and 'NextJobNumber' references which target ECL FUNCTION will be run on completion of
        the ECL FUNCTION at state 'JobNumber'. A 'JobNumber' of 0 terminates that particular sequence of WUs.
        (though not those WU's running in PARALLEL)

        Start states, or JobNumbers, are all those JobNumbers that are not referenced in any 'NextJobNumber'
        It is assumed that all such 'Start JobJumbers' can be started in PARALLEL and MACROS to
        generate such ECL, from a given FSM, are supplied below.

        Note the 'JobNumber' and 'NextJobNumber' can be the same. In which case the same target
        FUNCTION is called indefinitely. The harness will rely on the target FUNCTION returning 0
        at some point to terminate.
        This is in place so, for example, an indefinite number of builds can be run in sequence;
        each incrementing a build date from its parents build date until some application defined
        stop condition is reached, e.g. a build date in the future.
    */
    EXPORT WUStateMachineStructure := {UNSIGNED2 JobNumber; UNSIGNED2 NextJobNumber; JobDefinitionLayout};
    /*
        Define workunit sequences that can be run on PARALLEL.
    */
    EXPORT ParallelJobDefinitionLayout := {DATASET(JobDefinitionLayout) Queue};

    SHARED STRING EscapeQuotes(STRING itm) := Std.Str.FindReplace(itm, '\047','\\\047');  //replace the single quotes with backslash-single quote (\’) 

    EXPORT ConcatenateStringFields(_ds_,_fld_,_sep_) := FUNCTIONMACRO
         _ds1_ := PROJECT(_ds_,TRANSFORM({STRING _fld_},SELF._fld_ := LEFT._fld_));
         RETURN ROLLUP(_ds1_,TRUE,TRANSFORM({STRING _fld_},SELF._fld_ := LEFT._fld_ + _sep_ + RIGHT._fld_))[1]._fld_;
    ENDMACRO;

    EXPORT CopyTextOfFSM(DATASET(WUStateMachineStructure) pFSM) := FUNCTION

        // Create an ECL STRING from FSM, basically copy it from one WU to another.
        // Useful in exposing the FSM to target FUNCTIONs and allowing them to use an existing
        // FSM in any WU's they want to start as a new distinct chain or WU's.
        #UNIQUENAME(FSM);

        {UNSIGNED LineNo;STRING txt} ConstructFSM(WUStateMachineStructure L,UNSIGNED Cnt) := TRANSFORM

            SELF.LineNo := Cnt;
            SELF.txt :=   IF(Cnt > 1,'        ,',%'FSM'%+' :=\nDATASET([')
                        + '{' + L.JobNumber
                        + ',' + L.NextJobNumber
                        + ',\'' + EscapeQuotes(L.WUName)
                        + '\',\'' + EscapeQuotes(L.eclFUNCTION) + '\'}\n'; 
        END;
        
        FSMStringFragments := PROJECT(pFSM,ConstructFSM(LEFT,COUNTER));
        
        Fragments       := FSMStringFragments & ROW({MAX(FSMStringFragments,LineNo)+1,'        ],'+HOME_FOLDER+'.WorkUnitManagement.WUStateMachineStructure);'},RECORDOF(FSMStringFragments));
        SortedFragments := SORT(Fragments,LineNo,FEW);      // Make sure ECL source code gets constructed in the correct order.

        RETURN MODULE
            EXPORT Name := %'FSM'%;
            EXPORT Text := ConcatenateStringFields(SortedFragments,txt,'');
        END;
    END;

    /*
      Given a Finite State Machine (FSM) of type 'WUStateMachineStructure' and a target 'JobNumber',
      this FUNCTION will start a WU off using the details held in said FSM from this JobNumber, passing to the 
      child WU the NextJobNumber in the machine and its parents WUId. This allows for communication of information
      from parent to child WU's in the chain.

      Requirements of any FUNCTION attribute used by this harness:
          1.  It must be a FUNCTION
          2.  It must take a parameter of TYPEOF(WUStateMachineStructure.JobNumber) indicated by the place holder <NextJob> or <state>
              in the target ecl.
          3.  It must return TYPEOF(WUStateMachineStructure.JobNumber) usually populated by the input 'NextJob' parameter (this being the
              next state to call in the machine.) 
              However this is not mandatory, the return state can cause the machine to call any other target FUNCTION in the machine by
              just returning the 'JobNumber' for that FUNCTION.
              If can also return 0 which terminates the sequence of workunits.
          4.  To pass the parent WUID to the target FUNCTION use '<parentWUid>' as a place holder in the parameter list of the
              target FUNCTION. Its type being STRING.
          5.  There is also the option to supply a target FUNCTION with the FSM used to kick it off.
              Use '<fsm>' as a place holder for a parameter of type DATASET(WUStateMachineStructure) to the target FUNCTION.
              This allows the target FUNCTION to use an existing FSM in any new chains of work it wants to initiate.
              (Say possibly on another cluster.)

       Example target FUNCTION:
            F(UNSIGNED2 JobNumber) := FUNCTION
              Action := DoSomeWork;
              RETURN WHEN(JobNumber,Action);
            END;

       Example showing use of <parentWUid>
            F(UNSIGNED2 NextJob,STRING parentWUID) := FUNCTION
              UNSIGNED4 PreviousRunDate := (UNSIGNED4) DATASET(WORKUNIT(parentWUID, 'BUILD_DATE'), {STRING date})[1].date;
              RunDate := STD.Date.AdjustDate(PreviousRunDate,0,1,0);
              Action := OUTPUT((STRING) RunDate,NAMED('BUILD_DATE'));
              TerminateionCondition := RunDate > STD.Date.Today();
              NextState := IF(TerminateionCondition,0,NextJob);    // Terminate when rundate is in the future
              RETURN WHEN(NextState,Action);
            END;

       Example making use of the FSM used by the initiator:
            ViewAndUseFSM(UNSIGNED2 NextJob,DATASET(#EXPAND(HOME_FOLDER).WorkUnitManagement.WUStateMachineStructure) FSM) := FUNCTION
            CopyOfFSM := #EXPAND(HOME_FOLDER).WorkUnitManagement.CopyTextOfFSM(FSM);
            NewWUStream := '#WORKUNIT(\'name\',\'Start new WU chain on different queue, using FSM supplied by parent WU.\');\n'
                          + CopyOfFSM.text
                          + '\n'                                            // '2' being the start state for the new WU sequence. 
                          + HOME_FOLDER+'.WorkUnitManagement.DaisyChain(2,WORKUNIT,'+CopyOfFSM.name+');';
            NewWUStream := EVALUATE(#EXPAND(HOME_FOLDER).WorkUnitManagement.fSubmitNewWorkunit(NewWUStream));
            Action := ORDERED(OUTPUT(FSM,NAMED('FSM'))
                             ,NewWUStream);
            RETURN WHEN(0,Action);    // Returning 0 closes down the current WU chain.
       END;
    */

    EXPORT STRING DaisyChain(TYPEOF(WUStateMachineStructure.JobNumber) TargetJobNumber,STRING ParentWUid,DATASET(WUStateMachineStructure) FSM,STRING pESPURL = ESP_service_IP_and_PORT,STRING pCluster = CLUSTER,STRING pQueue = QUEUE) := FUNCTION

        CopyOfFSM := CopyTextOfFSM(FSM);

        // The only way we can get a input state of zero is if the application has passed back zero.
        // Add an internal '0' ROW to the FSM to fix up the DICTIONARY lookup on such a input state.
        nFSM := FSM + ROW({0,0,'WorkUnit Chain Completed by Application, Spawned from '+ParentWUid,'OUTPUT(\''+ParentWUid+'\',NAMED(\'WorkUnit_Chain_Completed_by_Application\'))'},WUStateMachineStructure);
        Task := DICTIONARY(nFSM,{JobNumber => NextJobNumber, WUName, eclFUNCTION})[TargetJobNumber];

        // Pass the 'NextJob' to the target action, usually the target action just echo's this back.
        // But there is the option for it to change the next Job, e.g. returning zero prematurely terminates the machine.
        // Ditto for the parent WUid, so there is a communication channel down through the chain of WU's.
        // Ditto for the Parent FSM, so target FUCNTIONs have exposure to the FSM being used in their context.
        STRING InsertNextJobNum(STRING itm) := REGEXREPLACE('<state>|<NextJob>',itm,(STRING)Task.NextJobNumber,NOCASE);
        STRING InsertParentWUid(STRING itm) := REGEXREPLACE('<parentWUid>',itm,ParentWUid,NOCASE);
        STRING InsertRefToFSM(STRING itm)   := REGEXREPLACE('<fsm>',itm,CopyOfFSM.Name,NOCASE);

        NextEcl := REGEXREPLACE(';{0,}$',InsertRefToFSM(InsertParentWUid(InsertNextJobNum(Task.eclFUNCTION))),'');

        RunECL(STRING eclStr) := FUNCTION
            STRING Str := '#WORKUNIT(\'name\',\''+InsertParentWUid(InsertNextJobNum(EscapeQuotes(Task.WUName)))+'\');\n' + eclStr + ';\n';
            RETURN fSubmitNewWorkunit(Str,pESPURL,pCluster,pQueue);
        END;

        // Rather than displaying potentially sensitive information held in URL's etc in the ECL source
        // of every workunit, allow FUNCTION 'DaisyChain' to use its defaults, if at all possible.
   
        OtherParameters :=   IF(pESPURL = ESP_service_IP_and_PORT,'',',pESPURL:=\''+pESPURL+'\'')
                           + IF(pCluster = CLUSTER               ,'',',pCluster:=\''+pCluster+'\'')
                           + IF(pQueue = QUEUE                   ,'',',pQueue:=\''+pQueue+'\'');

        NextTask  :=  CopyOfFSM.Text
                    + '\n'
                    + IF(Task.NextJobNumber > 0
                        ,'\n'+HOME_FOLDER+'.WorkUnitManagement.DaisyChain('+NextEcl+',WORKUNIT,'+CopyOfFSM.Name+OtherParameters+')'
                        ,NextEcl);
        RETURN RunECL(NextTask);
    END;

    /*
      Given a list of tasks to run as individual workunits, return a Finite State Machine
      that can be passed as input to FUNCTION 'DaisyChain' above.
  
      Inputs:
        idFSM     : The name of the attribute to be defined.
        JobsToRun : DATASET(JobDefinitionLayout) where empty rows indicate a new sequence of workunits follow.
                    These will run in parallel with all other sequences.

      e.g.  (note that '<NextJob>' is a mandatory parameter to every target FUNCTION run by 'DaisyChain'.)
 
          DATASET([{'Workunit 1 of task sequence 1','DoJob(1,1,<NextJob>)'}
                  ,{'Workunit 2 of task sequence 1','DoJob(2,1,<NextJob>)'}
                  ,{'Workunit 3 of task sequence 1','DoJob(3,1,<NextJob>)'}
                  ,{'',''}
                  ,{'Workunit 1 of task sequence 2','DoJob(1,2,<NextJob>)'}
                  ,{'',''}
                  ,{'Workunit 1 of task sequence 3','DoJob(1,3,<NextJob>)'}
                  ,{'Workunit 2 of task sequence 3','DoJob(2,3,<NextJob>)'}],JobDefinitionLayout);

        Note this MACRO Can only be used in straight forward cases, where all sequences are distinct.
        i.e. One workunit sequence does NOT jump to use all, or part of, another sequence.
        If you have a complex state machine, i.e. a complex sequence of workunit calls you'll have to 
        construct your own DATASET(WUStateMachineStructure).
    */
    EXPORT DefineWorkFlowPrimitive(idFSM,JobsToRun) := MACRO

        #UNIQUENAME(BasicStateMachine)
        %BasicStateMachine% := PROJECT(JobsToRun,TRANSFORM(#EXPAND(HOME_FOLDER).WorkUnitManagement.WUStateMachineStructure;
                                                           SELF.JobNumber     := COUNTER;
                                                           SELF.NextJobNumber := COUNTER+1;
                                                           SELF := LEFT))(eclFUNCTION != '');
        
        // Set 'nextState' to 0 where its not used in the 'state' field.
        #UNIQUENAME(ZeroisedNextStates)
        %ZeroisedNextStates% := PROJECT(%BasicStateMachine%(NextJobNumber NOT IN SET(%BasicStateMachine%,JobNumber))
                                       ,TRANSFORM(#EXPAND(HOME_FOLDER).WorkUnitManagement.WUStateMachineStructure;SELF.NextJobNumber := 0;SELF := LEFT));
        
        idFSM := SORT(PROJECT(%BasicStateMachine%(NextJobNumber IN SET(%BasicStateMachine%,JobNumber)) + %ZeroisedNextStates%
                             ,TRANSFORM(#EXPAND(HOME_FOLDER).WorkUnitManagement.WUStateMachineStructure;SELF := LEFT))
                     ,JobNumber);
    ENDMACRO;

    /*
      Second form of FSM constructor.
  
      Inputs:
        idFSM     : The name of the attribute to be defined.
        JobsToRun : DATASET(ParallelJobDefinitionLayout) 
                    A collection of child JobDefinitionLayout DATASETS. Each Child dataset being one list of
                    workunits that can run in PARALLEL with all other child datasets.

      DATASET([{DATASET([{'Job 1 on Queue 1','ecl 1/1'},{'Job 2 on Queue 1','ecl 2/1'},{'Job 3 on Queue 1','ecl 3/1'}
                        ,{'Job 4 on Queue 1','ecl 4/1'},{'Job 5 on Queue 1','ecl 5/1'},{'Job 6 on Queue 1','ecl 6/1'}
                        ],#EXPAND(HOME_FOLDER).WorkUnitManagement.JobDefinitionLayout)}
              ,{DATASET([{'Job 1 on Queue 2','ecl 1/2'}],#EXPAND(HOME_FOLDER).WorkUnitManagement.JobDefinitionLayout)}
              ,{DATASET([{'Job 1 on Queue 3','ecl 1/3'},{'Job 2 on Queue 3','ecl 2/3'},{'Job 3 on Queue 3','ecl 3/3'}
                        ,{'Job 4 on Queue 3','ecl 4/3'},{'Job 5 on Queue 3','ecl 5/3'},{'Job 6 on Queue 3','ecl 6/3'}
                        ,{'Job 7 on Queue 3','ecl 7/3'},{'Job 8 on Queue 3','ecl 8/3'},{'Job 9 on Queue 3','ecl 9/3'}
                       ],#EXPAND(HOME_FOLDER).WorkUnitManagement.JobDefinitionLayout)}
              ],#EXPAND(HOME_FOLDER).WorkUnitManagement.ParallelJobDefinitionLayout);

        Note this MACRO Can only be used in straight forward cases, where all sequences are distinct.
        i.e. One workunit sequence does NOT jump to use all, or part of, another sequence.
        If you have a complex state machine, i.e. a complex sequence of workunit call's you'll have to 
        construct your own DATASET(WUStateMachineStructure).
    */

    EXPORT DefineWorkFlow(idFSM,JobsToRun) := MACRO

        #UNIQUENAME(Flatten)
        // Note for 'NORMALIZE' COUNTER is always 1 for the 1st of every new child dataset.
        {DATASET(#EXPAND(HOME_FOLDER).WorkUnitManagement.JobDefinitionLayout) intermediate}
          %Flatten%(#EXPAND(HOME_FOLDER).WorkUnitManagement.JobDefinitionLayout L,UNSIGNED Cnt) := TRANSFORM
            seperator := ROW({'',''},#EXPAND(HOME_FOLDER).WorkUnitManagement.JobDefinitionLayout);
            ATask     := DATASET([{L.WUName,L.eclFUNCTION}],#EXPAND(HOME_FOLDER).WorkUnitManagement.JobDefinitionLayout);
            SELF.intermediate := IF(cnt = 1
                                   ,seperator & ATask
                                   ,ATask);
        END;
        #UNIQUENAME(jtor)
        %jtor% := NORMALIZE(NORMALIZE(JobsToRun,LEFT.Queue,%Flatten%(RIGHT,COUNTER)),LEFT.intermediate,TRANSFORM(RIGHT));

        #EXPAND(HOME_FOLDER).WorkUnitManagement.DefineWorkFlowPrimitive(idFSM,%jtor%);

    ENDMACRO;

    /*
      From a DaisyChain FSM (DATASET(WUStateMachineStructure)) this defines 'Action' as the string of ECL
      that can be used to start the first of the workunits in all parallel sequences.
      Optional 3rd parameter to allow a 'name' to be given to the initiating WU.

      Note this MACRO only generates code that uses the default settings:

         ESP service
         Cluster
         Queue

      defined at the start of the this file.
    */
    EXPORT DefineWorkFlowInitiatorECL(Action,FSM,WUName = '') := MACRO

        // Construct the supplied FSM as an ECL attribute
        #UNIQUENAME(OL);
        %OL% := {UNSIGNED LineNo;STRING Oneline};   // USe LineNo to ensure the ECL source gets genberated in the correct order.

        #UNIQUENAME(CopyOfFSM);
        %CopyOfFSM% := #EXPAND(HOME_FOLDER).WorkUnitManagement.CopyTextOfFSM(FSM);
        
        // Generate ECL that will start all the 1st WU's in every chain in PARALLEL.
        #UNIQUENAME(StartWUs);
        %StartWUs% := FSM(JobNumber not in set(FSM, NextJobNumber)); 

        #UNIQUENAME(EnvelopeECL);
        %EnvelopeECL%(UNSIGNED Lin,STRING typ) := DATASET([{Lin,typ}],%OL%);
        #UNIQUENAME(InstantiateTask);
        STRING %InstantiateTask%(TYPEOF(#EXPAND(HOME_FOLDER).WorkUnitManagement.WUStateMachineStructure.JobNumber) JobNumber)
                := HOME_FOLDER+'.WorkUnitManagement.DaisyChain('+(STRING)JobNumber+',WORKUNIT,'+%CopyOfFSM%.Name+')';
        #UNIQUENAME(nameWU)
        %nameWU% := %EnvelopeECL%(1,IF(#TEXT(WUName) <> '','#WORKUNIT(\'name\','+#TEXT(WUName)+');',''));
        #UNIQUENAME(eclOfFSM)
        %eclOfFSM% := %EnvelopeECL%(2,%CopyOfFSM%.Text);
        #UNIQUENAME(Components);
        %Components% := SORT(  %nameWU%
                             & %eclOfFSM%
                             & IF(COUNT(%StartWUs%) = 1
                                 ,  %EnvelopeECL%(3,%InstantiateTask%(%StartWUs%[1].JobNumber) + ';')
                                 ,  %EnvelopeECL%(3,'PARALLEL(')
                             & PROJECT(%StartWUs%
                                      ,TRANSFORM(%OL%; SELF.LineNo := COUNTER+COUNT(%eclOfFSM%)+3;SELF.OneLine := IF(COUNTER = 1,' ',',') + %InstantiateTask%(LEFT.JobNumber)))
                             & %EnvelopeECL%(10000000,');'))
                            ,LineNo,FEW);

        Action := #EXPAND(HOME_FOLDER).WorkUnitManagement.ConcatenateStringFields(%Components%,OneLine,'\n')

    ENDMACRO;

    /////////////////////////////////////////////////////////////////////////////////
    // Some Example target applications
    /////////////////////////////////////////////////////////////////////////////////

    EXPORT UNSIGNED2 Delay(UNSIGNED2 State,UNSIGNED Minutes)    := WHEN(State,STD.System.Debug.Sleep(Minutes*60*1000));
    EXPORT UNSIGNED2 Out(UNSIGNED2 State,STRING mess)           := WHEN(State,OUTPUT(mess));
    EXPORT UNSIGNED2 BuildKeys(UNSIGNED2 State,STRING PeriodType,STRING parentWUID) := FUNCTION
        UNSIGNED4 PreviousRunDate := (UNSIGNED4) DATASET(WORKUNIT(parentWUID, 'BUILD_DATE'), {STRING date})[1].date;
        RunDate := MAP(PeriodType = 'year'  => STD.Date.AdjustDate(PreviousRunDate,1,0,0)
                      ,PeriodType = 'month' => STD.Date.AdjustDate(PreviousRunDate,0,1,0)
                      ,                        STD.Date.AdjustDate(PreviousRunDate,0,0,1));
        Action := OUTPUT((STRING) RunDate,NAMED('BUILD_DATE'));
        TerminateionCondition := (RunDate > STD.Date.Today()) OR (PeriodType = 'day' AND RunDate = 20170612);
        NextState := IF(TerminateionCondition,0,State);    // Terminate when rundate is in the future, and don't do too many 'day' builds
        RETURN WHEN(NextState,Action);
    END;
    EXPORT UNSIGNED2 StartWatcher(UNSIGNED2 NextJobNumber,STRING MailList) := FUNCTION

      IMPORT lib_system;
 
      STD.System.Email.SendEmail( MailList
                                ,'WorkUnitManagement Demonstration \'Watcher\' Has Fired.'
                                ,IF(EVENTEXTRA('Result') = 'SUCCESS'
                                     ,'Exercised the detection of a successful completion of the watched WU.'
                                     ,'Exercised the detection of a crash of the watched WU.')
                                ,  lib_system.smtpserver
                                ,  lib_system.smtpport
                                ,  lib_system.emailAddress
                              ) : WHEN(EVENT('WorkUnitManagement','*'),COUNT(1));

      RETURN WHEN(NextJobNumber,OUTPUT('WATCHER_STARTED'));
    END;

    TellWorld(STRING param) := NOTIFY(EVENT('WorkUnitManagement','<Event><Result>'+Param+'</Result></Event>'));
    
    EXPORT UNSIGNED2 WatchedWU (UNSIGNED2 NextJobNumber,STRING param) := FUNCTION
      Action := ASSERT(param = 'SUCCESS','Exercising Watching WUs.',FAIL);
      RETURN WHEN(NextJobNumber,Action);
    END : FAILURE(TellWorld('FAILURE'))
        , SUCCESS(TellWorld('SUCCESS'))
      ;

END; // END of MODULE WorkUnitManagement









/////////////////////////////////////////////////////////////////////////////////
// First Demonstration of the harness.
// Cut and paste into a 'Builder' window and set the STRING HOME_FOLDER to the
// name of the folder that holds the WorkUnitManagement MODULE.
/////////////////////////////////////////////////////////////////////////////////
STRING HOME_FOLDER := '';
// Define the Machine

JDL  := #EXPAND(HOME_FOLDER).WorkUnitManagement.JobDefinitionLayout;
DATASET(JDL) AddDetails(DATASET(JDL) ds) := PROJECT(ds,TRANSFORM(JDL;SELF.WUName := LEFT.WUName + ' Spawned from <ParentWUID>';SELF := LEFT));
    

Queue1 := DATASET([{'Queue 1 Workunit 1',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 1 Workunit 1\')'}
                  ,{'Queue 1 Workunit 2',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 1 Workunit 2\')'}
                  ,{'Queue 1 Workunit 3',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 1 Workunit 3\')'}
                  ,{'Queue 1 Workunit 4',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 1 Workunit 4\')'}],JDL);
Queue2 := DATASET([{'Queue 2 Delay 1 minute',     HOME_FOLDER+'.WorkUnitManagement.Delay(<state>,1)'}
                  ,{'Queue 2 Workunit 2',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 2 Workunit 2\')'}
                  ,{'Queue 2 Workunit 3',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 2 Workunit 3\')'}],JDL);
Queue3 := DATASET([{'Queue 3 Delay 2 minutes',    HOME_FOLDER+'.WorkUnitManagement.Delay(<state>,2)'}
                  ,{'Queue 3 Workunit 2',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 3 Workunit 2\')'}
                  ,{'Queue 3 Workunit 3',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 3 Workunit 3\')'}
                  ,{'Queue 3 Workunit 4',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 3 Workunit 4\')'}
                  ,{'Queue 3 Workunit 5',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 3 Workunit 5\')'}
                  ,{'Queue 3 Workunit 6',         HOME_FOLDER+'.WorkUnitManagement.Out(<state>,\'Queue 3 Workunit 6\')'}],JDL);
WUSequenceDefinition :=  DATASET([{AddDetails(Queue1)}
                                 ,{AddDetails(Queue2)}
                                 ,{AddDetails(Queue3)}],#EXPAND(HOME_FOLDER).WorkUnitManagement.ParallelJobDefinitionLayout);

// Start the machine running.

#EXPAND(HOME_FOLDER).WorkUnitManagement.DefineWorkFlow(FSM,WUSequenceDefinition);
#EXPAND(HOME_FOLDER).WorkUnitManagement.DefineWorkFlowInitiatorECL(Action,FSM,'Initiate Workunit sequence Demonstration 1.');
#EXPAND(HOME_FOLDER).WorkUnitManagement.fSubmitNewWorkunit(Action);











/////////////////////////////////////////////////////////////////////////////////
// Second Demonstration of the harness.
// Cut and paste into a 'Builder' window and set the STRING HOME_FOLDER to the
// name of the folder that holds the WorkUnitManagement MODULE.
// This demonstrates the use of <parentWUID> in passing a parameter from one workunit to another,
// and where it's the responsibility of the application to terminate the sequence of execution.
// In this example the scenario is a sequence of retro key builds all for rundates one month apart,
// up to, but not exceeding, the current date.
// So the parameter is a 'Rundate' and apart from performing the actual key build the application must either:
//   1.   Detect that the build its performed is the last required and terminate the sequence of builds
//   2.   Increment the rundate and make accessible to its successor workunit (via a named OUTPUT)
/////////////////////////////////////////////////////////////////////////////////
STRING HOME_FOLDER := '';
#WORKUNIT('name','Initiate Workunit sequence Demonstration 2.');
FSM := DATASET([{1,  1,'BuildKeys Spawned from <ParentWUID>', HOME_FOLDER+'.WorkUnitManagement.BuildKeys(<state>,\'month\',\'<parentWUID>\');'}],#EXPAND(HOME_FOLDER).WorkUnitManagement.WUStateMachineStructure);
WHEN(#EXPAND(HOME_FOLDER).WorkUnitManagement.DaisyChain(1,WORKUNIT,FSM),OUTPUT('20170604',NAMED('BUILD_DATE')));











/////////////////////////////////////////////////////////////////////////////////
// Third Demonstration of the harness.
// Cut and paste into a 'Builder' window and set the STRING HOME_FOLDER to the
// name of the folder that holds the WorkUnitManagement MODULE.
// Similar to the 2nd demonstration above, but carry out annual, month and day
// retro key file builds in parallel.
/////////////////////////////////////////////////////////////////////////////////
STRING HOME_FOLDER := '';
#WORKUNIT('name','Initiate Workunit sequence Demonstration 3.');
FSM := DATASET([{1,  1,'Year BuildKeys Spawned from <ParentWUID>',  HOME_FOLDER+'.WorkUnitManagement.BuildKeys(<state>,\'year\', \'<parentWUID>\');'}
               ,{2,  2,'Month BuildKeys Spawned from <ParentWUID>', HOME_FOLDER+'.WorkUnitManagement.BuildKeys(<state>,\'month\',\'<parentWUID>\');'}
               ,{3,  3,'Day BuildKeys Spawned from <ParentWUID>',   HOME_FOLDER+'.WorkUnitManagement.BuildKeys(<state>,\'day\',  \'<parentWUID>\');'}
               ],#EXPAND(HOME_FOLDER).WorkUnitManagement.WUStateMachineStructure);
MultiBuild := PARALLEL( #EXPAND(HOME_FOLDER).WorkUnitManagement.DaisyChain(1,WORKUNIT,FSM)
                       ,#EXPAND(HOME_FOLDER).WorkUnitManagement.DaisyChain(2,WORKUNIT,FSM)
                       ,#EXPAND(HOME_FOLDER).WorkUnitManagement.DaisyChain(3,WORKUNIT,FSM));
WHEN(MultiBuild,OUTPUT('20170604',NAMED('BUILD_DATE')));










/////////////////////////////////////////////////////////////////////////////////
// Forth Demonstration of the harness.
// Cut and paste into a 'Builder' window and set the STRING HOME_FOLDER to the
// name of the folder that holds the WorkUnitManagement MODULE.
// In this scenario a 'Watcher' workunit is started that will complete and email out
// the success or failure of the spawned 2nd workunit in the sequence.
// In this run the 2nd, watched, workunit succeeds.
// Obviously setup EMAIL_RECIPIENT to an appropriate email address.
/////////////////////////////////////////////////////////////////////////////////
STRING HOME_FOLDER := '';
STRING EMAIL_RECIPIENT := '';
FSM := DATASET([{1,2,'Start Watcher',             HOME_FOLDER+'.WorkUnitManagement.StartWatcher(<NextJob>,\''+EMAIL_RECIPIENT+'\');'}
               ,{2,0,'Run a successfull workunit',HOME_FOLDER+'.WorkUnitManagement.WatchedWU(<state>,\'SUCCESS\');'}
                   ],#EXPAND(HOME_FOLDER).WorkUnitManagement.WUStateMachineStructure);
#EXPAND(HOME_FOLDER).WorkUnitManagement.DaisyChain(1,WORKUNIT,FSM);














/////////////////////////////////////////////////////////////////////////////////
// Fifth Demonstration of the harness.
// Cut and paste into a 'Builder' window and set the STRING HOME_FOLDER to the
// name of the folder that holds the WorkUnitManagement MODULE.
// In this scenario a 'Watcher' workunit is started that will complete and email out
// the success or failure of the spawned 2nd workunit in the sequence.
// In this run the 2nd, watched, workunit fails.
// Obviously setup EMAIL_RECIPIENT to an appropriate email address.
/////////////////////////////////////////////////////////////////////////////////
STRING HOME_FOLDER := '';
STRING EMAIL_RECIPIENT := '';
FSM := DATASET([{1,2,'Start Watcher',         HOME_FOLDER+'.WorkUnitManagement.StartWatcher(<NextJob>,\''+EMAIL_RECIPIENT+'\');'}
               ,{2,0,'Run a failing workunit',HOME_FOLDER+'.WorkUnitManagement.WatchedWU(<state>,\'FAILURE\');'}
                   ],#EXPAND(HOME_FOLDER).WorkUnitManagement.WUStateMachineStructure);
#EXPAND(HOME_FOLDER).WorkUnitManagement.DaisyChain(1,WORKUNIT,FSM);
