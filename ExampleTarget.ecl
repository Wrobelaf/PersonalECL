EXPORT ExampleTarget := MODULE
    IMPORT STD;
    /////////////////////////////////////////////////////////////////////////////////
    // Some Example target applications
    /////////////////////////////////////////////////////////////////////////////////
    
    EXPORT UNSIGNED2 Delay(UNSIGNED2 State,UNSIGNED Minutes)    := WHEN(State,STD.System.Debug.Sleep(Minutes*60*1000));
    EXPORT UNSIGNED2 Out(UNSIGNED2 State,STRING mess)           := WHEN(State,OUTPUT(mess));

END; // END of MODULE ExampleTarget


