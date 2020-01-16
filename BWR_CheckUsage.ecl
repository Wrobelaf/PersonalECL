IMPORT Parminder_ECLTraining;
tgtAttrib := 'Control';

#workunit('name', 'Syntax check \''+tgtAttrib+'\' in repository (pre removal)');

		C_(d) := MACRO
				OUTPUT(COUNT(d), NAMED('Count_of_'+ #TEXT(d)));
		ENDMACRO;

		O(d) := MACRO
				OUTPUT(d, NAMED(#TEXT(d)));
		ENDMACRO;

		OA(d) := MACRO
        C_(d);
				OUTPUT(d, NAMED(#TEXT(d)), ALL);
		ENDMACRO;

		OC(d, _nn = 1000) := MACRO
				C_(d) ;
				OUTPUT(CHOOSEN(d, _nn), NAMED(#TEXT(d)));
		ENDMACRO;

b := Parminder_ECLTraining.SyntaxChecker('10.193.64.21:9145', '10.193.64.21:9010', 'thordev3').UsageOf(tgtAttrib);

Source_list := PROJECT(b.FoundIn,TRANSFORM(Parminder_ECLTraining.SyntaxCheckerLayouts.AttributeRec; SELF := LEFT));    // Remove the ECL

OC(Source_list);

Results0  := b.Results;

RR := RECORD
    TYPEOF(Results0.Severity) Severity := Results0.Severity;
    UNSIGNED Cnt := COUNT(GROUP);
END;

SORT(TABLE(Results0,RR,Severity),Severity);

Results := DEDUP(SORT(Results0,Filename,Severity),Filename,Severity);
OA(Results);


