#workunit('name','afw test regex');

RIN := RECORD
    STRING test;
		BOOLEAN expected;
END;

ROUT := RECORD
 RIN;
 BOOLEAN result;
END;

ind := DATASET([{'11111-345444',true},{'11211-345444',false},{'11111-111111',true},{'12345-999999',false},{'',false},{'1',false}],RIN);

ROUT doTest(RIN L) := TRANSFORM
		SELF.result := REGEXFIND('^(.{1})\\1+($|-{1}.*$)',L.test);
    SELF:= L;
END;

res  := PROJECT(ind,doTest(LEFT));
diff := res(expected != result);
OUTPUT(diff,ALL);
