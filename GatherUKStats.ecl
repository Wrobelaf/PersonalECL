IMPORT * from STD;
IMPORT * FROM insuranceUK_iesp.ws_insurance_uk;
IMPORT * FROM ProjectUK_Deltas;

rundate:='2013-01-07';

#workunit('name','Gather Ageas Score Values - '+rundate)

Account_No:= '1000001';
Ageas_abi:='248';

tl := DISTRIBUTE(delta_files.DS_BASE_DELTA_TRANSLOG(account_number = Account_No AND REGEXFIND('^.*X?'+Ageas_abi+'X?.*$',Str.FindReplace(abi_request_list,'|','X'))),HASH32(transaction_id)): PERSIST ('~Ageas::tl');
il := DISTRIBUTE(delta_files.DS_BASE_DELTA_INTLOG(content_type = 'UKResponse'),HASH32(transaction_id)) : PERSIST ('~Ageas::il');
il2:= il(date_added[1..10] = rundate );

RVals := RECORD
    STRING Value;
END;

R := RECORD
    STRING20 transaction_id;
    DATASET(RVals) Score_Value;
END;

R GatherInfo(LayoutDeltaExports.Int_Log_Rec R) := TRANSFORM

    ws_insurance_uk.t_InsQuoteScoresResponse rsp := FROMXML(ws_insurance_uk.t_InsQuoteScoresResponse,'<Row>'+R.Content_data+'</Row>');
    SELF.transaction_id := R.transaction_id;
    
    RVals GatherValues(ws_insurance_uk.t_UKInsScore L) := TRANSFORM
        SELF.Value := L.Value;
    END;
    SELF.Score_Value := PROJECT(rsp.InsurerResults.SubjectsResults.Scores(ScoreId = 'MSRA01_1'),GatherValues(LEFT));

END;

fn := '~AgeasRetail::ScoreValues::'+rundate;
op := OUTPUT(JOIN(tl,il2,LEFT.transaction_id = RIGHT.transaction_id,GatherInfo(RIGHT),LOCAL),,fn,CSV(SEPARATOR('|'),TERMINATOR('\n')),OVERWRITE);
Despray := STD.File.DeSpray(fn,'10.222.20.29','/data/HPCCSystems/dropzone/Ageas_ScoreValues_'+rundate+'.txt',,,,TRUE);
SEQUENTIAL(op,Despray);
