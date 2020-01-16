IMPORT * FROM STD;
IMPORT * FROM Projectuk_deltas;
IMPORT * FROM UKServices_V7;
IMPORT * FROM UKServices_V7.LoggingConstants;
IMPORT InsuranceUK_iesp.ws_insurance_uk;
IMPORT UKServices_Utilities;

EXPORT IL_Miner(INTEGER pDaysBack) := MODULE

    SHARED DaysBack := -ABS(pDaysBack);
	  SHARED FNRoot   := '~ilminer::il::';
		SHARED Decompress := UKServices_Utilities.MySqlZlib.Decompress;
		SHARED MakeSFName(STRING id) := STD.Str.FindReplace(FNRoot+'XXX_key','XXX',id);		// A SF name from a key name.

    SHARED KID := ENUM(UNSIGNED1,ACCOUNT=1
                                ,ABI
                                ,SCORE
                                ,DATE         
                                ,TIME         
                                ,TID          
                                ,ORDERSTATUS  
                                ,REF          
                                ,BROKER       
                                ,VRN          
                                ,TITLE        
                                ,FIRSTNAME    
                                ,MIDDLENAME   
                                ,SURNAME      
                                ,DOB          
                                ,POSTCODE     
                                ,CONTENTTYPE  
                                ,ADD1         
                                ,ADD2         
                                ,VALUE        
                                ,VRM );         
    
    SHARED keys := DATASET([{'account',    'STRING12'}    // 1
													 ,{'abi',        'STRING6'}     // 2
													 ,{'score',      'STRING100'}   // 3
													 ,{'date',       'STRING8' }    // 4
													 ,{'time',       'STRING8' }    // 5
													 ,{'tid',        'STRING20'}    // 6
													 ,{'orderstatus','STRING3'}     // 7
                           ,{'ref',        'STRING20'}    // 8
                           ,{'broker',     'STRING6'}     // 9
                           ,{'vrn',        'STRING7'}     // 10
                           ,{'title',      'STRING6'}     // 11
                           ,{'firstname',  'STRING50'}    // 12
                           ,{'middlename', 'STRING50'}    // 13
                           ,{'surname',    'STRING50'}    // 14
                           ,{'dob',        'STRING8'}     // 15
                           ,{'postcode',   'STRING8'}     // 16
                           ,{'contenttype','STRING21'}    // 17
                           ,{'add1',       'STRING100'}   // 18
                           ,{'add2',       'STRING100'}   // 19
                           ,{'value',      'STRING100'}   // 20
                           ,{'vrm',        'STRING7'}     // 21
                           ],{STRING keyname,STRING keytype});
    
    SHARED Clean(STRING itm)    := STD.Str.CleanSpaces(STD.Str.ToUpperCase(itm));    // Needed on query side of processing.
    SHARED Collapse(STRING itm) := TRIM(STD.Str.ToUpperCase(itm),ALL);
    SHARED CleanAndRemove(STRING itm) := Clean(REGEXREPLACE('[\'"{}\\[\\]]',itm,''));   // For names and addesses O'Connar => OConner
		SHARED EscapeChar(STRING itm,STRING1 char) := REGEXREPLACE('\\'+char,itm,'\\\\\\\\'+char);
		SHARED EscapeChars(STRING itm) := EscapeChar(EscapeChar(Clean(itm),'{'),'}');								// '{'  => '\\{' on the query to generate EECL REGEX 

		EXPORT Results := RECORD
				Projectuk_deltas.LayoutDeltaExports.Transaction_log;
				Projectuk_deltas.LayoutDeltaExports.Int_Log_Rec AND NOT [transaction_id
																																,product_id
																																,date_added
																																,is_compressed
																																,content_data];
				IFBLOCK(SELF.content_type = UKResponse)
						  ws_insurance_uk.t_InsQuoteScoresResponse response;
				END;
				IFBLOCK(SELF.content_type = Request)
							ws_insurance_uk.t_InsQuoteScoresRequest  request;
				END;
				IFBLOCK(SELF.content_type = CCResponseMessage)
							UKLayouts.Layout_CC_Results    CCresponse;
				END;
				IFBLOCK(SELF.content_type = ExperianResponse)
							UKLayouts.Layout_Experian_Results  Experianresponse;
				END;
				IFBLOCK(SELF.content_type = MapViewPerilsResponse)
							UKLayouts.Layout_MapViewPerils_Results MapViewresponse;
				END;
				IFBLOCK(SELF.content_type = MapViewAccumResponse)
							UKLayouts.Layout_MapViewAccumulations_Results  MapViewAccresponse;
				END;
				IFBLOCK(SELF.content_type = CarWebResponse)
							UKLayouts.Layout_CW_Results    CarWebresponse;
				END;
				IFBLOCK(SELF.content_type = CUEResponse)
							UKLayouts.Layout_CUE_Results   CUEresponse;
				END;
				IFBLOCK(SELF.content_type = COOPResponse)
							UKLayouts.Layout_CoOpMembership_Results    COOPresponse;
				END;
				IFBLOCK(SELF.content_type = NCDResponse)
							UKLayouts.Layout_NCD_Results   NCDresponse;
				END;
				IFBLOCK(SELF.content_type = CallIDSearchResponse)
							UKLayouts.Layout_CallIDSearch_Results  CallIdresponse;
				END;
		END;

    EXPORT Size_IL_Keys() := FUNCTION
		    d := NOTHOR(STD.File.LogicalFileList(FNRoot[2..]+'*'));
		    RETURN MODULE
				    EXPORT FileList := d;
						EXPORT Size     := SUM(d,size); 
				END;
    END;

    EXPORT Purge_IL_Keys() := FUNCTION
        CleanOut(STRING name) := FUNCTION
            RETURN IF(STD.File.SuperFileExists(FNRoot+name+'_key')
                     ,SEQUENTIAL(STD.File.StartSuperFileTransaction()
                                ,STD.File.ClearSuperFile(FNRoot+name+'_key',TRUE)
                                ,STD.File.FinishSuperFileTransaction())
                     );
        END;
        RETURN NOTHOR(APPLY(Keys,CleanOut(keyname)));
    END;
    
		EXPORT Create_IL_Keys(STRING8 Rundate) := FUNCTION

			  // Call using:
				// #workunit('name','Test IL Key build(No Payload)');
				// Create_IL_Key('20160218');

				ASSERT(REGEXFIND('20[0-9]{2}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01]).*',Rundate),'Rundate in build of IL index invalid: '+Rundate,FAIL);

        STRING MakeLFName(TYPEOF(KID) id) := FNRoot+Rundate+ '::'+keys[id].keyname+'_key';   // Make a logical filename from an ID.

				d0  := dfm_Files.DS_BASE_RECENT_INTLOG_PLUS_FP(DaysBack) : INDEPENDENT;
        d   := d0(content_type = UKResponse);
        dr  := d0(content_type = Request);
        dcc := d0(content_type = CCResponseMessage);
				dex := d0(content_type = ExperianResponse);
				dmv := d0(content_type = MapViewPerilsResponse);
				dcu := d0(content_type = CUEResponse);
				dci := d0(content_type = CallIDSearchResponse);
				dca := d0(content_type = CarWebResponse);
        dth := d0(content_type IN [ MapViewAccumResponse
                                  , COOPResponse
                                  , NCDResponse 
                                  ]);
        Link := RECORD
        	 STRING20  tid;
					 UNSIGNED8 RecPos;
        END;
 
				RABI := RECORD
						STRING6 Abi;
            STRING6 Broker
				END;

				RScore := RECORD
						STRING100 Score;
				END;

 				Rvrn := RECORD
					 STRING7  vrn;
				END;
        
        Rpii := RECORD
           STRING6  title;
           STRING50 firstname;
           STRING50 middlename;
           STRING50 surname;
           STRING8  dob;
        END;

        Radd1 := RECORD
            STRING100 add1;      // Make field name the same name as the key file.
        END;
        
        Radd2 := RECORD
            STRING100 add2;      // Make field name the same name as the key file.
        END;

        Rvalue := RECORD
            STRING100 value;		// Make distinct from 'score' to ensure unique key field name for each key.
        END;

				Rextract := RECORD
            Link;            
						STRING8   date;
						STRING8   time;
						STRING12  account;
            STRING20  ref;
						STRING3   orderstatus;
            STRING21  contenttype;
						DATASET(RABI)   ABIs;
						DATASET(RScore) ScrIDs;
				END;

				Rextract tran(RECORDOF(d) L) := TRANSFORM

            STRING3 fn_order_status_code(String iCode, string iStatus) :=
                                                      MAP(iCode = '401' => '401',                  //Invalid Data
                                                          iCode = '402' => '402',                  //Invalid Account
                                                          iCode = '410' => '410',                  //Invalid ABI
                                                          iCode = '413' => '413',                  //Invalid Test Case
                                                          iCode = '' AND iStatus = 'E' => '404',   //Error
                                                                         iStatus = 'I' => '104',   //Incomplete
                                                          '100');                                  //Ready to be billed

						SELF.date    := L.date_added[1..4]+L.date_added[6..7]+L.date_added[9..10];
						SELF.time    := L.date_added[12..19];
						SELF.tid     := L.transaction_id;
            SELF.contenttype
                         := Clean(L.Content_Type);
						SELF.account := Clean(XMLTEXT('User/AccountNumber'));
						SELF.ref     := Clean(XMLTEXT('User/ReferenceNumber'));
						SELF.orderstatus
                         := fn_order_status_code(XMLTEXT('Header/Exceptions/Exception[1].Code'),XMLTEXT('Header/Status'));
						SELF.ABIs    := XMLPROJECT('InsurerResults/InsurerResult/ABI'
																			 ,TRANSFORM(RABI,
																									SELF.Abi    := Clean(XMLTEXT('Number'));
                                                  SELF.Broker := Clean(XMLTEXT('Broker'));
																								 ));

						SELF.ScrIDs  := XMLPROJECT('InsurerResults/InsurerResult/SubjectsResults/SubjectResult/Scores/Score'
																			 ,TRANSFORM(RScore,
																									SELF.Score := Clean(XMLTEXT('ScoreId')+'='+Clean(XMLTEXT('Value')) );
																								 ));
						SELF := L;
				END;
				
				d_for_idx := PARSE(d,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
														,tran(LEFT),XML('Row')) : INDEPENDENT;

				// For some strange reason one cannot do the following TRANSFORM inside PARSE.
				// Concatenate as many k=v items as possible into one keylength of key.
        // The concaternated string must be proceeded by and end in a separator (comma). In this way searchs
        // of the form REGEXFIND(',(XTB=89|ZTB=184),',value)  will succed even if the key being searched for
        // is at the start or end of a concaternated key.
        // Their being no ^ or $ anchor in ECL so one can't do REGEXFIND('(^|,)(XTB=89|ZTB=184)(,|$)',value)
				
				RcatScr := RECORD
				    Link;
						Rextract.ScrIDs;
				END;

				RcatScr ConcatenateScore(Rextract L) := TRANSFORM

						SELF.ScrIDs := ROLLUP(L.ScrIDs,LENGTH(TRIM(LEFT.Score,LEFT,RIGHT)) + LENGTH(TRIM(RIGHT.Score,LEFT,RIGHT)) < SIZEOF(RScore.Score)-2,
																	TRANSFORM(RScore,SELF.Score := TRIM(LEFT.Score,LEFT,RIGHT) + ',' + TRIM(RIGHT.Score,LEFT,RIGHT)));
						SELF := L;
				END;

				d2_for_idx := PROJECT(d_for_idx,ConcatenateScore(LEFT));

				Rrequest := RECORD
            Link;            
            STRING21  contenttype;
            STRING8   postcode;
            DATASET(Rvrn)   vrns;
            DATASET(Rpii)   pii;
            DATASET(Radd1)  Address1;
            DATASET(Radd2)  Address2;
				END;

				Rrequest tranRequest(RECORDOF(dr) L) := TRANSFORM

						SELF.tid     := L.transaction_id;
            SELF.contenttype
                         := Clean(L.Content_Type);

            SELF.vrns := XMLPROJECT('Vehicles/Vehicle'
                                   ,TRANSFORM(Rvrn,
                                              SELF.vrn := Collapse(XMLTEXT('RegNumber'));
                                             ));
            SELF.pii  := XMLPROJECT('Subjects/Subject'
                                   ,TRANSFORM(Rpii,
                                              SELF.title      := Clean(XMLTEXT('Name/Title'));
                                              SELF.firstname  := CleanAndRemove(XMLTEXT('Name/First'));
                                              SELF.middlename := CleanAndRemove(XMLTEXT('Name/Middle'));
                                              SELF.surname    := CleanAndRemove(XMLTEXT('Name/Last'));
                                              SELF.dob        := INTFORMAT((INTEGER)XMLTEXT('DOB/Year'),4,1)+INTFORMAT((INTEGER)XMLTEXT('DOB/Month'),2,1)+INTFORMAT((INTEGER)XMLTEXT('DOB/Day'),2,1);
                                             ));
            SELF.Address1 :=  ROW({CleanAndRemove(XMLTEXT('AddressInfo/Current/StreetAddress1'))},Radd1)
                            & ROW({CleanAndRemove(XMLTEXT('AddressInfo/Current/HouseName')  + ' '
                                                + XMLTEXT('AddressInfo/Current/FlatNumber') + ' '
                                                + XMLTEXT('AddressInfo/Current/HouseNumber') + ' '
                                                + XMLTEXT('AddressInfo/Current/StreetName'))},Radd1);
            SELF.Address2 :=  ROW({CleanAndRemove(XMLTEXT('AddressInfo/Current/StreetAddress2'))},Radd2)
                            & ROW({CleanAndRemove(XMLTEXT('AddressInfo/Current/District')  + ' '
                                                + XMLTEXT('AddressInfo/Current/PostalTown')  + ' '
                                                + XMLTEXT('AddressInfo/Current/County'))},Radd2);
            SELF.postcode := Collapse(XMLTEXT('AddressInfo/Current/PostCode'));

						SELF := L;
				END;

				dpii_idx := PARSE(dr,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
														,tranRequest(LEFT),XML('Row')) : INDEPENDENT;

				Rvalues := RECORD
            Link;            
            STRING21  contenttype;
            DATASET(Rvalue) values;
				END;

				BOOLEAN filterPrototype(STRING txt) := TRUE;		// Default is to pass through everything.
				
				Rvalues tranValues(RECORDOF(dr) L,STRING xmlitm,filterPrototype filter = filterPrototype) := TRANSFORM

						SELF.tid         := L.transaction_id;
            SELF.contenttype := Clean(L.Content_Type);

						itm := DATASET(STD.Str.SplitWords(XMLTEXT(xmlitm),'<'),{STRING line});

						Rvalue MakeFields(RECORDOF(itm) K) := TRANSFORM
								SELF.value := Clean(REGEXREPLACE('>',K.line,'='));
						END;

						// Only leaf nodes contain data after the '>'

						SELF.values :=  PROJECT(itm(REGEXFIND('.*>.+',line) AND filter(line)),MakeFields(LEFT));
						SELF := L;
				END;

				BOOLEAN bsbfilter(STRING txt) := NOT REGEXFIND('>\\{ND\\}',txt);
				
				dcc_for_idx := PARSE(dcc,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
														,tranValues(LEFT,'BSB<>',bsbfilter),XML('Row'));
				
				dex_for_idx := PARSE(dex,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
														,tranValues(LEFT,'//ConsumerSummary<>'),XML('Row'));

				dmv_for_idx := PARSE(dmv,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
														,tranValues(LEFT,'<>'),XML('Row'));

				dcu_for_idx := PARSE(dcu,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
														,tranValues(LEFT,'//ProductResponses<>'),XML('Row'));

				dci_for_idx := PARSE(dci,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
														,tranValues(LEFT,'//Results<>'),XML('Row'));

				dca_for_idx := PARSE(dca,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
														,tranValues(LEFT,'//Vehicle<>'),XML('Row'));

				// For some strange reason one cannot do the following TRANSFORM inside PARSE.
				// Concatenate as many k=v items as possible into one keylength of key.
        // The concaternated string must be proceeded by and end in a separator (comma). In this way searchs
        // of the form REGEXFIND(',(XTB=89|ZTB=184),',value)  will succed even if the key being searched for
        // is at the start or end of a concaternated key.
        // Their being no ^ or $ anchor in ECL so one can't do REGEXFIND('(^|,)(XTB=89|ZTB=184)(,|$)',value)

				Rvalues ConcatenateValues(Rvalues L) := TRANSFORM

						SELF.values := ROLLUP(L.values,LENGTH(TRIM(LEFT.value,LEFT,RIGHT)) + LENGTH(TRIM(RIGHT.value,LEFT,RIGHT)) < SIZEOF(Rvalue.value)-2,
																  TRANSFORM(Rvalue,SELF.value := TRIM(LEFT.value,LEFT,RIGHT) + ',' + TRIM(RIGHT.value,LEFT,RIGHT)));
						SELF := L;
				END;

				dvals_idx := PROJECT(dcc_for_idx
				                    +dex_for_idx
														+dmv_for_idx
														+dcu_for_idx
														+dci_for_idx
														+dca_for_idx,ConcatenateValues(LEFT)) : INDEPENDENT;
				RCarWeb := RECORD
            Link;            
						STRING7  vrm;
				END;

				RCarWeb tranCarweb(RECORDOF(dr) L) := TRANSFORM

						SELF.tid := L.transaction_id;
            SELF.vrm := Collapse(XMLTEXT('SearchCriteria/VRM'));
						SELF := L;
				END;

				dca_idx := PARSE(dca,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
														,tranCarweb(LEFT),XML('Row'));
				Rother := RECORD
            Link;            
            STRING21  contenttype;
			  END;
				
				Rother tranOther(RECORDOF(dth) L) := TRANSFORM
						SELF.tid     := L.transaction_id;
            SELF.contenttype := Clean(L.content_type);
						SELF := L;
				END;

				dother := PARSE(dth,'<Row>' + IF(is_compressed = 'Y',Decompress(Content_data),content_data) + '</Row>'
													 ,tranOther(LEFT),XML('Row'));
				NRabi := RECORD
           RABI;
					 Link;
				END;
				
				abi_for_idx := NORMALIZE(d_for_idx,LEFT.ABIs,TRANSFORM(NRabi,SELF := RIGHT;SELF := LEFT)) : INDEPENDENT;

				NRScore := RECORD
					 RScore;
					 Link;
				END;

        // Actually add the prefix and posfix separator (comma) here.

				scr_for_idx := NORMALIZE(d2_for_idx,LEFT.ScrIDs,TRANSFORM(NRScore,SELF.score := ','+TRIM(RIGHT.score,LEFT,RIGHT)+',',SELF := LEFT));

        NRvrn := RECORD
            Rvrn;
            Link;
        END;

        vrn_for_idx := NORMALIZE(dpii_idx,LEFT.vrns,TRANSFORM(NRvrn,SELF := RIGHT;SELF := LEFT));   // No INDEPENDENT as only used once.

        NRpii := RECORD
            Rpii;
            Link;
        END;

        pii_for_idx := NORMALIZE(dpii_idx,LEFT.pii,TRANSFORM(NRpii,SELF := RIGHT;SELF := LEFT)) : INDEPENDENT;
       
        NRadd1 := RECORD
            Radd1;
            Link;
        END;

        Add1_idx := NORMALIZE(dpii_idx,LEFT.Address1,TRANSFORM(NRadd1,SELF.add1 := TRIM(RIGHT.add1,LEFT,RIGHT),SELF := LEFT));

        NRadd2 := RECORD
            Radd2;
            Link;
        END;

        Add2_idx := NORMALIZE(dpii_idx,LEFT.Address2,TRANSFORM(NRadd2,SELF.add2 := TRIM(RIGHT.add2,LEFT,RIGHT),SELF := LEFT));

        NRvalue := RECORD
            Rvalue;
            Link;
        END;

        // Actually add the prefix and posfix separator (comma) here.
        
        value_idx := NORMALIZE(dvals_idx,LEFT.Values,TRANSFORM(NRvalue,SELF.value := ','+TRIM(RIGHT.value,LEFT,RIGHT)+',',SELF := LEFT));

        ct_for_idx :=  TABLE(d_for_idx,{contenttype,tid,RecPos})
                      +TABLE(dpii_idx, {contenttype,tid,RecPos})
                      +TABLE(dvals_idx,{contenttype,tid,RecPos})
                      +TABLE(dother,   {contenttype,tid,RecPos});

        /////////////////////////////////////////////////////////////////
        // Now start placing all the data we've collected into key files.
        /////////////////////////////////////////////////////////////////

				i1  := INDEX(DEDUP(SORT(d_for_idx (account != ''),account,tid),account,tid),             {account},    {tid,RecPos},MakeLFName(KID.ACCOUNT), OPT);
				i2  := INDEX(DEDUP(SORT(abi_for_idx(abi != ''),    abi,tid),    abi,tid),                {abi},        {tid,RecPos},MakeLFName(KID.ABI), OPT);
				i3  := INDEX(DEDUP(SORT(scr_for_idx(score != ''),score,tid),score,tid),                  {score},      {tid,RecPos},MakeLFName(KID.SCORE), OPT);
				i4  := INDEX(DEDUP(SORT(d_for_idx (date != ''),date,tid),date,tid),                      {date},       {tid,RecPos},MakeLFName(KID.DATE), OPT);
				i5  := INDEX(DEDUP(SORT(d_for_idx (time != ''),time,tid),time,tid),                      {time},       {tid,RecPos},MakeLFName(KID.TIME), OPT);
				i6  := INDEX(DEDUP(SORT(d_for_idx (tid != ''),tid),tid),                                 {tid},        {RecPos},    MakeLFName(KID.TID), OPT);
				i7  := INDEX(DEDUP(SORT(d_for_idx (orderstatus != ''),orderstatus,tid),orderstatus,tid), {orderstatus},{tid,RecPos},MakeLFName(KID.ORDERSTATUS), OPT);
				i8  := INDEX(DEDUP(SORT(d_for_idx (ref != ''),ref,tid),ref,tid),                         {ref},        {tid,RecPos},MakeLFName(KID.REF), OPT);
				i9  := INDEX(DEDUP(SORT(abi_for_idx(broker != ''),broker,tid),broker,tid),               {broker},     {tid,RecPos},MakeLFName(KID.BROKER), OPT);
				i10 := INDEX(DEDUP(SORT(vrn_for_idx(vrn != ''),vrn,tid),vrn,tid),                        {vrn},        {tid,RecPos},MakeLFName(KID.VRN), OPT);
				i11 := INDEX(DEDUP(SORT(pii_for_idx(title != ''),title,tid),title,tid),                  {title},      {tid,RecPos},MakeLFName(KID.TITLE), OPT);
				i12 := INDEX(DEDUP(SORT(pii_for_idx(firstname != ''),firstname,tid),firstname,tid),      {firstname},  {tid,RecPos},MakeLFName(KID.FIRSTNAME), OPT);
				i13 := INDEX(DEDUP(SORT(pii_for_idx(middlename != ''),middlename,tid),middlename,tid),   {middlename}, {tid,RecPos},MakeLFName(KID.MIDDLENAME), OPT);
				i14 := INDEX(DEDUP(SORT(pii_for_idx(surname != ''),surname,tid),surname,tid),            {surname},    {tid,RecPos},MakeLFName(KID.SURNAME), OPT);
				i15 := INDEX(DEDUP(SORT(pii_for_idx(dob != ''),dob,tid),dob,tid),                        {dob},        {tid,RecPos},MakeLFName(KID.DOB), OPT);
				i16 := INDEX(DEDUP(SORT(dpii_idx(postcode != ''),postcode,tid),postcode,tid),            {postcode},   {tid,RecPos},MakeLFName(KID.POSTCODE), OPT);
				i17 := INDEX(DEDUP(SORT(ct_for_idx(contenttype != ''),contenttype,tid),contenttype,tid), {contenttype},{tid,RecPos},MakeLFName(KID.CONTENTTYPE), OPT);
				i18 := INDEX(DEDUP(SORT(Add1_idx(add1 != ''),add1,tid),add1,tid),                        {add1},       {tid,RecPos},MakeLFName(KID.ADD1), OPT);
				i19 := INDEX(DEDUP(SORT(Add2_idx(add2 != ''),add2,tid),add2,tid),                        {add2},       {tid,RecPos},MakeLFName(KID.ADD2), OPT);
				i20 := INDEX(DEDUP(SORT(value_idx(value != ''),value,tid),value,tid),                    {value},      {tid,RecPos},MakeLFName(KID.VALUE), OPT);
				i21 := INDEX(DEDUP(SORT(dca_idx(vrm != ''),vrm,tid),vrm,tid),                            {vrm},        {tid,RecPos},MakeLFName(KID.VRM), OPT);

				AddToSF(TYPEOF(KID) id) := FUNCTION

          fn := MakeLFName(id);				
          sk := REGEXREPLACE('(.*?)::(il|tl)::.*::(.*?)',fn,'$1::$2::$3',NOCASE);
					
					RETURN SEQUENTIAL(STD.File.StartSuperFileTransaction()
													 ,STD.File.CreateSuperFile(sk,,TRUE)
													 ,STD.File.ClearSuperFile(sk,TRUE)
													 ,STD.File.AddSuperFile(sk,fn)
													 ,STD.File.FinishSuperFileTransaction()
													 );
				END;

				RETURN SEQUENTIAL(Purge_IL_Keys()
                         ,PARALLEL(SEQUENTIAL(BUILD(i1, OVERWRITE),AddToSF(KID.ACCOUNT))
											            ,SEQUENTIAL(BUILD(i2, OVERWRITE),AddToSF(KID.ABI))
											            ,SEQUENTIAL(BUILD(i3, OVERWRITE),AddToSF(KID.SCORE))
											            ,SEQUENTIAL(BUILD(i4, OVERWRITE),AddToSF(KID.DATE))
											            ,SEQUENTIAL(BUILD(i5, OVERWRITE),AddToSF(KID.TIME))
											            ,SEQUENTIAL(BUILD(i6, OVERWRITE),AddToSF(KID.TID))
											            ,SEQUENTIAL(BUILD(i7, OVERWRITE),AddToSF(KID.ORDERSTATUS))
                                  ,SEQUENTIAL(BUILD(i8, OVERWRITE),AddToSF(KID.REF))
                                  ,SEQUENTIAL(BUILD(i9, OVERWRITE),AddToSF(KID.BROKER))
                                  ,SEQUENTIAL(BUILD(i10,OVERWRITE),AddToSF(KID.VRN))
                                  ,SEQUENTIAL(BUILD(i11,OVERWRITE),AddToSF(KID.TITLE))
                                  ,SEQUENTIAL(BUILD(i12,OVERWRITE),AddToSF(KID.FIRSTNAME))
                                  ,SEQUENTIAL(BUILD(i13,OVERWRITE),AddToSF(KID.MIDDLENAME))
                                  ,SEQUENTIAL(BUILD(i14,OVERWRITE),AddToSF(KID.SURNAME))
                                  ,SEQUENTIAL(BUILD(i15,OVERWRITE),AddToSF(KID.DOB))
                                  ,SEQUENTIAL(BUILD(i16,OVERWRITE),AddToSF(KID.POSTCODE))
                                  ,SEQUENTIAL(BUILD(i17,OVERWRITE),AddToSF(KID.CONTENTTYPE))
                                  ,SEQUENTIAL(BUILD(i18,OVERWRITE),AddToSF(KID.ADD1))
                                  ,SEQUENTIAL(BUILD(i19,OVERWRITE),AddToSF(KID.ADD2))
                                  ,SEQUENTIAL(BUILD(i20,OVERWRITE),AddToSF(KID.VALUE))
                                  ,SEQUENTIAL(BUILD(i21,OVERWRITE),AddToSF(KID.VRM))
                                  )
                         );
         // RETURN PARALLEL(OUTPUT(dcc_for_idx,NAMED('dcc_for_idx'),ALL)
                        // ,OUTPUT(dex_for_idx,NAMED('dex_for_idx'),ALL)
				                // ,OUTPUT(dmv_for_idx,NAMED('dmv_for_idx'),ALL)
                        // ,OUTPUT(dcu_for_idx,NAMED('dcu_for_idx'),ALL)
                        // ,OUTPUT(dci_for_idx,NAMED('dci_for_idx'),ALL)
                        // ,OUTPUT(dca_for_idx,NAMED('dca_for_idx'),ALL)
                        // );

		END;  //Create_IL_Keys

		EXPORT Get_From_IL_Keys := MODULE
				//#workunit('name','Test IL Key (get)'); 

				SHARED fn1  := FNRoot+'account_key';
				SHARED fn2  := FNRoot+'abi_key';
				SHARED fn3  := FNRoot+'scoreid_key';

				SHARED i1 := SORT(DISTRIBUTE(INDEX({STRING12 account},{STRING20 tid,UNSIGNED8 RecPos},fn1)(account = '111'),    HASH32(tid)),tid,LOCAL);
				SHARED i2 := SORT(DISTRIBUTE(INDEX({STRING12 abi},    {STRING20 tid,UNSIGNED8 RecPos},fn2)(abi = '236'),        HASH32(tid)),tid,LOCAL);
				SHARED i3 := SORT(DISTRIBUTE(INDEX({STRING12 scoreid},{STRING20 tid,UNSIGNED8 RecPos},fn3)(scoreid = 'MSRZ01a'),HASH32(tid)),tid,LOCAL);

				SHARED j1 := SORT(JOIN(i1,i2,LEFT.tid = RIGHT.tid,TRANSFORM(LEFT),NOSORT,LOCAL,PARALLEL),tid,LOCAL);

				SHARED j2 := JOIN(j1,i3,LEFT.tid = RIGHT.tid,TRANSFORM(LEFT),NOSORT,LOCAL,PARALLEL);

				EXPORT internals := MODULE
				    EXPORT cj1 := COUNT(j1);
						EXPORT ires1 := j1;
				    EXPORT cj2 := COUNT(j2);
						EXPORT ires2 := j2;
				END;
				
				base := dfm_Files.DS_BASE_RECENT_INTLOG_PLUS_FP(DaysBack);

				EXPORT RawResult := FETCH(base,j2,RIGHT.RecPos,TRANSFORM(LayoutDeltaExports.Int_Log_Rec, SELF := LEFT));

				Rextract := RECORD
						DATASET(ws_insurance_uk.t_InsQuoteScoresResponse) response;
				END;

				Rextract XF1(LayoutDeltaExports.Int_Log_Rec L) := TRANSFORM
						SELF.response := FROMXML(ws_insurance_uk.t_InsQuoteScoresResponse,'<Row>'+IF(L.is_compressed = 'Y',Decompress(L.Content_data),L.Content_data)+'</Row>');
				END;		

				EXPORT ContentDataResult := PROJECT(RawResult,XF1(LEFT));  
		END;

		EXPORT GenerateQuery(STRING queryText,STRING GroupByText,STRING OutputRecords,STRING4 Format,INTEGER ReturnNLines = 0,INTEGER DeSprayOnSize = 0) := FUNCTION
       /*
          queryText     : The query to pass
          GroupByText   : Any 'GroupBy' directective   e.g. account_number,date_added[1..10]
          OutputRecords : The record types to include in any output, or input to 'GroupBy'. This is a comma separated list of 'content_type'

          RETURNS
              Text of a ECL program that 'compiles' or text of an error message/
       */
       ActionType := ENUM(UNSIGNED1,None,LogicalOr,LogicalAnd,LogicalXor,LogicalNot);
                    
        Symbol := RECORD
            ActionType Action;
            STRING5 id;
            STRING key;
            STRING Filter;
        END;

				Production := RECORD
				    DATASET(Symbol) itm;
				END;

        DATASET(Symbol) ParseQuery() := FUNCTION
        
            infile := DATASET(ROW(transform({ string line }, self.line := queryText)));
            TYPEOF(Symbol.id) GetID := 'S'+INTFORMAT(HASH32(STD.System.Util.GetUniqueInteger(),RANDOM())%10000,SIZEOF(Symbol.id)-1,1);
            
            PRULE := RULE TYPE (Production);

            PATTERN ws         := PATTERN('[[:space:]]');
            TOKEN   key        := PATTERN('[A-Za-z][A-Za-z0-9_]*');
            PATTERN firstchar  := PATTERN('[[:alnum:]\'\\|*&%!~#;:@?<>=+\\-_()\\{\\},.\\[\\]/]');
            PATTERN subsequent := firstchar | ws;
            PATTERN anything   := firstchar+subsequent*;
            PATTERN quotechar  := '"';
            TOKEN   quotedword := quotechar anything quotechar;

            PRULE forwardExpr  := USE(Production, 'ExpressionRule');

            PRULE op
                :=    key quotedword        TRANSFORM(Production,
                                                     SELF.itm := ROW({ActionType.None,GetID,$1,Clean($2[2..length($2)-1])},Symbol);
                                                    )
                    | '(' forwardExpr ')'
                    | 'NOT' key quotedword  TRANSFORM(Production,
                                                     SELF.itm := ROW({ActionType.LogicalNot,GetID,$2,Clean($3[2..length($3)-1])},Symbol);
                                                    )
                    | 'NOT' '(' forwardExpr ')' TRANSFORM(Production,
                                                     SELF.itm := $3.itm & ROW({ActionType.LogicalNot,GetID,$3.itm[COUNT($3.itm)].id,''},Symbol);
                                                    )
                    ;
            PRULE factor
                :=    op
                    | SELF 'AND' op         TRANSFORM(Production,
                                                      SELF.itm := $1.itm & $3.itm & ROW({ActionType.LogicalAnd,GetID,$1.itm[COUNT($1.itm)].id,$3.itm[COUNT($3.itm)].id},Symbol)
                                                     )
                    ;
            PRULE term
                :=    factor
                    | SELF 'OR' factor      TRANSFORM(Production,
                                                      SELF.itm := $1.itm & $3.itm & ROW({ActionType.LogicalOr ,GetID,$1.itm[COUNT($1.itm)].id,$3.itm[COUNT($3.itm)].id},Symbol)
                                                     )
                    | SELF 'XOR' factor     TRANSFORM(Production,
                                                      SELF.itm := $1.itm & $3.itm & ROW({ActionType.LogicalXor,GetID,$1.itm[COUNT($1.itm)].id,$3.itm[COUNT($3.itm)].id},Symbol)
                                                     )
                    ; 
            PRULE expr
                :=  term                  : DEFINE ('ExpressionRule');

            p1 := PARSE(infile,line,expr,TRANSFORM(Production,SELF := $1),FIRST,WHOLE,SKIP(ws+),NOCASE,PARSE);

            RETURN NORMALIZE(p1,LEFT.itm,TRANSFORM(Symbol,SELF := RIGHT));

        END : INDEPENDENT;        // End of ParseQuery
        
        RGroup := RECORD
            STRING field;
            STRING filter;
        END;

        RDGroup := RECORD
            DATASET(RGroup) itm;
        END;

        DATASET(RGroup) ParseGroup() := FUNCTION

            infile := DATASET(ROW(transform({ string line }, self.line := GroupByText)));
            PRULE := RULE TYPE (RDGroup);

            PATTERN ws         := PATTERN('[[:space:]]');
            PATTERN firstchar  := PATTERN('[[:alpha:]]');
            PATTERN nextchar   := PATTERN('[[:alnum:]_]');
            PATTERN digits     := PATTERN('[[:digit:]]+');
            TOKEN   field      := firstchar+nextchar*;
            TOKEN   filter     := (digits (ws* '..' ws* digits?)?) | ( ws* '..' ws* digits);     // Currently only allow [n] or [n..m] or [n..] or [..n]

            PRULE forwardExpr  := USE(RDGroup, 'ExpressionRule');

            PRULE term
                :=   field        TRANSFORM(RDGroup,
                                             SELF.itm := ROW({$1,''},RGroup);
                                         )
                   | field '[' filter  ']' TRANSFORM(RDGroup,
                                             SELF.itm := ROW({$1,Collapse($2+$3+$4)},RGroup);
                                         )
                   ;
            PRULE op
                :=   term        
                   | forwardExpr ',' term  TRANSFORM(RDGroup,
                                             SELF.itm := $1.itm & $3.itm;
                                          )
                   ;
            PRULE expr
                :=   op : DEFINE ('ExpressionRule');

            p1 := PARSE(infile,line,expr,TRANSFORM(RDGroup,SELF := $1),FIRST,WHOLE,SKIP(ws+),NOCASE,PARSE);

            RETURN NORMALIZE(p1,LEFT.itm,TRANSFORM(RGroup,SELF := RIGHT));

        END : INDEPENDENT;        // End of ParseGroup

        ParseOutputRecords() := FUNCTION

            /* Check that the supplied list of 'content_type' records to output is in the allowed list
               Output a string that is either an error message if the requests content_types are invalid, or output the ECL
               requerned to define the correcly filtered 'content_type' key so a JOIN on the queried results returns the
               IL BASE records of the required type.

               If no output records are specified then output all record types for the transaction(s) that match the query. 

               Note one can query on one set of record types and output a completly different set of record types.
            */
            validContentTypes := DATASET([{UKResponse}
                                         ,{Request}
                                         ,{CCResponseMessage}
                                         ,{ExperianResponse}
                                         ,{MapViewPerilsResponse}
                                         ,{CUEResponse}
                                         ,{CallIDSearchResponse}
                                         ,{CarWebResponse}
                                         ,{MapViewAccumResponse}
                                         ,{COOPResponse}
                                         ,{NCDResponse}],{STRING txt});

            hdr := 'ilct   := SORT(DISTRIBUTE(INDEX({STRING21 contenttype},{STRING20 tid,UNSIGNED8 RecPos},\''+MakeSFName(keys[KID.CONTENTTYPE].keyname)+'\'),HASH32(tid)),tid,LOCAL)';
            
            o1 := DEDUP(SORT(DATASET(STD.Str.SplitWords(TRIM(OutputRecords,ALL),','),{STRING txt}),txt),txt);
            po1:= PROJECT(o1,TRANSFORM(RECORDOF(o1), SELF.txt := '\''+Clean(LEFT.txt)+'\''));
        
            j  := JOIN(o1,validContentTypes,STD.Str.ToUpperCase(LEFT.txt) = STD.Str.ToUpperCase(RIGHT.txt),TRANSFORM(LEFT),LEFT ONLY,LOOKUP);
            cj := UKServices_Utilities.ConcatenateStringFields(j,txt,',');

            RETURN MAP(EXISTS(j)  => '-5: Unrecognised content_type: '+cj
                      ,NOT EXISTS(o1) => hdr+';\n'
                      ,hdr+'(contenttype IN [ '+UKServices_Utilities.ConcatenateStringFields(po1,txt,',')+']);\n'
                       );
        END : INDEPENDENT;      // End of ParseOutputRecords

				ParseFormatQualifier() := FUNCTION

						// Return either the qualifiing string to the OUTPUT command
						// or an error message.

						f := TRIM(Clean(Format),ALL);
						RETURN  MAP(f = 'CSV'    => 'CSV(SEPARATOR(\',\'),TERMINATOR(\'\\n\'))'
											 ,f = 'XML'    => f
											 ,f = 'JSON'	=> 'JSON(HEADING(\'[\',\']\'))'
											 ,'-6: Unrecognised Output Format');
				END : INDEPENDENT;

        /////////////////////////////////////////////////////////////////
        // GenerateQuery Main Entry Point
        /////////////////////////////////////////////////////////////////
 
        // Do some checks on the resultant expressions before attemting any execution
        
        n1 := ParseQuery();
        invalidkeys := UKServices_Utilities.ConcatenateStringFields(JOIN(n1(Action = ActionType.None),keys,LEFT.key = RIGHT.keyname,TRANSFORM(LEFT),LEFT ONLY),key,',');

        g1 := ParseGroup();
        invalidGroup := GroupByText != '' AND NOT EXISTS(g1);
        validGroup   := GroupByText != '' AND EXISTS(g1);
				
        o1 := ParseOutputRecords();
        invalidContentType := o1[1] = '-';
        RestrictingOutput  := NOT invalidContentType AND TRIM(OutputRecords,ALL) != '';

				formatQualifier := ParseFormatQualifier();
				invalidFormat := formatQualifier[1] = '-';

        line := RECORD
            STRING txt;
        END;

        STRING IGenerateQuery() := FUNCTION

            // Generate ECL filter and FETCH code from parsed expression.

            d_keys := DICTIONARY(keys,{keyname => keytype});

						// If 'key' is metioned in the 'filter', as a whole word, then assume the filter has been completly 
						// specified by the 'Filter', otherwise make the 'key' the LHS of the 'Filter'.
            
            GenerateFilter(Symbol L) := IF(REGEXFIND('[^[:alnum:]]'+L.key+'[^[:alnum:]]',L.Filter,NOCASE),L.Filter,L.key+' '+L.Filter);

            line GenerateSearch(Symbol L) := TRANSFORM
                SELF.txt := L.id + ' := ' + CASE(L.Action
                                                ,ActionType.None        => 'SORT(DISTRIBUTE(INDEX({'+d_keys[L.key].keytype+' '+L.key+'},{STRING20 tid,UNSIGNED8 RecPos},\''+MakeSFName(L.key)+'\')('+GenerateFilter(L)+'),HASH32(tid)),tid,LOCAL);'
                                                ,ActionType.LogicalOr   => 'DEDUP(SORT(JOIN('+L.key+','+L.Filter+',LEFT.tid = RIGHT.tid,FULL OUTER,NOSORT,LOCAL,PARALLEL),tid,LOCAL),tid,LOCAL);'
                                                ,ActionType.LogicalAnd  => 'SORT(JOIN('+L.key+','+L.Filter+',LEFT.tid = RIGHT.tid,TRANSFORM(LEFT),NOSORT,LOCAL,PARALLEL),tid,LOCAL);'
                                                ,ActionType.LogicalXor  => 'SORT(JOIN('+L.key+','+L.Filter+',LEFT.tid = RIGHT.tid,FULL ONLY,NOSORT,LOCAL,PARALLEL),tid,LOCAL);'
                                                ,ActionType.LogicalNot  => IF(L.Filter = '',   // NOT on a previously created intermediate result, negate against all transaction_IDs.
                                                                                'SORT(JOIN(iltid,'+L.key+',LEFT.tid = RIGHT.tid,TRANSFORM(LEFT),LEFT ONLY,NOSORT,LOCAL,PARALLEL),tid,LOCAL);'
                                                                             ,                 // NOT on a single operand
                                                                                'SORT(DISTRIBUTE(INDEX({'+d_keys[L.key].keytype+' '+L.key+'},{STRING20 tid,UNSIGNED8 RecPos},\''+MakeSFName(L.key)+'\')(NOT '+GenerateFilter(L)+'),HASH32(tid)),tid,LOCAL);'
                                                                             )
                                                ,ERROR('GenerateQuery: Invalid Action'));
            END;

						line GenerateGroupByRecordStructure(RGroup L,INTEGER cnt,STRING1 id) := TRANSFORM
							  SELF.txt := IF(cnt = 1,'RT'+id+' := RECORD\n','')
								             +'   '+L.field+ ' := t'+id+'.'+L.field+IF(id = '1' AND L.filter != '',L.filter,'')+';';
						END;
						
						line GenerateGroupByCommand(RGroup L,STRING1 id) := TRANSFORM
							  SELF.txt := L.field+IF(id = '1' AND L.filter != '',L.filter,'');
						END;

            mess1              := '         ,\''+REGEXREPLACE('\'',queryText,'\\\\\'')+'\\n\\n';
            messgrp            := 'Grouped By: '+REGEXREPLACE('\'',GroupByText,'\\\\\'')+'\\n\\n';
            returnedRecTypes   := 'Record Types Returned from Query: '+OutputRecords+'\\n\\n';
            messresultsNoGroup := '\'+IF(cp>0,cp+\' Records (\'+tp+\') in\\n\\n\'+ofn,\'No Results\')\n';
            messresultsGroup   := '\'+IF(cp>0,cp+\' Groups (\'+tp+\') in\\n\\n\'+ofn,\'No Results\')\n';

            emailBody := mess1+MAP(NOT validGroup AND NOT RestrictingOutput  => messresultsNoGroup
                                  ,NOT validGroup AND     RestrictingOutput  => returnedRecTypes+messresultsNoGroup
                                  ,    validGroup AND NOT RestrictingOutput  => messgrp+messresultsGroup
                                  ,                                             messgrp+returnedRecTypes+messresultsGroup);
            constECL := DATASET([// Preamble to all ECL constructs
						  /* 1 */            {'IMPORT InsuranceUK_iesp.ws_insurance_uk;\n'
                                 +'IMPORT UKServices_V7.LoggingConstants AS I;\n'
																 +'#workunit(\'name\',\'Generated IL_Miner Query\');\n'
																 +'Decompress := UKServices_Utilities.MySqlZlib.Decompress;\n'
																 +'V := UKServices_V7.UKLayouts;\n'
																 +'// '+queryText+'\n'
                                 +'iltid  := SORT(DISTRIBUTE(INDEX({'+keys[KID.TID].keytype+' '+keys[KID.TID].keyname+'},{UNSIGNED8 RecPos},\''+MakeSFName(keys[KID.TID].keyname)+'\'),HASH32(tid)),tid,LOCAL);\n'
                                 +o1
                                 +'tltid  := ProjectUK_Deltas.Key_transaction_log.Key;'}
																 // Postamble to dynamic query constructs
              /* 2 */           ,{'ilbase := Projectuk_deltas.dfm_Files.DS_BASE_RECENT_INTLOG_PLUS_FP('+DaysBack+');\n'
																 +'gather := SORT(JOIN(ilct,'+n1[COUNT(n1)].id+',LEFT.tid = RIGHT.tid,TRANSFORM(LEFT),NOSORT,LOCAL,PARALLEL)'+IF(ReturnNLines > 0,'[1..'+ReturnNLines+']','')+',tid,LOCAL);\n'
																 +'ilRslt := FETCH(ilbase,gather,RIGHT.RecPos,TRANSFORM(LEFT));\n'
                                 +'Result := JOIN(ilRslt,tltid,LEFT.transaction_id = RIGHT.transaction_id,KEYED) : INDEPENDENT;\n'
                                 +'Wrobel.IL_Miner('+DaysBack+').Results XF1(RECORDOF(Result) L) := TRANSFORM\n'
						                     +'   SELF.response           := IF(L.Content_Type = I.UKResponse,            FROMXML(ws_insurance_uk.t_InsQuoteScoresResponse, \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.Request            := IF(L.Content_Type = I.Request,               FROMXML(ws_insurance_uk.t_InsQuoteScoresRequest,  \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.CCresponse         := IF(L.Content_Type = I.CCResponseMessage,     FROMXML(V.Layout_CC_Results,                      \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.Experianresponse   := IF(L.Content_Type = I.ExperianResponse,      FROMXML(V.Layout_Experian_Results,                \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.MapViewresponse    := IF(L.Content_Type = I.MapViewPerilsResponse, FROMXML(V.Layout_MapViewPerils_Results,           \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.MapViewAccresponse := IF(L.Content_Type = I.MapViewAccumResponse,  FROMXML(V.Layout_MapViewAccumulations_Results,    \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.CarWebresponse     := IF(L.Content_Type = I.CarWebResponse,        FROMXML(V.Layout_CW_Results,                      \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.CUEresponse        := IF(L.Content_Type = I.CUEResponse,           FROMXML(V.Layout_CUE_Results,                     \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.COOPresponse       := IF(L.Content_Type = I.COOPResponse,          FROMXML(V.Layout_CoOpMembership_Results,          \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.NCDresponse        := IF(L.Content_Type = I.NCDResponse,           FROMXML(V.Layout_NCD_Results,                     \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF.CallIdresponse     := IF(L.Content_Type = I.CallIDSearchResponse,  FROMXML(V.Layout_CallIDSearch_Results,            \'<Row>\'+IF(L.is_compressed = \'Y\',Decompress(L.Content_data),L.Content_data)+\'</Row>\'));\n'
						                     +'   SELF := L;\n'
																 +'END;'}
																 // Code where there is no 'GroupBy'
							/* 3 */						,{'p  := PROJECT(Result,XF1(LEFT));'}
																 // Preamble Code for 'GroupBy'																
							/* 4 */						,{'t1 := PROJECT(Result,XF1(LEFT));'}
                                 // Postambles for 'GroupBy'
							/* 5 */						,{'   UNSIGNED cnt := COUNT(GROUP);\n'
                                 +'END;'}
                                 // Postambles for 'GroupBy'
							/* 6 */						,{'   UNSIGNED cnt := SUM(GROUP,t2.cnt);\n'
                                 +'END;'}
																	// Postamble to all ECL constructs
							/* 7 */						,{'cp := COUNT(p) : INDEPENDENT;\n'
																 +'dp := COUNT(DEDUP(Result,transaction_id)) : INDEPENDENT;\n'
																 +'tp := dp+\' Transaction\'+IF(dp!=1,\'s\',\'\');\n'
                                 +'tname := \'~ilminer::'+WORKUNIT+'\';\n'
                                 +'o := OUTPUT(p,,tname,'+formatQualifier+');\n'
                                 +'ofn := \'/data/HPCCSystems/dropzone/\'+REGEXREPLACE(\'::\',tname[2..],\'_\')+\'.'+STD.Str.ToLowerCase(TRIM(Format,ALL))+'\';\n'
                                 +'dsp := STD.File.DeSpray(tname\n' 
                                 +'                       ,_Control.ThisEnvironment.landingzone\n'
                                 +'                       ,ofn,,,,TRUE);\n'
                                 +'del := STD.File.DeleteLogicalFile(tname,TRUE);\n'
                                 +'dspray := SEQUENTIAL(o,dsp,del);\n'
                                 +'email := fileservices.sendemail(\'matthew.rundle@lexisnexis.co.uk,Alan.Osborne@lexisnexis.com,domenico.pirlo@lexisnexis.com,Insurance.Technical.Support@lexisnexis.co.uk,allan.wrobel@lexisnexis.com\'\n'
                                 +'         ,\'Query Complete, see \'+WORKUNIT\n'
																 +emailBody
																 +'         ,lib_system.smtpserver, lib_system.smtpport, lib_system.emailAddress);\n'
                                 +'SEQUENTIAL(OUTPUT(cp,NAMED(\'COUNT_RES\'))\n'
                                 +'          ,IF(cp>'+DeSprayOnSize+',dspray,OUTPUT(p,NAMED(\'RES\'),ALL))\n'
																 +'          ,NOTIFY(EVENT(\'ilminer\',\'<Event><returnTo>'+WORKUNIT+'</returnTo></Event>\'))\n'
																 +'          ,email\n'
																 +'          );'}
                                ],line);
            
            p := PROJECT(n1,GenerateSearch(LEFT));
						gone := PROJECT(g1,GenerateGroupByRecordStructure(LEFT,COUNTER,'1'));
						gtwo := PROJECT(g1,GenerateGroupByRecordStructure(LEFT,COUNTER,'2'));
						tone := UKServices_Utilities.ConcatenateStringFields(PROJECT(g1,GenerateGroupByCommand(LEFT,'1')),txt,',');
						ttwo := UKServices_Utilities.ConcatenateStringFields(PROJECT(g1,GenerateGroupByCommand(LEFT,'2')),txt,',');
						tbl1Command := DATASET([{'t2 := TABLE(t1,RT1,'
						                       +tone
													         +',LOCAL);'}],line);
 						tbl2Command := DATASET([{'p := SORT(TABLE(t2,RT2,'
						                       +ttwo
													         +'),'
													         +ttwo
													         +');'}],line);
           RETURN IF(validGroup
						         ,UKServices_Utilities.ConcatenateStringFields( constECL[1]
										                                              & p 
																																	& constECL[2]
																																	& constECL[4]
																																	& gone
																																	& constECL[5]
																																	& tbl1Command
																																	& gtwo
																																	& constECL[6]
																																	& tbl2Command
																																	& constECL[7],txt,'\n')
						         ,UKServices_Utilities.ConcatenateStringFields( constECL[1]
										                                              & p
																																	& constECL[2]
																																	& constECL[3]
																																	& constECL[7],txt,'\n')
										 );
        END;						// End of IGenerateQuery

        ECLQuery := IGenerateQuery();

        RawCompiledResult := UKServices_Utilities.WorkUnitManagement.fCompileECL(ECLQuery)(Severity = 'Error') : INDEPENDENT;

        line ErrorText(RECORDOF(RawCompiledResult) L,INTEGER cnt) := TRANSFORM,SKIP(cnt > 1)    // Dont confuse; only show the 1st error.
            SELF.txt := L.Severity + '(' + L.Code + ') '+L.Message + ' (Line:' + L.LineNo + ', Column:'+L.Column+')';
        END;

        CompilerErrors := UKServices_Utilities.ConcatenateStringFields(PROJECT(RawCompiledResult,ErrorText(LEFT,COUNTER)),txt,'\n');

        // RETURN ECLQuery;
        RETURN MAP(NOT EXISTS(n1)            => '-1: Syntax error in supplied query.'
                  ,LENGTH(invalidkeys) > 0   => '-2: Reference to unknown keys: '+invalidkeys
                  ,invalidGroup              => '-3: Invalid Grouping construct.'
                  ,invalidContentType        => o1
									,invalidFormat             => formatQualifier
 /* last chk */   ,EXISTS(RawCompiledResult) => '-4: Invalid Filter(s) or Field names:\n'+CompilerErrors
                  ,ECLQuery);
    END;

		EXPORT RunQuery(STRING QueryText,STRING GroupByText,STRING OutputRecords,STRING4 Format,INTEGER ReturnNLines = 0,INTEGER DeSprayOnSize = 0) := FUNCTION

       /*
          queryText     : The query to pass
          GroupByText   : Any 'GroupBy' directective   e.g. account_number,date_added[1..10]
          OutputRecords : The record types to include in any output, or input to 'GroupBy'. This is a comma separated list of 'content_type'

          Executes the ECL generated from the supplied parameters as a background WU.
       */
				ecl          := GenerateQuery(QueryText,GroupByText,OutputRecords,Format,ReturnNLines,DeSprayOnSize) : INDEPENDENT;
        
				Cluster      := STD.System.job.Target() : INDEPENDENT;

				GenCompilerError := '#workunit(\'name\',\'Compiler Error IL_Miner\');\n'
				                   +'SEQUENTIAL(output(-1,NAMED(\'COUNT_RES\'))\n'
													 +'          ,NOTIFY(EVENT(\'ilminer\',\'<Event><returnTo>'+WORKUNIT+'</returnTo></Event>\'))\n'
												   +'          );';

				ValidEcl     := ecl[1] != '-';
				
				eclToRun     := IF(ValidEcl,ecl,GenCompilerError) : INDEPENDENT;
				
        RunQueryWUid := UKServices_Utilities.WorkUnitManagement.fSubmitNewWorkunit(eclToRun, Cluster, Cluster+'.thor') : INDEPENDENT;

				CountResult  := DATASET(WORKUNIT(RunQueryWUid,'COUNT_RES'),{INTEGER cnt}) : INDEPENDENT;
				Result       := DATASET(WORKUNIT(RunQueryWUid,'RES'),Results) : INDEPENDENT;

				LaterReturn  := MAP(CountResult[1].cnt = -1            => OUTPUT(ecl,NAMED('FAILURE'))
				                   ,CountResult[1].cnt > DeSprayOnSize => OUTPUT(CountResult[1].cnt+' Records Desprayed to: ilminer_'+WORKUNIT+'.csv')
				                   ,OUTPUT(Result,NAMED('RESULT'),ALL)
													 );
				
				BackGround := SEQUENTIAL(OUTPUT(QueryText,NAMED('Query'))
				                        ,IF(OutputRecords != '',OUTPUT(OutputRecords,NAMED('Record_Types_to_be_Returned_from_Query')))
				                        ,IF(GroupByText   != '',OUTPUT(GroupByText,NAMED('GroupBy')))
                                ,OUTPUT(RunQueryWUid,NAMED('WUID_Running_Query'))
                                ,OUTPUT('Results will be Desprayed to: ilminer_'+WORKUNIT+'.'+STD.Str.ToLowerCase(TRIM(Format,ALL)),NAMED('Response'))
									              ,RunQueryWUid);

				// doit := FUNCTION
				    // RETURN IF(EVENTEXTRA('returnTo') = WORKUNIT AND ValidEcl AND DeSprayOnSize != 0,LaterReturn);
				// END;

				// doit : WHEN(EVENT('ilminer','*'),COUNT(1));

				// ForeGround := SEQUENTIAL(RunQueryWUid
																// ,doit 
																// );

			  //RETURN IF(ValidEcl AND DeSprayOnSize = 0,BackGround,ForeGround);
        RETURN IF(ValidEcl,BackGround,OUTPUT(ecl,NAMED('COMPILER_ERROR')));
		END;

		EXPORT DoxieQuery() := FUNCTION

       #workunit('name','IL_Miner '+-DaysBack+' Day'+IF(-DaysBack>1,'s',''));
       STRING      Query := ''            : STORED('Query');
       STRING      GroupBy := ''          : STORED('Group_By');
			 INTEGER     ReturnNLines := 100    : STORED('Limit_Records_Returned_from_Query');
       STRING      OutputRecords := ''    : STORED('Record_Types_Returned_from_Query');
       INTEGER     DeSprayOnSize := 0     : STORED('Despray_on_Size');
			 STRING4     Format        := 'CSV' : STORED('Output_Format_CSV_XML_JSON');

			 // When grouping results, we want them all out, only restrict number of records output
			 // when just outputing records. (no grouping)
			 
       RETURN RunQuery(Query,GroupBy,OutputRecords,Format,IF(GroupBy = '',ReturnNLines,0),0);
		END;
END;
