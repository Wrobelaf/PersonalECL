IMPORT * FROM ProjectUK_deltas;
#workunit('name','IL conversion BASE64');
d := DATASET('~thor::base::deltauk::20140127::recent::intermediate_log.txt', LayoutDeltaExports.Int_Log_Rec, THOR, OPT);;

LayoutNewIL := record 
        string20 transaction_id ;
        string11 product_id;
        string20 date_added;
        string4  process_type;
        string8  processing_time;
        string20 source_code;
        string20 Content_Type;
        string20 Version;
        string20 reference_number;
        string2  Subject_id;
        string   Content_data {BLOB, maxlength(2000000)};
				STRING1  is_compressed;
    END;


LayoutNewIL Convert(LayoutDeltaExports.Int_Log_Rec L) := TRANSFORM
   SELF.is_compressed := 'N';
	 SELF := L;
END;

d2 := PROJECT(d,Convert(LEFT),LOCAL);

OUTPUT(d2,,'~thor::base::deltauk::20140128::recent::intermediate_log.txt',overwrite,COMPRESSED);
