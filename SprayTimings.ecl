IMPORT * FROM ContributionLoadUKPD;
IMPORT STD;

EXPORT SprayTimings := MODULE

   R := RECORD
        STRING8 id;
        STRING20 Fn;
        STRING20 Mn;
        STRING20 Sn;
        STRING100 Add;
        STRING50 City;
        STRING30 County;
        STRING11 PostCode;
        STRING20 Country;
        STRING200 Txt;
   END;

  SHARED c :=     ContributionLoadUKPD.Constants;
	
  EXPORT
	sprayV  := STD.File.SprayVariable(   c.landingzone,               // landing zone 
																			 c.dropzone+'1.csv',          // input file
																			 1000,                        // max rec
																			 ',',                         // field sep
																			 ,                            // rec sep (use default)
																			 '"',                         // quote
																			 c.thor_dest,                 // destination group
																			 '~afw::sprayVariable',       // destination logical name
																			 -1,                          // time
																			 c.espserverIPport,           // esp server IP port
																			 ,                            // max connections
																			 TRUE,                        // overwrite
																			 FALSE,                       // replicate
																			 TRUE );                      // compress

  EXPORT
	sprayF  := STD.File.SprayFixed   (   c.landingzone,               // landing zone 
																			 c.dropzone+'2.csv',          // input file
																			 125,                         // rec size
																			 c.thor_dest,                 // destination group
																			 '~afw::sprayFixed',          // destination logical name
																			 -1,                          // time
																			 c.espserverIPport,           // esp server IP port
																			 ,                            // max connections
																			 TRUE,                        // overwrite
																			 FALSE,                       // replicate
																			 TRUE );                      // compress
   
END;
