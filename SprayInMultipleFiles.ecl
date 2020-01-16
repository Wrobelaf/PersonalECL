EXPORT SprayInMultipleFiles := MODULE

IMPORT * FROM STD;
IMPORT _Control;
IMPORT UKServices_Utilities;

#workunit('name','Load lots of logical files');

d := DATASET([{'p-thorXX.ukrisk.net.txt'}],{string root});

RECORDOF(d) DoIt(d L,INTEGER cnt) := TRANSFORM
    SELF.root := REGEXREPLACE('XX',L.root,INTFORMAT(cnt,2,1));
END;

  EXPORT Go() := FUNCTION

     SprayIn(STRING name) := FUNCTION

        lfl := '~PhySize::'+name;
        sfn := '~PhySize::p-thor';
        Spray :=
        STD.File.SprayVariable(
                              _Control.ThisEnvironment.landingzone,
                              '/data/thorprodfiles/'+name,         // input file
                              1000,                    // max rec
                              ':',          // field sep
                              '\n',        // rec sep
                              '\'',                           // quote
                              UKServices_Utilities.GetAGroup('thordev3'),               // destination group
                              lfl,           // destination logical name
                              -1,                         // time
                              _Control.ThisEnvironment.ESP_FileSprayURL,
                              ,                           // max connections
                              TRUE,                       // overwrite
                              FALSE,                      // replicate
                              TRUE                      // compress
               );

        createsf := STD.File.CreateSuperFile(sfn,,TRUE);
        addsf := STD.File.AddSuperFile(sfn,lfl);
        RETURN SEQUENTIAL(Spray,createsf,addsf);
        
    END;

    RETURN NOTHOR(APPLY(NORMALIZE(d,80,DoIt(LEFT,COUNTER)),SprayIn(root)));
    
  END;

END;
