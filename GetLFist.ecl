EXPORT GetLFList := MODULE

IMPORT _Control;

#if(_Control.ThisEnvironment.Name = 'Prod')	
   SHARED STRING URl := 'http://10.193.68.21:8010//WsDfu';
 #elseif(_Control.ThisEnvironment.Name = 'QA')	
   SHARED STRING URl := 'http://10.193.64.21:8010//WsDfu';
 #else
   SHARED STRING URl := 'http://10.193.64.21:9010//WsDfu';
#end	

    EXPORT FileSubSets := ENUM(UNSIGNED1,SuperOnly,LogicalOnly,Both); 
   /*
      Get list of logical files using full regex input

      Get list using standard wildcarding, then apply input regex on the restuned result.
      The filter passed to HPCC is all alphanumeric characters upto the 1st regex component of the input, followed by '*'

      So input 'thor::base::(inter|tran).*'  is converted to 'thor::base::*' for input to the HPCC probe.
   */
   EXPORT Go(STRING fName = 'thor*deltauk*inter*'
            ,FileSubSets ftype = FileSubSets.Both
            ,BOOLEAN CaseSignificant = FALSE
            ,STRING pURL = URl) := FUNCTION
   
      fName0 := TRIM(fName,LEFT,RIGHT);
      fName1 := IF(fName0[1] = '~',fName0[2..],fname0);     //  Strip any leading tilda
      fName2 := REGEXFIND('(.*?)[^A-Z0-9:]',fName1,1,NOCASE)+'*';
      
      DFUInfoRequest 											:= RECORD, MAXLENGTH(100)
            STRING  LogicalName{XPATH('LogicalName')} := fName2;
            INTEGER FirstN{XPATH('FirstN')} := 1000000;
      END;

      ESPExceptions_Lay 									:= RECORD
            STRING  Code{XPATH('Code'),MAXLENGTH(10)};
            STRING  Audience{XPATH('Audience'),MAXLENGTH(50)};
            STRING  Source{XPATH('Source'),MAXLENGTH(30)};
            STRING  Message{XPATH('Message'),MAXLENGTH(200)};
      END;

      DFULogicalFile 											:=  RECORD ,  MAXLENGTH(1000)	
            STRING  ClusterName{XPATH('ClusterName')};
      //			STRING  Directory{XPATH('Directory'),MAXLENGTH(255)};
            STRING  Name{XPATH('Name'),MAXLENGTH(70)};
            Boolean isSuperfile{XPATH('isSuperfile')};
            STRING 	Owner{XPATH('Owner'),MAXLENGTH(20)};
            STRING 	RecordCount{XPATH('RecordCount'),MAXLENGTH(20)};
            STRING 	LongSize{XPATH('LongSize'),MAXLENGTH(20)};
            Boolean isZipfile{XPATH('isZipfile')};
            Boolean Replicate{XPATH('Replicate')};
      END;

      DFULogicalFiles 										:= RECORD, MAXLENGTH(300)
            DATASET(DFULogicalFile)  LogicalFiles{XPATH('DFULogicalFile'),maxcount(500)};
      END;

      DFUInfoResponse      								:= RECORD
            DATASET(ESPExceptions_Lay)    Exceptions{XPATH('Exceptions/ESPException'),maxcount(110)};
            DATASET(DFULogicalFiles)  		LogicalFiles{XPATH('DFULogicalFiles'),maxcount(500)};
      END;

      DATASET DFUInfoSoapCall   					:=   SOAPCALL(pURL
            ,'DFUQuery'
            ,DFUInfoRequest
            ,DFUInfoResponse
            ,XPATH('DFUQueryResponse')
            );


      DATASET(DFULogicalFile) logicalFiles0 :=  NORMALIZE(DFUInfoSoapCall.LogicalFiles,LEFT.LogicalFiles,TRANSFORM(RIGHT));
      logicalFiles := MAP(ftype = FileSubSets.SuperOnly   =>  logicalFiles0(IsSuperfile = TRUE)
                         ,ftype = FileSubSets.LogicalOnly =>  logicalFiles0(IsSuperfile = FALSE)
                         ,logicalFiles0);
      
      //OUTPUT(logicalFiles,NAMED('LogicalFiles'),ALL);

      res := IF(CaseSignificant,logicalFiles(REGEXFIND(fName1,Name)),logicalFiles(REGEXFIND(fName1,Name,NOCASE)));
      
      RETURN res;
   END;
  
END;
