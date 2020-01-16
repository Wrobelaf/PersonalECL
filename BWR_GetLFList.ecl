#workunit('name','Get File List Full Regex');
IMPORT _Control;
w := Wrobel.GetLFList;
STRING regex := '' : STORED('regex');
BOOLEAN Super := TRUE : STORED('Include_Supers_default_true');
BOOLEAN Logical := TRUE : STORED('Include_Logicals_default_true');
BOOLEAN CaseNo := TRUE : STORED('Case_Insensitive_default_true');

#if(_Control.ThisEnvironment.Name = 'Prod')	
   STRING URl := 'http://10.193.68.21:8010' : STORED('URL_default_same_thor');
 #elseif(_Control.ThisEnvironment.Name = 'QA')	
   STRING URl := 'http://10.193.64.21:8010' : STORED('URL_default_same_thor');
 #else
   STRING URl := 'http://10.193.64.21:9010' : STORED('URL_default_same_thor');
#end	

ftype := MAP(Super = TRUE  AND Logical = TRUE  => w.FileSubSets.Both
            ,Super = FALSE AND Logical = TRUE  => w.FileSubSets.LogicalOnly
            ,Super = TRUE  AND Logical = FALSE => w.FileSubSets.SuperOnly,
            w.FileSubSets.Both);

w.Go(regex,ftype,CaseNo,URl+'//WsDfu');