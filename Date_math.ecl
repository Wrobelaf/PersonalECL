/**
  *Function to return a calculated date by taking in starting date (YYYYMMDD) and offset.  
  *Offset is # of days and can be positive or negative.
 */
IMPORT STD; 
EXPORT date_math(STRING8 start_date, INTEGER offset) := FUNCTION 
 
 tDate := TRIM(start_date,LEFT,RIGHT); 
 lDate := LENGTH(tDate);
 pDate := MAP(lDate = 4 => tDate+'1201',
              tDate[5..6] ='00'=> tDate[1..4]+'1201',
              lDate = 6 => tDate+'01' ,
              tDate[5..8] ='0000'=> tDate[1..4]+'1201',
              tDate);				 
 Adjustoffset := IF(LENGTH(tDate) = 4 OR LENGTH(tDate) = 6 OR tDate[5..8]   ='0000' , offset-1, offset);  
 
 RETURN (STRING) Std.Date.AdjustDate((UNSIGNED4)pDate, 0,0,Adjustoffset); 

END; 