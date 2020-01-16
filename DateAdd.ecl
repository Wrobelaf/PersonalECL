IMPORT STD;
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//// FIRST INPUT: character string (Y M D order)
////	Format Examples:	[0-9]+ Years [0-9]+ Months [0-9]+ Days
////						[0-9]+ Yrs [0-9]+ Mos 
////						[0-9]+ Years 
////						[0-9]+ Mnth [0-9]+ Dy
////						[0-9]+ Days
////						etc...
////
//// SECOND INPUT: character string in YYYYMMDD format
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

EXPORT date_add(string pinput, string indate) := function
//////////// place string input in workable format
	input := if(regexreplace('[^YMD]' ,regexreplace('(DAYS)|(DAY)|(DYS)|(DY)',regexreplace('(MONTHS)|(MONTH)|(MOS)|(MO)|(MTHS)|(MTH)',regexreplace('(YEARS)|(YEAR)|(YRS)|(YR)', trim(STD.Str.ToUpperCase(pinput), all), 'Y'), 'M'), 'D'), '') in ['YMD', 'YM', 'YD', 'MD', 'D', 'Y', 'M'], pinput, '');
	upcase := regexreplace('[^YMD0-9]' ,regexreplace('(DAYS)|(DAY)|(DYS)|(DY)',regexreplace('(MONTHS)|(MONTH)|(MOS)|(MO)|(MTHS)|(MTH)',regexreplace('(YEARS)|(YEAR)|(YRS)|(YR)', trim(STD.Str.ToUpperCase(input), all), 'Y'), 'M'), 'D'), '');
	years := (integer)if(regexfind('Y', upcase), (integer)regexreplace('[^0-9]', upcase[1..STD.Str.Find(upcase, 'Y', 1)], ''), 0) ;
	months := (integer)if(regexfind('Y*M', upcase), regexreplace('[^0-9]', upcase[STD.Str.Find(upcase,'Y',1)..STD.Str.Find(upcase,'M',1)], ''), 
				if( regexfind('M', upcase),regexreplace('[^0-9]', upcase[1..STD.Str.Find(upcase, 'M', 1)], ''),'0')) ;
	days := (integer)if(~regexfind('D', upcase),'',if(regexfind('[^D]', regexreplace('[0-9]', upcase, '')), 
				regexreplace('[^0-9]',regexreplace('^.*[YM]', upcase, '' ),  '')
					,regexreplace('[^0-9]', upcase, '')));
	
	//handle partial dates as STD library wont work for partial dates 	
  indateYear  := indate[1..4]; 
	indateMonth := IF(indate[5..6] in ['00',''], '01',indate[5..6]); 
	indateDay   := IF(indate[7..8] in ['00',''], '01',indate[7..8]); 
	
	indatefull := indateYear+indateMonth+indateDay	;	
	newdate    := (STRING)Std.Date.AdjustDate((UNSIGNED)indatefull, years,months,days);									 
  RETURN newdate;
END;
