import STD, _validate,lib_stringlib, ut;

_Flag	:=	unsigned4;

export Date
 :=
  module
	shared	lValidYearLow	:=	1600		:	stored('_Validate_Year_Range_Low');
	shared	lValidYearHigh	:=	2999		:	stored('_Validate_Year_Range_High');
	shared	sDaysNormalYear	:=	[31,28,31,30,31,30,31,31,30,31,30,31];
	shared	sDaysLeapYear	:=	[31,29,31,30,31,30,31,31,30,31,30,31];
	shared	boolean		fValidYear(integer pYear)
							 :=	_Validate.Support.fIntegerInRange(pYear,lValidYearLow,lValidYearHigh);
	export	boolean		fIsLeapYear(integer pYear)
							:=	fValidYear(pYear)
							and pYear % 4 = 0
							and (pYear % 100 <> 0 or pYear % 400 = 0)
							;
	export	integer		fDaysInMonth(integer pYear, integer pMonth)
							:=	if(_Validate.Support.fIntegerInRange(pMonth,1,12),
								   if(fIsLeapYear(pYear),
									  sDaysLeapYear[pMonth],
									  sDaysNormalYear[pMonth]
									 ),
								   0
								  )
							;
	shared	boolean		fValidMonth(integer pMonth)	:=	_Validate.Support.fIntegerInRange(pMonth,1,12);
	shared	boolean		fValidDay(integer pYear, integer pMonth, integer pDay)
							:=	_Validate.Support.fIntegerInRange(pDay,1,fDaysInMonth(pYear,pMonth));
	export	Rules	:=	enum(_Flag,
							 None				=	0,
							 Optional			=	1b,
							 YearFilled			=	10b,
							 MonthFilled		=	100b,
							 DayFilled			=	1000b,
							 YearNonZero		=	10000b,
							 MonthNonZero		=	100000b,
							 DayNonZero			=	1000000b,
							 YearValid			=	10000000b,
							 MonthValid			=	100000000b,
							 DayValid			=	1000000000b,
							 DateInPast			=	10000000000b,
							 InvalidCharacters	=	100000000000000b,
							 ChronStateUnknown	=	1000000000000000b,
							 DateYMFilled		=	MonthFilled,
							 DateYMDFilled		=	DayFilled,
							 DateYMValid		=	MonthValid,
							 DateYMDValid		=	DayValid
							)
					;
	export	string	fGetRulesFromFlags(_Flag pFlags)
	 :=	
	  function
		boolean		fFlagIsOn(_Flag pValue, _Flag pFlag)	:=	pValue & pFlag = pFlag;
		string		lReturnValue		:=	if(pFlags = Rules.None,
											   'None',
											   if(fFlagIsOn(pFlags, Rules.Optional),			' Optional','')
										+	   if(fFlagIsOn(pFlags, Rules.YearFilled),			' YearFilled','')
										+	   if(fFlagIsOn(pFlags, Rules.MonthFilled),			' MonthFilled','')
										+	   if(fFlagIsOn(pFlags, Rules.DayFilled),			' DayFilled','')
										+	   if(fFlagIsOn(pFlags, Rules.YearNonZero),			' YearNonZero','')
										+	   if(fFlagIsOn(pFlags, Rules.MonthNonZero),		' MonthNonZero','')
										+	   if(fFlagIsOn(pFlags, Rules.DayNonZero),			' DayNonZero','')
										+	   if(fFlagIsOn(pFlags, Rules.YearValid),			' YearValid','')
										+	   if(fFlagIsOn(pFlags, Rules.MonthValid),			' MonthValid','')
										+	   if(fFlagIsOn(pFlags, Rules.DayValid),			' DayValid','')
										+	   if(fFlagIsOn(pFlags, Rules.DateInPast),			' DateInPast','')
										+	   if(fFlagIsOn(pFlags, Rules.InvalidCharacters),	' InvalidCharacters','')
										+	   if(fFlagIsOn(pFlags, Rules.ChronStateUnknown),	' ChronStateUnknown','')
											  )
										;
		return		lib_stringlib.stringlib.stringfindreplace(trim(lib_stringlib.stringlib.stringcleanspaces(lReturnValue),left,right),' ',',');
	  end
	 ;
	shared	set		sValidDateLengths	:=	[4,6,8];
	shared	boolean	fDateStringTestsOk(string pDateString) := lib_stringlib.stringlib.stringfilter(pDateString,'0123456789') = pDateString;
	shared	unsigned8	fDateContentTest(string pDateString, boolean pZeroMonthOkay=false, boolean pZeroDayOkay=false)
	 :=
	  function
		unsigned1	lLowestValidMonth	:=	if(pZeroMonthOkay,0,1);
		unsigned1	lLowestValidDay		:=	if(pZeroMonthOkay or pZeroDayOkay,0,1);
		string		lDateString			:=	if(pDateString = '0','',trim(pDateString,right));
		integer8	lDateInteger		:=	(unsigned8)lDateString;
		boolean		lDateStringTestsOk	:=	fDateStringTestsOk(lDateString);//regexfind('[0-9]{0,8}',lDateString,0) = lDateString;
		string		lDateStringIfValid	:=	if(lDateStringTestsOk and length(lDateString) in ([0] + sValidDateLengths),lDateString,'');
		unsigned1	lLengthIfValid		:=	length(lDateStringIfValid);
		string4		lYearString			:=	if(lLengthIfValid >= 4,lDateString[1..4],'-1');
		string2		lMonthString		:=	if(lLengthIfValid >= 6,lDateString[5..6],'-1');
		string2		lDayString			:=	if(lLengthIfValid  = 8,lDateString[7..8],'-1');
		integer2	lYearSigned			:=	(integer2)lYearString;
		integer2	lMonthSigned		:=	(integer2)lMonthString;
		integer2	lDaySigned			:=	(integer2)lDayString;
		_Flag		lInvalidChars		:=	if(lDateStringTestsOk,0,Rules.InvalidCharacters);
		_Flag		lNotYearFilled		:=	if(lYearSigned  <> -1,0,Rules.YearFilled);
		_Flag		lNotMonthFilled		:=	if(lMonthSigned <> -1,0,Rules.MonthFilled);
		_Flag		lNotDayFilled		:=	if(lDaySigned   <> -1,0,Rules.DayFilled);
		_Flag		lNotYearNonZero		:=	if(lNotYearFilled <> 0 	or lYearSigned  =   0,Rules.YearNonZero,0);
		_Flag		lNotMonthNonZero	:=	if(lNotMonthFilled <> 0 or lMonthSigned =   0,Rules.MonthNonZero,0);
		_Flag		lNotDayNonZero		:=	if(lNotDayFilled <> 0 	or lDaySigned   =   0,Rules.DayNonZero,0);
		_Flag		lNotYearValid		:=	if(fValidYear(lYearSigned),0,Rules.YearValid);
		_Flag		lNotMonthValid		:=	if(lNotYearValid = 0  and _Validate.Support.fIntegerInRange(lMonthSigned,lLowestValidMonth,12),0,Rules.MonthValid);
		_Flag		lNotDayValid		:=	if(lNotMonthValid = 0 and _Validate.Support.fIntegerInRange(lDaySigned,lLowestValidDay,fDaysInMonth(lYearSigned,lMonthSigned)),0,Rules.DayValid);
		string4		lYearStringIfValid	:=	if(lNotYearValid <> 0,'',lYearString);
		string2		lMonthStringIfValid	:=	if(lNotMonthValid <> 0,'',lMonthString);
		string2		lDayStringIfValid	:=	if(lNotDayValid <> 0,'',lDayString);
		string8		lpDateForChronTest	:=	trim(lYearStringIfvalid + lMonthStringIfValid + lDayStringIfValid,right);
		integer1	lLengthOfpDateChron	:=	length(trim(lpDateForChronTest));
		_Flag		lChronStateUnknown	:=	if(lLengthOfpDateChron <> 0,0,Rules.ChronStateUnknown);
		_Flag		lNotInPast			:=	if(lChronStateUnknown <> 0 or (unsigned4)lpDateForChronTest > (unsigned4)((STRING8)STD.Date.Today())[1..lLengthOfpDateChron],Rules.DateInPast,0);
		_Flag		lOptionalPossible	:=	if(lInvalidChars = 0 and lNotYearNonZero | lNotMonthNonZero | lNotDayNonZero = Rules.YearNonZero | Rules.MonthNonZero | Rules.DayNonZero,Rules.Optional,0);

		unsigned8	lValidDateValue		:=	(((unsigned8)lYearStringIfValid)*10000)
										+	(((unsigned8)lMonthStringIfValid)*100)
										+	((unsigned8)lDayStringIfValid)
										<<	16;

		return		lValidDateValue
				  |	lInvalidChars
				  |	lNotYearFilled
				  | lNotMonthFilled
				  | lNotDayFilled
				  | lNotYearNonZero
				  | lNotMonthNonZero
				  | lNotDayNonZero
				  | lNotYearValid
				  | lNotMonthValid
				  | lNotDayValid
				  |	lChronStateUnknown
				  |	lNotInPast
				  |	lOptionalPossible
				  ;
	  end
	 ;

	export	boolean	fIsValid(string pDateString, _Flag pRuleFlags = Rules.DayValid, boolean pZeroMonthOkay=false, boolean pZeroDayOkay=false)
	 :=
	  function
		_Flag		lAllButOptional		:=	(_Flag)0xFFFF ^ Rules.Optional;
		_Flag		lDateValidityFlags	:=	fDateContentTest(pDateString, pZeroMonthOkay, pZeroDayOkay) & 0xFFFF;
		boolean		lOptionalOverride	:=	pRuleFlags & lDateValidityFlags & Rules.Optional = Rules.Optional;
		boolean		lReturnValue		:=	lOptionalOverride or (lDateValidityFlags & lAllButOptional & pRuleFlags) = 0;
		return		lReturnValue;
	  end
	 ;

	export	_Flag	fInvalidFlags(string pDateString, boolean pZeroMonthOkay=false, boolean pZeroDayOkay=false)
	 :=
	  function
		return fDateContentTest(pDateString,pZeroMonthOkay,pZeroDayOkay) & 0xFFFF;
	  end
	 ;

	export	string8	fCorrectedDateString(string pDateString, boolean pUseOneInsteadOfZero=false)
	 :=
	  function
		unsigned6	lEntireReturnValue	:=	fDateContentTest(pDateString);
		_Flag		lDateValidityFlags	:=	lEntireReturnValue & 0xFFFF;
		boolean		lYearValid			:=	lDateValidityFlags & Rules.YearValid = 0;
		boolean		lMonthValid			:=	lDateValidityFlags & Rules.MonthValid = 0;
		boolean		lDayValid			:=	lDateValidityFlags & Rules.DayValid = 0;
		unsigned4	lDateValueReturned	:=	lEntireReturnValue >> 16;
		unsigned4	lDateMonthDayFixed	:=	lDateValueReturned
										+	if(not lYearValid,
											   0,
											   if(lMonthValid,
												  0,
												  (100 * if(pUseOneInsteadOfZero,1,0))
												 )
										+	   if(lDayValid,
												  0,
												  if(pUseOneInsteadOfZero,1,0)
												 )
											  );
		return		(string8)intformat(lDateMonthDayFixed,8,1);
	  end
	 ;

  end
 ;
