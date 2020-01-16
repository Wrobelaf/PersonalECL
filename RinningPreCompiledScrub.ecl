EXPORT RunningPreCompiledScrub := MODULE

/*
<email_publish_testRequest>
	<email_address>String</email_address>
	<email_subject>String</email_subject>
	<email_body>String</email_body>
</email_publish_testRequest>
*/
/*
<email_publish_testResponse>
	<Results>
		<Result/>
	</Results>
</email_publish_testResponse>
*/
// ------------------------------------------------------------------------------------------------

/*
Example NCD scrub on DEV is:

scrub::ukpd::c000880001::i000233347_b131851010_eukpdiu1000003_20130704141800_firstzurichpolicies_sprayed

*/
IMPORT ContributionScrubs,ContributionScrubsOrbit,_control;

string	gPublishedCluster		:=	'thordev10_2';
string	gPublishedJobName		:=	'ncd_scrub';
string	gTargetESPAddress		:=	'http://10.222.64.15';
string	gTargetESPPort			:=	'8002';

	EXPORT fCallscrub(string pFilename,ContributionScrubs.Constants.ScrubMode mode)	:= FUNCTION

		Appid := REGEXFIND('^.*?::(.*?)::.*$',pFilename,1,NOCASE);

		rCallScrubJobRequest	:=
		record
			STRING  qascrub_stored_sidex_applicationid{xpath('qascrub_stored_sidex_applicationid'),maxlength(4)} 		:= Appid;
			STRING  qascrub_stored_sidex_lineofbusiness{xpath('qascrub_stored_sidex_lineofbusiness'),maxlength(2)}	:= '01';
			STRING  qascrub_stored_sidex_sourceid{xpath('qascrub_stored_sidex_sourceid'),maxlength(15)} 						:= REGEXFIND('^.*?e'+Appid+'(.*?)_.*$',pFilename,1,NOCASE);
			STRING  qascrub_stored_batchnumber{xpath('qascrub_stored_batchnumber'),maxlength(15)}										:= REGEXFIND('^.*?_b(.*?)_.*$',pFilename,1,NOCASE);
			STRING  qascrub_stored_istestitem{xpath('qascrub_stored_istestitem'),maxlength(1)} 											:= '0';
			STRING  qascrub_stored_cc_flag_dupefldrpt{xpath('qascrub_stored_cc_flag_dupefldrpt'),maxlength(1)} 			:= '0';
			STRING  qascrub_stored_isitemonhold{xpath('qascrub_stored_isitemonhold'),maxlength(2)} 									:= '0';
			STRING  qascrub_stored_inputfilename{xpath('qascrub_stored_inputfilename'),maxlength(512)} 							:= pFilename;
			STRING  qascrub_stored_productenum{xpath('qascrub_stored_productenum'),maxlength(1)} 									  := '1';
			STRING  _validate_year_range_high{xpath('_validate_year_range_high'),maxlength(4)} 											:= '1600';
			STRING  _validate_year_range_low{xpath('_validate_year_range_low'),maxlength(4)} 												:= '2999';
			STRING  qascrub_stored_scrubmodeenum{xpath('qascrub_stored_scrubmodeenum'),maxlength(1)} 								:= (STRING1) mode;
	end;

		rESPExceptions	:=
		record
			string					Code{xpath('Code'),maxlength(10)};
			string					Audience{xpath('Audience'),maxlength(50)};
			string					Source{xpath('Source'),maxlength(30)};
			string					Message{xpath('Message'),maxlength(200)};
		end;

		rResults	:=
		record
			string					Result{xpath('Result'),maxlength(1024)};
		end;

		rCallScrubJobResponse	:=
		record
			dataset(rResults)					Results{xpath('Results'),maxcount(100)};
			dataset(rESPExceptions)		Exceptions{xpath('Exceptions/ESPException'),maxcount(110)};
		end;

		dWUCallScrubJobResult	:=	soapcall(gTargetESPAddress + ':' + gTargetESPPort + '/WsEcl/soap/query/' + gPublishedCluster + '/' + gPublishedJobName + '?_async=1&_jobname=xyz',
																 'gTargetJobName',
																 rCallScrubJobRequest,
																 rCallScrubJobResponse,
																 xpath('ncd_scrub_testResponse')
																);

		return	dWUCallScrubJobResult;
	end;

	EXPORT TestCallScrub() := FUNCTION
			RETURN output(fCallscrub('scrub::ukpd::c000880001::i000233347_b131851910_eukpdiu1000003_20131212121200_firstzurichpolicies_sprayed',ContributionScrubs.Constants.ScrubMode.Test));
	END;

END;

