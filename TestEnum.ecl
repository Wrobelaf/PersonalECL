EXPORT TestEnum := FUNCTION

    #workunit('name','Test Enum');
    SET OF STRING FilterTypes := ['InsuredValue','Bound','Address','PerilSource'];

    AFilter := RECORD
       TYPEOF(FilterTypes) Dimension;
       STRING Value;
    END;

    iSearchTerms := INTERFACE
      EXPORT BOOLEAN Active := FALSE;
      EXPORT AFilter := [];
    END;

    Parms := STORED(iSearchTerms);
    RETURN Parms;

END;
