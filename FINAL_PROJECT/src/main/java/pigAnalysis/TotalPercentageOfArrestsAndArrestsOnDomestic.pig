totalarrests = LOAD '/Users/nidhisarode/Desktop/EBDFinal/PercentageOfArrestsByYear/part-r-00000' AS (year,percentagearrests);

domesticarrests = LOAD '/Users/nidhisarode/Desktop/EBDFinal/PercentageOfArrestsBasedOnDomesticViolence/part-r-00000';

joined = JOIN totalarrests BY year , domesticarrests BY $0;

final = FOREACH joined GENERATE $0,$1,$3;

Sorted = ORDER final BY $0 ASC;

STORE Sorted INTO '/Users/nidhisarode/Desktop/EBDFinal/PigAnalysis/JoinedDomesticPerArrestswithTotalArrests/';