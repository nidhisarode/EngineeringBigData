crimedata = LOAD '/Users/nidhisarode/Downloads/crimes-in-chicago/Chicago_Crimes_2012_to_2017.csv' USING PigStorage (',') AS (id1,ID,CaseNumber,Date,Block,IUCR,PrimaryType,Description,LocationDescription,Arrest,Domestic,Beat,District,Ward,Community,Area,FBICode,XCoordinate,YCoordinate,Year,UpdatedOn,Latitude,Longitude,Location);

iucr = LOAD '/Users/nidhisarode/Documents/EBD/FinalProjDatasets/IUCR.csv' USING PigStorage (',') AS (IUCR1,PRIMARYDESCRIPTION,SECONDARYDESCRIPTION,INDEXCODE);

joined = JOIN crimedata BY IUCR LEFT OUTER, iucr BY IUCR1;

final = FOREACH joined GENERATE ID,CaseNumber,Date,IUCR,PRIMARYDESCRIPTION,SECONDARYDESCRIPTION;

STORE final INTO '/Users/nidhisarode/Desktop/EBDFinal/PigAnalysis/iucrjoin';