crimedata = LOAD '/Users/nidhisarode/Downloads/crimes-in-chicago/Chicago_Crimes_2012_to_2017.csv' USING PigStorage (',') AS (id1,ID,CaseNumber,Date,Block,IUCR,PrimaryType,Description,LocationDescription,Arrest,Domestic,Beat,District,Ward,Community,Area,FBICode,XCoordinate,YCoordinate,Year,UpdatedOn,Latitude,Longitude,Location);

filtered = FILTER crimedata BY EqualsIgnoreCase(Arrest,'TRUE');

grpd = GROUP filtered BY District;

cntd = FOREACH grpd GENERATE group, COUNT(filtered);

STORE cntd INTO '/Users/nidhisarode/Desktop/EBDFinal/PigAnalysis/countofarrestsbydistricts';

