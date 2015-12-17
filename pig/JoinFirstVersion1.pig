REGISTER s3://mapreduce-xiao/hw3/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Set the amount of reduce tasks to 10
SET default_parallel 10;

-- Load the file from input parameter
Flight = LOAD '$INPUT' using CSVLoader();

-- parse only required attributes as Flight1
Flight1 = FOREACH Flight GENERATE $5 as FlightDate, $11 as Origin, $17 as Dest, 
          $24 as DepTime, $35 as ArrTime, $37 as ArrDelayMinutes, $41 as Cancelled,
	      $43 as Diverted;

-- parse only required attributes as Flight2
Flight2 = FOREACH Flight GENERATE $5 as FlightDate, $11 as Origin, $17 as Dest, 
          $24 as DepTime, $35 as ArrTime, $37 as ArrDelayMinutes, $41 as Cancelled,
	      $43 as Diverted; 

-- Filter out unmatching records
Flight1_data = FILTER Flight1 BY (Origin eq 'ORD' AND Dest neq 'JFK') AND (Cancelled neq 1.0 AND Diverted neq 1.0);
Flight2_data = FILTER Flight2 BY (Origin neq 'ORD' AND Dest eq 'JFK') AND (Cancelled neq 1.0 AND Diverted neq 1.0);

-- Join Flight1_data and Flight2_data using the same flight date and the same intermediate airport
Results = JOIN Flight1_data by (FlightDate, Dest), Flight2_data by (FlightDate, Origin);

-- Filter out the results where the DepTime in Flight2 is not after the ArrTime in Flights1
Results = FILTER Results BY $4 < $11;

-- Filter out the results where the FlightDate does not fall into the time period
Results = FILTER Results BY (ToDate($0,'yyyy-MM-dd') <= ToDate('2008-05-31','yyyy-MM-dd')) 
		AND (ToDate($0,'yyyy-MM-dd') >= ToDate('2007-06-01','yyyy-MM-dd'))
		AND (ToDate($8,'yyyy-MM-dd') <= ToDate('2008-05-31','yyyy-MM-dd'))
		AND (ToDate($8,'yyyy-MM-dd') >= ToDate('2007-06-01','yyyy-MM-dd'));

-- Calculate delay for each valid two-leg flight
Results = FOREACH Results GENERATE ($5 + $13) as Delay;

-- Calculate average delay for all valid two-leg flights
GroupResults = GROUP Results all;
AverageDelay = FOREACH GroupResults GENERATE AVG(Results.Delay);

-- Store the result to output parameter
STORE AverageDelay into '$OUTPUT';
