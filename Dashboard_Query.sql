SELECT id,flight_date, airline_name, departure_airport, departure_country, 
departure_timezone, arrival_airport, arrival_country,arrival_timezone,
SPLIT(CONCAT(SPLIT(departure_scheduled_time,"T")[0], ' ', SPLIT(departure_scheduled_time,"T")[1]),"\\+")[0] as departure_scheduled_time, 
(unix_timestamp(SPLIT(CONCAT(SPLIT(departure_scheduled_time,"T")[0], ' ', SPLIT(departure_scheduled_time,"T")[1]),"\\+")[0])-1771000000)/10 as departure_scheduled_time_number,
SPLIT(departure_estimated_time,"\\.")[0] as departure_estimated_time, 
(unix_timestamp(SPLIT(departure_estimated_time,"\\.")[0])-1771000000)/10 as departure_estimated_time_number,
SPLIT(CONCAT(SPLIT(arrival_scheduled_time,"T")[0], ' ', SPLIT(arrival_scheduled_time,"T")[1]),"\\+")[0] as arrival_scheduled_time,
(unix_timestamp(SPLIT(CONCAT(SPLIT(arrival_scheduled_time,"T")[0], ' ', SPLIT(arrival_scheduled_time,"T")[1]),"\\+")[0])-1771000000)/10 as arrival_scheduled_time_number, 
SPLIT(arrival_estimated_time,"\\.")[0] as arrival_estimated_time,
(unix_timestamp(SPLIT(arrival_estimated_time,"\\.")[0])-1771000000)/10 as arrival_estimated_time_number
 FROM workspace.aviation_db.scheduled_flights

 --Some Scalar in time adjustement and format to compare time