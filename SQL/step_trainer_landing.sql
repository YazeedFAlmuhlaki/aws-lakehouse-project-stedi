CREATE EXTERNAL TABLE `stedi_db`.`step_trainer_landing` 
( 
`sensorReadingTime` bigint, 
`serialNumber` string, 
`distanceFromObject` int 
) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
LOCATION 's3://stedi-lakehouse-yazeedalmuhlaki/step_trainer/landing/'