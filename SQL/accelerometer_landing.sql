CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_db`.`accelerometer_landing` 
( 
`user` string, 
`timeStamp` bigint, 
`x` float, 
`y` float, 
`z` float 
) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
LOCATION 's3://stedi-lakehouse-yazeedalmuhlaki/accelerometer/landing/'