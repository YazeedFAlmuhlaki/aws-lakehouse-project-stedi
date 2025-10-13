CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_db`.`customer_landing` 
( `customerName` string, 
`email` string, 
`phone` string, 
`birthDay` string, 
`serialNumber` string, 
`registrationDate` bigint, 
`lastUpdateDate` bigint, 
`shareWithResearchAsOfDate` bigint, 
`shareWithPublicAsOfDate` bigint, 
`shareWithFriendsAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-lakehouse-yazeedalmuhlaki/customer/landing/'