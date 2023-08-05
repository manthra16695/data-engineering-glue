{\rtf1\ansi\ansicpg1252\cocoartf2709
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Monaco;}
{\colortbl;\red255\green255\blue255;\red18\green19\blue24;\red255\green255\blue255;}
{\*\expandedcolortbl;;\cssrgb\c8627\c9804\c12157;\cssrgb\c100000\c100000\c100000;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs24 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 CREATE EXTERNAL TABLE `customer_landing_moorthy`(\
  `customername` string COMMENT 'from deserializer', \
  `email` string COMMENT 'from deserializer', \
  `phone` string COMMENT 'from deserializer', \
  `birthday` string COMMENT 'from deserializer', \
  `serialnumber` string COMMENT 'from deserializer', \
  `registrationdate` bigint COMMENT 'from deserializer', \
  `lastupdatedate` bigint COMMENT 'from deserializer')\
ROW FORMAT SERDE \
  'org.openx.data.jsonserde.JsonSerDe' \
WITH SERDEPROPERTIES ( \
  'case.insensitive'='TRUE', \
  'dots.in.keys'='FALSE', \
  'ignore.malformed.json'='FALSE', \
  'mapping'='TRUE') \
STORED AS INPUTFORMAT \
  'org.apache.hadoop.mapred.TextInputFormat' \
OUTPUTFORMAT \
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
LOCATION\
  's3://customerlandingmoorthyproj/'\
TBLPROPERTIES (\
  'classification'='json', \
  'transient_lastDdlTime'='1691273860')\
}