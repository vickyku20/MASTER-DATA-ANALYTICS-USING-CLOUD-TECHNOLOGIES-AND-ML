CREATE OR REPLACE DATABASE AWS_DATABASE;
USE AWS_DATABASE;
 
CREATE OR REPLACE TABLE RETAIL_TXNS LIKE  AZUREDATABASE.PUBLIC.TRANSACTION_RAW;
CREATE OR REPLACE FILE FORMAT AWS_RETAIL_TXNXS_CSV LIKE AZUREDATABASE.PUBLIC.RETAIL_TRNXS_CSV;

CREATE OR REPLACE STORAGE integration retail_txns_aws_stage
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::944234077060:role/retaill_txn_access_role'
STORAGE_ALLOWED_LOCATIONS =('s3://retailvickyy/');

desc integration retail_txns_aws_stage;

CREATE OR REPLACE STAGE retail_txns_aws_stage
URL ='s3://retailvickyy'
file_format= AWS_RETAIL_TXNXS_CSV 
storage_integration =retail_txns_aws_stage;

LIST @retail_txns_aws_stage;

SHOW STAGES;

---CREATE SNOWPIPE THAT RECOGNISES CSV THAT ARE INGESTED FROM EXTERNAL STAGE AND COPIES THE DATA INTO EXISTING TABLE

--The AUTO_INGEST=true parameter specifies to read 
--- event notifications sent from an S3 bucket to an SQS queue when new data is ready to load.

CREATE OR REPLACE PIPE RETAIL_SNOWPIPE_TRANSACTION AUTO_INGEST = TRUE AS
COPY INTO AWS_DATABASE.PUBLIC.RETAIL_TXNS  -----COPY INTO TABLE NAME @STAGING NAME FOLLOWED BY BUCKET FOLDER NAME----
FROM '@retail_txns_aws_stage/retaildevelop/'
FILE_FORMAT = AWS_RETAIL_TXNXS_CSV ;

SHOW PIPES;

SELECT count(*) FROM RETAIL_TXNS;

--------------------------------------------------------PIPEREFRESH-------------------------------------------------------------

select * from RETAIL_TXNS;

alter pipe RETAIL_SNOWPIPE_TRANSACTION refresh;

SELECT SYSTEM$PIPE_STATUS('RETAIL_SNOWPIPE_TRANSACTION');