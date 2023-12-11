CREATE OR REPLACE DATABASE AZUREDATABASE;
USE AZUREDATABASE;

CREATE OR REPLACE TABLE TRANSACTION_RAW 
(household_key	INT,
BASKET_ID	INT,
DAY	INT,
PRODUCT_ID	INT,
QUANTITY	INT,
SALES_VALUE	FLOAT,
STORE_ID	INT,
RETAIL_DISC	FLOAT,
TRANS_TIME	INT,
WEEK_NO	INT,
COUPON_DISC	INT,
COUPON_MATCH_DISC INT
--FOREIGN KEY (PRODUCT_ID) references PRODUCT_RAW(PRODUCT_ID),
--FOREIGN KEY (household_key) references demographic_RAW(household_key)
);

 create or replace file format RETAIL_TRNXS_CSV
 type = 'csv'
 compression = 'none'
 field_delimiter = ','
 field_optionally_enclosed_by = 'none'
 skip_header = 1 ;


CREATE OR REPLACE NOTIFICATION INTEGRATION RETAIL_TXNS_INTEGRATION
ENABLED = TRUE
TYPE= QUEUE
NOTIFICATION_PROVIDER= AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI='https://vickyretailfilestorage.queue.core.windows.net/retail-txnn-queue'
AZURE_TENANT_ID='651b2858-0772-4768-b5eb-f8126b8447cf';

SHOW INTEGRATIONS;

DESC NOTIFICATION INTEGRATION RETAIL_TXNS_INTEGRATION;


CREATE OR REPLACE STAGE TRANSACTION_STAGE
url = 'azure://vickyretailfilestorage.blob.core.windows.net/retail-txnn-container/'
credentials = (azure_sas_token='?sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2023-11-24T13:09:05Z&st=2023-11-24T05:09:05Z&spr=https&sig=c1zZVCKUmXRLYXivcTfaW4%2B7NlPUOIRnYlcZvzovl%2Bo%3D');

SHOW STAGES;
LS @TRANSACTION_STAGE;

CREATE OR REPLACE PIPE "RETAIL_TRANSACTION_PIPE"
 auto_ingest = true
 integration = 'RETAIL_TXNS_INTEGRATION'
 as
 copy into TRANSACTION_RAW
 from @TRANSACTION_STAGE
 file_format = RETAIL_TRNXS_CSV ;
 
SHOW PIPES;
SELECT count(*) FROM TRANSACTION_RAW;

SNOWFLAKE AUTO DATA INGESTION FROM AZURE BLOB STORAGE SCRIPT
SELECT * FROM TRANSACTION_RAW ;

ALTER PIPE RETAIL_TRANSACTION_PIPE REFRESH;

select *
from table(information_schema.copy_history (table_name=> 'TRANSACTION_RAW',
start_time=> dateadd (hours, -1, current_timestamp())));

 


