USE DATABASE DEMO_DATABASE;

create or replace table strings (v varchar(50));
insert into strings (v) values
    ('San Francisco'),
    ('San Jose'),
    ('Santa Clara'),
    ('Sacramento');
    
    select * from strings;
    
--Use wildcards to search for a pattern:
select v from strings
where v regexp 'San* [fF].*'
order by v;    

select v from string
where regexp '[[:alpha:]]';

select v, v regexp 'San\\b.*' as matches
    from strings
    order by v;

select regexp_count('It was the best of times, it was the worst of times', '\\bit\\b', 1,'i') as "result" from dual;


SELECT  TRIM(REGEXP_REPLACE(string, '[^[:digit:]]', ' ')) AS Numeric_value
FROM (SELECT ' Area code for employee ID 112244 is 12345.' AS string) a;

SELECT  TRIM(REGEXP_REPLACE(string, '[^[:alpha:]]', ' ')) AS alpha_value
FROM (SELECT ' Area code for employee ID 112244 is 12345.' AS string) a;

CREATE or replace TABLE demo3 (id INT, string1 VARCHAR);
INSERT INTO demo3 (id, string1) VALUES
    (5, 'A MAN A PLAN A CANAL')
    ;
    SELECT * FROM DEMO3;
    
select id, 
    regexp_substr(string1, 'A\\W+(\\w+)', 1, 1, 'e', 1) as "RESULT1",
    regexp_substr(string1, 'A\\W+(\\w+)', 1, 2, 'e', 1) as "RESULT2",
    regexp_substr(string1, 'A\\W+(\\w+)', 1, 3, 'e', 1) as "RESULT3",
    regexp_substr(string1, 'A\\W+(\\w+)', 1, 4, 'e', 1) as "RESULT4"
    from demo3;

/* Snowflake Regular Expression Functions
The regular expression functions are string functions that match a given regular expression. These functions are commonly called as a ‘regex’ functions.

Below are some of the regular expression function that Snowflake cloud data warehouse supports:

REGEXP_COUNT
REGEXP_INSTR
REGEXP_LIKE
REGEXP_SUBSTR
REGEXP_REPLACE
REGEXP
RLIKE */

/* Snowflake REGEXP_COUNT Function
The REGEXP_COUNT function searches a string and returns an integer that indicates the number of 
times the pattern occurs in the string. If no match is found, then the function returns 0.

syntax : REGEXP_COUNT( <string> , <pattern> [ , <position> , <parameters> ] ) */ 
select regexp_count('It was the best of times, it was the worst of times', '\\bit\\b', 1,'i') as "result" from dual;
select regexp_count('qqqabcrtrababcbcd', 'abc');
select regexp_count('qqqabcrtrababcbcd', '[cba]') as abc_character_count;
select REGEXP_COUNT('QQQABCRTRABABCBCD', '[CBA]{8}');

create or replace table overlap (id number, text string);
insert into overlap values (1,',abc,def,ghi,jkl,');
insert into overlap values (2,',abc,,def,,ghi,,jkl,');

select * from overlap;

--the count of first punctuation+ the count of other punctuatiuon+alphabaets is given as result
,abc, is 1st count 
def - not counted
,ghi, - 2nd counted
jkl, - is not counted - total 2

select id, regexp_count(text,'[[:punct:]][[:alnum:]]+[[:punct:]]', 1, 'i') from overlap;

select REGEXP_COUNT('QQQABCRTRABABCBCD', '[Q]{2}'); --1
select REGEXP_COUNT('QQQABCRTRABABCBCD', '[Q]{3}'); --1
select REGEXP_COUNT('QQQABCRTRABABCBCD', '[AB]{2}'); --ABSOLUTELY FINE
select REGEXP_COUNT('QQQABCRTRABABCBCD', '[T]{1}'); --ABSOLUTELY FINE

/*
The Snowflake REGEXP_REPLACE function returns the string by replacing specified pattern. 
If no matches found, original string will be returned.

Following is the syntax of the Regexp_replace function.

REGEXP_REPLACE( <string> , <pattern> [ , <replacement> , <position> , <occurrence> , <parameters> ] )

1. Extract date from a text string using Snowflake REGEXP_REPLACE Function
The REGEXP_REPLACE function is one of the easiest functions to get the required value when manipulating strings data.
Consider the below example to replace all characters except the date value. */

--For example, consider following query to return only user name.
select regexp_replace( 'anandjha2309@gmail.com', '@.*\\.(com)');
select regexp_replace( 'anandjha_1990@yahoo.com', '@.*\\.(com)');


select regexp_replace('Customers - (NY)','\\(|\\)','') as customers; --| is OR space and bracket
select regexp_replace( 'vk73742@gmail.co.in', '@.*\\.(co).(in)');



SELECT TRIM(REGEXP_REPLACE(string, '[a-z/-/A-Z/.]', ''))AS date_value 
FROM (SELECT 'My DOB is 04-12-1976.' AS string) a;

select regexp_replace('acd@gmail.com.IN','@.*');

select regexp_replace('firstname middlename lastname','(.*) (.*) (.*)','\\3, \\1 \\2') as "name sort" from dual;

create or replace table cities(city varchar(20));
insert into cities values
    ('Sacramento'),
    ('San Francisco'),
    ('San Jose'),
    (null);

SELECT * FROM cities;

select * from cities where regexp_like(city, 'san.*','i');

CREATE or replace TABLE demo1 (id INT, string1 VARCHAR);
INSERT INTO demo1 (id, string1) VALUES 
    (1, 'nevermore123, nevermore24, nevermore34567.')
    ;
select * from demo1;

select id, string1,
      --regexp_substr(string1, 'nevermore\\d') AS "SUBSTRING", 
      regexp_instr( string1, 'nevermore\\d') AS "POSITION"
    from demo1
    order by id;
    
select id, string1,
      --regexp_substr(string1, 'nevermore\\d', 5) AS "SUBSTRING", 
      regexp_instr( string1, 'nevermore\\d', 5) AS "POSITION"
    from demo1
    order by id;

--it is returning the position for the 3rd match of 'nevermore' starting from the first character
--nevermore1 = 12 + ,  + 12, = 25

select id, string1,
      --regexp_substr(string1, 'nevermore\\d', 1, 3) AS "SUBSTRING", 
      regexp_instr( string1, 'nevermore\\d', 1, 3) AS "POSITION"
    from demo1
    order by id;
    
    select id, string1,
       --regexp_substr(string1, 'nevermore\\d', 1, 3) AS "SUBSTRING", 
       regexp_instr( string1, 'nevermore\\d', 1, 3, 0) AS "START_POSITION",
       regexp_instr( string1, 'nevermore\\d', 1, 3, 1) AS "AFTER_POSITION"
    from demo1
    order by id;

-- 2. Extract date using REGEXP_SUBSTR 
--Alternatively, REGEXP_SUBSTR function can be used to get date field from the string data. 

CREATE or replace TABLE demo2 (id INT, string1 VARCHAR);
INSERT INTO demo2 (id, string1) VALUES 
    -- A string with multiple occurrences of the word "the".
    (2, 'It was the best of times, it was the worst of times.'),
    -- A string with multiple occurrences of the word "the" and with extra
    -- blanks between words.
    (3, 'In    the   string   the   extra   spaces  are   redundant.'),
    -- A string with the character sequence "the" inside multiple words 
    -- ("thespian" and "theater"), but without the word "the" by itself.
    (4, 'A thespian theater is nearby.')
    ;

select * from demo2;

select id, regexp_substr(string1, 'the\\W+\\w+') as "RESULT"
    from demo2
    order by id;
    
    
    select id, string1, 
       regexp_substr(string1, 'the\\W+\\w+', 1, 2) as "SUBSTRING",
       regexp_instr(string1, 'the\\W+\\w+', 1, 2) as "POSITION"
    from demo2
    order by id;
    
select id, string1,
       regexp_substr(string1, 'the\\W+(\\w+)', 1, 2,    'e', 1) as "SUBSTRING"
       --regexp_instr( string1, 'the\\W+(\\w+)', 1, 2, 0, 'e', 1) as "POSITION"
    from demo2
    order by id;
    
--For example, consider the below example to get date value from a string containing text and the date. */
--SELECT REGEXP_SUBSTR('I am celebrating my birthday on 05/12/2020 this year','[0-9][0-9]//[0-9][0-9]//[0-9][0-9][0-9][0-9]') as dob;

-- 3. Validate if date is in a valid format using REGEXP_LIKE function
SELECT * FROM (SELECT '04-12-1976' AS string) a where REGEXP_LIKE(string,'\\d{1,2}\\-\\d{1,2}-\\d{4,4}');

--4. String pattern matching using REGEXP_LIKE
WITH tbl
  AS (select t.column1 mycol 
      from values('A1 something'),('B1 something'),('Should not be matched'),('C1 should be matched') t )

SELECT * FROM tbl WHERE regexp_like (mycol,'[a-zA-z]\\d{1,}[\\s0-9a-zA-Z]*');
--second word start with small s



/*
-- Snowflake REGEXP Function
The Snowflake REGEXP function is an alias for RLIKE.

Following is the syntax of the REGEXP function.

-- 1st syntax
REGEXP( <string> , <pattern> [ , <parameters> ] )

-- 2nd syntax
<string> REGEXP <pattern> */

--For example, consider following query to matches string with query.
SELECT city REGEXP 'M.*' 
FROM   ( 
              SELECT 'Bangalore' AS city 
              UNION ALL 
              SELECT 'Mangalore' AS city ) AS tmp;

/*
Snowflake RLIKE Function
The Snowflake RLIKE function is an alias for REGEXP and regexp_like.

Following is the syntax of the RLIKE function.

-- 1st syntax
RLIKE( <string> , <pattern> [ , <parameters> ] )

-- 2nd syntax
<string> RLIKE <pattern>
*/

--For example, consider following query to matches string with query.
SELECT city RLIKE 'M.*'
FROM   ( 
              SELECT 'Bangalore' AS city 
              UNION ALL 
              SELECT 'Mangalore' AS city ) AS tmp;














