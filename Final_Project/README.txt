These are the SQL queries and some bq command lines:

1) Web UI

SQL queries:

CDNUsage.sql:

SELECT cdn, num, ROUND(ratio*100) as percent FROM (
  SELECT cdn, COUNT(cdn) as num, RATIO_TO_REPORT(num) OVER() ratio FROM (
     SELECT CASE 
       WHEN _cdn_provider IN ('')
         THEN 'None'
         ELSE 'CDN'
       END as cdn
      FROM httparchive:runs.2015_12_01_requests,  
           httparchive:runs.2015_11_15_requests,
           httparchive:runs.2015_11_01_requests,  
           httparchive:runs.2015_10_15_requests,
           httparchive:runs.2015_12_01_requests_mobile,
           httparchive:runs.2015_11_15_requests_mobile,
           httparchive:runs.2015_11_01_requests_mobile,
           httparchive:runs.2015_10_15_requests_mobile
   ) GROUP BY cdn
) ORDER BY percent DESC


CDNProvider.sql:

SELECT provider, ROUND(100*ratio) AS percent, num FROM (
  SELECT
    _cdn_provider AS provider, COUNT(*) AS num, RATIO_TO_REPORT(num) OVER() ratio
  FROM
    httparchive:runs.2015_12_01_requests,
    httparchive:runs.2015_11_15_requests,
    httparchive:runs.2015_11_01_requests,
    httparchive:runs.2015_10_15_requests,
    httparchive:runs.2015_12_01_requests_mobile,
    httparchive:runs.2015_11_15_requests_mobile,
    httparchive:runs.2015_11_01_requests_mobile,
    httparchive:runs.2015_10_15_requests_mobile
  WHERE
    _cdn_provider != ''
  GROUP BY provider )
ORDER BY num DESC
LIMIT 10


HostRankInGoogle.sql:

SELECT req_host, COUNT(req_host) AS num
FROM
  httparchive:runs.2015_12_01_requests,
  httparchive:runs.2015_11_15_requests,
  httparchive:runs.2015_11_01_requests,
  httparchive:runs.2015_10_15_requests,
  httparchive:runs.2015_12_01_requests_mobile,
  httparchive:runs.2015_11_15_requests_mobile,
  httparchive:runs.2015_11_01_requests_mobile,
  httparchive:runs.2015_10_15_requests_mobile
WHERE
  _cdn_provider = 'Google'
GROUP BY req_host
ORDER BY num DESC
LIMIT 20


2) 'bq' command line

hqiu@bos-mpdei>> bq shell
Welcome to BigQuery! (Type help for more information.)
friendly-path-115605> ls
      datasetId      
 ------------------- 
  my_children_names 


hqiu@bos-mpdei>> bq ls -p
       projectId         friendlyName  
 ---------------------- -------------- 
  friendly-path-115605   My Project 
  
  
hqiu@bos-mpdei>> bq show publicdata:samples.shakespeare
Table publicdata:samples.shakespeare

   Last modified                  Schema                 Total Rows   Total Bytes   Expiration  
 ----------------- ------------------------------------ ------------ ------------- ------------ 
  26 Aug 17:43:49   |- word: string (required)           164656       6432064                   
                    |- word_count: integer (required)                                           
                    |- corpus: string (required)                                                
                    |- corpus_date: integer (required)  
                    
    
hqiu@bos-mpdei>> bq query "SELECT word, COUNT(word) as count FROM publicdata:samples.shakespeare WHERE word CONTAINS 'raisin' GROUP BY word"
Waiting on bqjob_r38d32b35230ce865_00000151927b78af_1 ... (0s) Current status: DONE    
+---------------+-------+
|     word      | count |
+---------------+-------+
| raising       |     5 |
| dispraising   |     2 |
| Praising      |     4 |
| praising      |     7 |
| dispraisingly |     1 |
| raisins       |     1 |
+---------------+-------+


hqiu@bos-mpdei>> bq mk my_children_names_bq
Dataset 'friendly-path-115605:my_children_names_bq' successfully created.
hqiu@bos-mpdei>> bq ls
       datasetId        
 ---------------------- 
  my_children_names     
  my_children_names_bq 
  
  
hqiu@bos-mpdei>> bq load --source_format CSV my_children_names_bq.my_data_set_bq gs://hqiu_big_query_bucket/yob1880.txt name:string,gender:string,count:integer  
Waiting on bqjob_r539a33511ffd9950_000001519294ae02_1 ... (34s) Current status: DONE


hqiu@bos-mpdei>> bq ls my_children_names_bq
     tableId       Type   
 ---------------- ------- 
  my_data_set_bq   TABLE  
  

hqiu@bos-mpdei>> bq show my_children_names_bq.my_data_set_bq
Table friendly-path-115605:my_children_names_bq.my_data_set_bq

   Last modified         Schema         Total Rows   Total Bytes   Expiration  
 ----------------- ------------------- ------------ ------------- ------------ 
  11 Dec 14:47:07   |- name: string     2000         37400                     
                    |- gender: string                                          
                    |- count: integer    
                    
                    
hqiu@bos-mpdei>> bq query "SELECT name,count FROM my_children_names_bq.my_data_set_bq WHERE gender = 'F' ORDER BY count DESC LIMIT 5"
Waiting on bqjob_r317289c0b8af0424_000001519297591d_1 ... (0s) Current status: DONE    
+-----------+-------+
|   name    | count |
+-----------+-------+
| Mary      |  7065 |
| Anna      |  2604 |
| Emma      |  2003 |
| Elizabeth |  1939 |
| Minnie    |  1746 |
+-----------+-------+
                



