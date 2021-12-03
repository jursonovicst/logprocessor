# logprocessor

# Concept

TODO: describe

# Datafiles

## References:

* [varnishncsa logformat](https://varnish-cache.org/docs/6.0/reference/varnishncsa.html)
* [uaparser](http://)

## Fields
uid,sid,livechannel,contentpackage,assetnumber,maxage,coordinates,devicebrand,devicefamily,devicemodel,osfamily,uafamily,uamajor,manifest,fragment
-,0,-,-,-,300,0,-,0,-,0,0,0,False,False

* NaN values are marked with `-`
* See References for the details of format codes in descriotion

|Name          |Type    |Unit  |Description|Anonymization|RAW example|Anonymized example|
|--------------|--------|------|-----------|-------------|-----------|------------------|
|timestamp     |datetime|      |%t         |shift with constatn number of weeks|2021-01-01 06:59:46|2088-01-08 06:59:46|
|statuscode    |int     |      |%s         |-            |200|200|
|contentlength |float   |xyte  |%b         |multiplication with a constant value|300kB|0.14285714285714285|
|host          |int     |      |%{Host}i   |subtitution. |livetv.cdn.telekom.de|3|
|timefirstbyte |float   |second|%{Varnish:time_firstbyte}x|-|0.01|0.01| 
|timetoserv    |float   |second|%D         |-            |0.123|0.123|
|hit           |str     |      |%{Varnish:hitmiss}|-|hit|hit|
|contenttype   |str     |      |%{Content-Type}o|-|application/json|application/json|
|cachecontrol  |str     |      |%{Cache-Control}o|-|Cache-Control:public,max-age=300|Cache-Control:public,max-age=300|
|cachename     |int     |      |cache's hostname|substitution|edge_cache_frankfurt|5|
|popname       |int     |      |cache's location|substitution|Frankfurt|3|
|method        |str     |      |%m         |-|GET|GET|
|protocol      |str     |      |%H         |-|HTTP/1.1|HTTP/1.1|
|path          |str     |      |%U%q       |substitution|/resource/index.html|656|


## Generated fields

|Name          |Type    |Unit  |Description|Anonymization|RAW example|Anonymized example|
|--------------|--------|------|-----------|-------------|-----------|------------------|
|uid           |str     |      |see session tracking|substitution|<telekom intern id>|45|
|sid           |str     |      |see session tracking|substitution|<telekom intern id>|42|
|livechannel   |str     |      |Live TV channel name|substitution|cnn|63|
|contentpackage|str     |      |Movie identifier|substitution|Return of the Jedi|22|
|assetnumber   |str     |      |Encoding variant of the content packages. It might be updated by every reencoding.|substitution|<telekom intern id>|65|
|maxage        |int     |second|parsed from %{Cache-Control}o|-|300|300|
|coordinates   |str     |      |long. and lat. of the client based on geoip lookup|substitution|8.454:46.444|6|
|devicebrand   |str     |      |uaparser on %{User-agent}i|substitution|Apple|3|
|devicefamily  |str     |      |uaparser on %{User-agent}i|substitution|Mac|32|
|devicemodel   |str     |      |uaparser on %{User-agent}i|substitution|Mac|5|
|osfamily      |str     |      |uaparser on %{User-agent}i|substitution|Mac OS X|7|
|uafamily      |str     |      |uaparser on %{User-agent}i|substitution|Chrome|9|
|uamajor       |str     |      |uaparser on %{User-agent}i|substitution|10|20|
|manifest      |bool    |      |regexp match on path|-|true|true|
|fragment      |bool    |      |regexp match on path|-|false|false|

## Session tracking
  
TODO: describe
  
## Anonymized logfiles:

`logname.bz2.ano-1.bz2.gpg`

``` csv
#timestamp,statuscode,contentlength,host,timefirstbyte,timetoserv,hit,contenttype,cachecontrol,cachename,popname,method,protocol,path,uid,sid,livechannel,contentpackage,assetnumber,maxage,coordinates,devicebrand,devicefamily,devicemodel,osfamily,uafamily,uamajor,manifest,fragment
2088-05-13 06:59:46,200,0.14285714285714285,0,0.000111,0.000161,hit,application/json,"Cache-Control:public,max-age=300",0,0,GET,HTTP/1.1,0,-,0,-,-,-,300,0,-,0,-,0,0,0,False,False
2088-05-13 06:59:46,200,15.079365079365079,0,8.2e-05,0.000119,hit,application/zip,"Cache-Control:public,max-age=300",0,0,GET,HTTP/1.1,1,-,0,-,-,-,300,1,-,0,-,0,0,0,False,False
2088-05-13 06:59:46,200,15.079365079365079,0,0.000143,0.000203,hit,application/zip,"Cache-Control:public,max-age=300",0,0,GET,HTTP/1.1,1,-,1,-,-,-,300,1,-,0,-,0,0,0,False,False
2088-05-13 06:59:46,200,25276.555555555555,1,0.000146,0.154427,hit,video/mp4,Cache-Control:max-age=14400,0,0,GET,HTTP/1.1,2,0,2,0,-,-,14400,1,0,1,0,1,1,-,False,False
2088-05-13 06:59:46,200,25276.555555555555,1,0.012388,0.25097,miss,video/mp4,Cache-Control:max-age=14400,0,0,GET,HTTP/1.1,2,-,3,0,-,-,14400,1,0,1,0,1,1,-,False,False
2088-05-13 06:59:46,200,15945.666666666666,1,0.000132,0.000662,hit,video/mp4,Cache-Control:max-age=14400,0,0,GET,HTTP/1.1,3,1,4,1,-,-,14400,1,0,1,0,1,2,1,False,False
2088-05-13 06:59:46,200,531.1587301587301,1,0.000125,0.0002,hit,video/mp4,Cache-Control:max-age=14400,0,0,GET,HTTP/1.1,4,1,4,1,-,-,14400,1,0,1,0,1,2,1,False,False
2088-05-13 06:59:46,200,15.079365079365079,0,0.000125,0.000179,hit,application/zip,"Cache-Control:public,max-age=300",0,0,GET,HTTP/1.1,1,-,5,-,-,-,300,2,-,0,-,0,0,0,False,False
2088-05-13 06:59:46,200,535.0952380952381,1,0.000128,0.000204,hit,video/mp4,Cache-Control:max-age=14400,0,0,GET,HTTP/1.1,5,2,6,2,-,-,14400,1,0,1,0,1,1,-,False,False
```

## Distance data:


`distances_150.csv`

|hash/hash|coord-01|...     |coord-i |...|coord-n|
|-------------|--------|--------|--------|---|-------|
|**coord-01** |        |        |        |   |       |
|**...**      |        |        |        |   |       |
|**
coord-j**  |        |        | NaN if distance(coord-i,coord-j) >150km<br/>distance(coord-i,coord-j) with 5km precision|   |       |
|**...**      |        |        |        |   |       |
|**coord-n**  |        |        |        |   |       |

## Delivery services:

`ds.csv`

Description of the different delivery services (identified by the host header). `Garbage` signals an unauthorized or
DDoS attack attempt (for
Example: `%22%3E%3Cscript%3Ealert('Qualys_XSS_Joomla_2.5.3')%3C%2Fscript%3E,host-cd8518fbd7c5dfd3`)




