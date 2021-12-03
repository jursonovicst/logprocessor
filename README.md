# logprocessor

# Concept

TODO: describe

# Datafiles

## References:

* [varnishncsa logformat](https://varnish-cache.org/docs/6.0/reference/varnishncsa.html)
* [uaparser](http://)

## Fields

|Name|Abbrev|
|---|---|
|cn|cachename|
|pn|popname|
|h|host|
|c|coordinates|
|db|devicebrand|
|df|devicefamily|
|dm|devicemodel|
|of|osfamily|
|uf|uafamily|
|um|uamajor|
|p|path|
|lc|livechannel|
|cp|contentpackage|
|an|assetnumber|
|uid|uid|
|sid|sid|

## Anonymized logfiles:

`logname.bz2.ano-1.bz2.gpg`

|Name|Description|Modified|Anonymization|Unit|Example| |--|--|--|--|--|--| |timestamp|varnishncsa %t|no|shifted with
whole days||`2081-08-10 07:27:45`| |statuscode|varnishncsa %s|no|no||`200`| |method|varnishncsa %r|???|no||`GET`|
|protocol|varnishncsa %r|???|no||`HTTP/1.1`| |hit|%{Varnish:hitmiss}x|no|no||`hit`| |contenttype|"%{Content-Type}o"
|no|no||`text/xml`| |cachename|hostname of the cache node|no|substitution||`cachename-9f7407ab`| |popname|name of the
pop, in which the cache sits|no|substitution||`popname-ea30c95d`| |host|varnishncsa
%{Host}i|no|substitution||`host-3e4e7625b87a06b4`| |coordinates|lat:long coordinates based on a geoip lookup of client's
IP|long/lat rounded up to two decimal places (~1km precision in EU)|substituted||`coordinates-cade362a712f9a5e`|
|devicebrand|%{User-agent}i|uaparser|substitution||`devicebrand-a514a965`|
|devicefamily|%{User-agent}i|uaparser|substitution||`devicefamily-59342d25`|
|devicemodel|%{User-agent}i|uaparser|substitution||`devicemodel-90ce0a37`|
|osfamily|%{User-agent}i|uaparser|substitution||`osfamily-f5984c0b`|
|uafamily|%{User-agent}i|uaparser|substitution||`uafamily-4cd61238`| |uamajor|||||| |path|varnishncsa
%r|???|substituted||`path-518c4a144f6e7cfed8a3b6178349b36e`| |manifest|regexp match on path|no|no||`False`|
|fragment|regexp match on path|no|no||`False`| |livechannel|live TV channel number parsed from
path|no|substitution||`livechannel-7441b3f7`| |contentpackage|VoD asset identifier parsed from path|no|substitution|||
|assetnumber|VoD asset encoding version parsed from path|no|substitution||| |uid|id unique to a single
user|no|substitution||`uid-951276f2635c065d28507d06`| |sid|id uniq to a streaming session (from a single VoD play till
the end of that VoD)|no|substitution||`sid-d0753013b4d5b24dc6b3e8fb`| |contentlength|varnishncsa %b (be aware, this may
not equal to the bytes actually sent)|no|converted to xite |xite|`0.19897032101756512`| |timefirstbyte|varnishncsa
%{Varnish:time_firstbyte}x|no|no|seconds|`0.000193`| |timetoserv|varnishncsa %D|no|no|seconds|`0.000257`|

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




