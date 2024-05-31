# Overview
> [!IMPORTANT]
> The term "archive" is not used consistently in the context of the official gazette portal. In the given context, "archive" is to be considered a **temporary file repository** rather than a long-term archive with a legally binding character. The term is to be distinguished from the archives of the respective publishers of official gazettes. These can obtain publications via API compliant with the [SIP standard](https://www.ech.ch/de/ech/ech-0160/1.0)

## Purpose of the Archive
xxx

## API
The API to access the SHAB archiving system is implemented using HTTP based REST. If not otherwise
specified, JSON objects are used to send and receive data from and to the archive, therefore the content
type when issuing requests should be set to application/json.

### Store

The store requests allows documents to be put into the archive. In order to be able to store a consistent
package containing the metadata and the actual PDF document, a multipart/form-data PUT request is
required to store the documents. The current implementation does not allow for an existing document with
the same ID to be updated or replaced.

**URL**
```
/api/v1/document/:tenant/:type/:id
```

**Method**
```
PUT
```
**URL Parameters**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| tenant | string | Yes | | Tenant to retrieve document from |
| id | string | Yes | | ID of the document to store |
| type | string | | notice | notice Document type (notice or issue) |

**Data Parameters**

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| canton | string | | | The canton the document applies to (if applicable) |
| cantons | string[] | ||List of cantons the document applies to (if applicable) |
| heading | string | for notices only | The heading of the document (e.g. hr) |
| issue | integer | || The issue number of the SHAB paper publication |
| language | string | for notices only ||The language code in ISO 639-1 format (e.g. de)|
|notice | string ||| The notice text (will be generated from pdf otherwise)|
|publicationTime | date |Yes|| The publication date and time in ISO 8601 format|
| publicUntil | date | || The date and time in ISO 8601 format until this document  is public (if omitted, the document is public indefinitely)|
| subheading | string | for notices only|| The subheading of the document (e.g. hr01)|
|submitter | string | for notices only | | The name of the submitter of the document|
| title | string | for notices only || The title of the document |

The date only form of the ISO 8601 date and time format is allowed (YYYY-MM-DD) and will be
converted to the full format. If the time zone is omitted, CET/CEST time zone is used:
```
publicationTime YYYY-MM-DDT00:00:00.000
publicUntil YYYY-MM-DDT23:59:59.999
```

**Request Example**

The following example describes a complete request body using the parameters previously explained 
for an initial search query: 
The following example describes storing a notice document into the archive:


> HTTP
```
PUT /api/v1/document/shab/1234567 
```

> JSON
```
{ "cantons" : [ "zh" ], 
 "heading" : "hr", 
 "subheading" : "hr01", 
 "submitter" : "Example submitter", 
 "language" : "de", 
 "issue" : 1, 
 "title" : "Example notice title", 
 "notice" : "Example notice text...", 
 "publicationTime" : "2017-01-01", 
 "publicUntil" : "2019-12-31" 
}
```

The following example describes storing an issue document into the archive: 


> HTTP 
```
PUT /api/v1/document/shab/issue/2017-01-03
```

> JSON
```
{ "language" : "de", 
 "issue" : 1, 
 "publicationTime" : "2017-01-01", 
 "publicUntil" : "2019-12-31", 
}
```

The requests needs to be sent as an multipart/form-data with the first content being the JSON 
object similar to the one shown above using the content type application/json encoded in UTF8. 
The second content needs to be the PDF document itself using the content type application/pdf. 

> CURL
```
curl –X PUT -H 'Content-Type: multipart/form-data' –F 'data={"canton":"zh", 
 "heading":"hr","issue":1,"language":"de","notice":"Example notice text...", 
 "publicationTime":"2017-01-03","subheading":"HR01","submitter":"Example submitter", 
 "title":"Example notice title"};type=application/json' \ 
–F 'pdf=@file;type=application/pdf' \ 
http://archive/api/v1/document/shab/1234567
```

> CURL 
```
curl –X PUT -H 'Content-Type: multipart/form-data' –F 'data={ "issue":1, 
 "language":"de","publicationTime":"2017-01-03"};type=application/json' \ 
–F 'pdf=@file;type=application/pdf' \ 
https://archive:8443/api/v1/document/shab/issue/2017-01-03
```

**Success Response**

Code: 201 

> JSON 
```
{ "message" : "Document 1234567 successfully stored" } 
```

**Error Response** 
Standard HTTP error codes are used to describe errors. The body will contain an error description 
object if possible.

Code: 4xx / 5xx

The following example describes an error if a document with the same ID already exists: 
Code: 405 
> JSON 
```
{ "error" : "Document 1234567 already exists" }
```

The following example describes an error if a semantically error has been found in the request: 
Code: 422 

>JSON 
```
{ "error" : "Unknown field: missingField" }
```
The following example describes an error if the required PDF data is not contained in the request: 
Code: 422 

>JSON
```
{ "error" : "Document 1234567 has no PDF attached" }
```

### Query
There are two main query requests available. Either to fetch a known document by specifying its ID or to 
get a list of documents matching a specified search expression. Both queries are available for internal 
usage and for public usage trough a dedicated URL path. The public queries are limited by restrictions 
which disallows searching or fetching documents which are not public anymore (publicUntil is lower 
than now). 

**Document**
The document query enables the retrieval of a specific document from the archive either as a JSON object 
which also contains details of the archiving process and signature or as a PDF. 

**URL**

For public queries with forced restrictions: 

```
/api/v1/public/document/:tenant/:type/:id 
/api/v1/public/document/:tenant/:type/:id.pdf 
/api/v1/public/document/:tenant/issue/:year/:issue 
/api/v1/public/document/:tenant/issue/:year/:issue.pdf 
/api/v1/public/document/:tenant/issue/latest 
/api/v1/public/document/:tenant/issue/latest.pdf
```
For internal queries without restrictions: 
```
/api/v1/document/:tenant/:type/:id 
/api/v1/document/:tenant/:type/:id.pdf 
/api/v1/document/:tenant/issue/:year/:issue 
/api/v1/document/:tenant/issue/:year/:issue.pdf 
/api/v1/document/:tenant/issue/latest 
/api/v1/document/:tenant/issue/latest.pdf
```

**Method**
GET 

**URL Parameters** 

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| tenant | string | Yes | | Tenant to retrieve document from |
| type | string | | notice | notice Document type (notice or issue) |
| id | string | Yes | | ID of the document to store |
| year | string | Year of published issue |
| issue | string | Issue | number of published issue | 


**Data Parameters** 

None. 

**Request Example** 

The following example describes fetching the document as a JSON object: 

>HTTP
```
GET /api/v1/document/shab/XX-YY01-0123456789 
GET /api/v1/document/shab/notice/1234567 
GET /api/v1/document/kabzh/issue/2017-01-06
GET /api/v1/document/kabzh/issue/2017/13 
GET /api/v1/document/kabzh/issue/latest 
```

>CURL
```
curl http://archive/api/v1/document/shab/XX-YY01-0123456789 
curl http://archive/api/v1/document/shab/notice/1234567 
curl http://archive/api/v1/document/kabzh/issue/2017-01-06 
curl http://archive/api/v1/document/kabzh/issue/2017/13 
curl http://archive/api/v1/document/kabzh/issue/latest
```

The following example describes fetching the document as a PDF: 

>HTTP
```
GET /api/v1/document/shab/XX-YY01-0123456789.pdf 
GET /api/v1/document/shab/notice/1234567.pdf 
GET /api/v1/document/kabzh/issue/2017-01-06.pdf 
GET /api/v1/document/kabzh/issue/2017/13.pdf 
GET /api/v1/document/kabzh/issue/latest.pdf
```

>CURL
``` 
curl -O http://archive/api/v1/document/shab/XX-YY01-0123456789.pdf 
curl –O http://archive/api/v1/document/shab/notice/1234567.pdf 
curl –O http://archive/api/v1/document/kabzh/issue/2017-01-06.pdf 
curl –O http://archive/api/v1/document/kabzh/issue/2017/13.pdf 
curl –O http://archive/api/v1/document/kabzh/issue/latest.pdf
```
**Success Response**

Only the structure of a request to a document as a JSON object is described here because requesting 
a PDF will return a PDF document in the body of the response: 

Code: 200 

When requesting a notice: 

>JSON 
```
{ "id" : "1234567", 
 "tenants" : [ "shab" ], 
 "cantons" : [ "zh" ], 
 "heading" : "hr", 
 "subheading" : "hr01", 
 "submitter" : "Example submitter", 
 "language" : "de", 
 "issue" : 1, 
 "title" : "Example notice title", 
 "notice" : "Example notice text...", 
 "publicationTime" : "2017-01-03T01:00:00.000Z", 
 "publicUntil" : "2020-01-02T22:59:59.999Z", 
 "archiveTime" : "2017-01-03T15:22:21.421Z" 
}
```
When requesting an issue: 

>JSON
```
{ "id" : "2017-01-06", 
 "tenants" : [ "shab" ], 
 "language" : "de", 
 "issue" : 1, 
 "publicationTime" : "2017-01-06T01:00:00.000Z",
 "publicUntil" : "2020-01-05T22:59:59.999Z",
 "archiveTime" : "2017-01-07T15:22:21.421Z" 
}
```

**Error Response**

Standard HTTP error codes are used to describe errors. The body will contain an error description 
object if possible.

Code: 4xx / 5xx

The following example describes an error if the requested document cannot be found:

Code: 404

>JSON
```
{ "error" : "Document with ID 1234567 not found" } 
{ "error" : "Tenant with name unknown not found" } 
```
**Search** 
The search query can be used to search through the archive using simple GET requests. 

**URL** 
For public queries with forced restrictions:

```
/api/v1/public/search/:tenant/:type 
/api/v1/public/search/:tenant/:type?parameter=value 
For internal queries without restrictions: 
/api/v1/search/:tenant/:type 
/api/v1/search/:tenant/:type?parameter=value
```

**Method** 

GET 

**URL Parameters**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| canton | string || Comma separated list and/or multiple occurrences |
| heading | string || Comma separated list and/or multiple occurrences |
| id | string || Exact match of the notice id (case insensitive) |
| notice | string || Full text search of the notice title and content |
| page | integer | 1 | The result page number to be fetched |
| pagesize | integer | 10 | The number of results per page to be fetched |
| publicationTime | date || The publication date and time in ISO 8601 format |
| publicationTime.from | date || The publication date and time in ISO 8601 format (beginning of range) |
| publicationTime.to | date | | The publication date and time in ISO 8601 format (end of range) |
| subheading | string || Comma separated list and/or multiple occurrences
| submitter | string || Full text search of the submitter | 
| tenant | string || Tenant to search documents in |
| title | string || The title of the document to search for |
| type | string || notice | Document type to search for (notice or issue) |


canton, heading and subheading can be specified multiple times or they can contain multiple values using a comma separated list (combination of both is also possible). A document needs to match one of canton and either one of heading or subheading (if specified) as well as all other specified criteria to be found. 
publicationTime and publicationTime.from/publicationTime.to are mutually exclusive. 
The date only form of the ISO 8601 date and time format is allowed (YYYY-MM-DD) and will be converted to the full format. If the time zone is omitted, CET/CEST time zone is used: 

```
publicationTime YYYY-MM-DDT00:00:00.000 YYYY-MM-DDT23:59:59.999 
publicationTime.from YYYY-MM-DDT00:00:00.000 
publicationTime.to YYYY-MM-DDT23:59:59.999
```

**Data Parameters**

None. 

**Request Example**

The following example describes the GET based search:

>CURL
```
curl http://archive/api/v1/search/shab?heading=hr&canton=be,zh&publicationTime.from= 
 2017-01-01&publicationTime.to=2017-01-31
```
**Success Response**

The following example describes a successful response to a search request. 

Code: 200 

When searching for notices: 

>JSON
```
{ "type" : "notice", 
 "page" : 1, 
 "pageCount" : 983, 
 "resultCount" : 9821, 
 "results" : [ 
 { "id" : "1234567", 
 "tenants" : [ "shab" ], 
 "heading" : "hr", 
 "subheading" : "hr01", 
 "submitter" : "EHRA-Import User", 
 "title" : "Example AG, Zürich", 
 "publicationTime" : "2017-01-03T01:00:00.000" 
 }, 
 { "id" : "1234566", 
 "tenants" : [ "shab" ], 
 "heading" : "hr", 
 "subheading" : "hr01", 
 "submitter" : "EHRA-Import User", 
 "title" : "Example GmbH, Bern", 
 "publicationTime" : "2017-01-03T01:00:00.000" 
 }, 
 …snip…
 ] 
}
```

When searching for issues: 

>JSON
```
{ "type" : "issue" 
 "page" : 1, 
 "pageCount" : 2, 
 "resultCount" : 12, 
 "results" : [ 
 { "tenants" : [ "shab" ], 
 "id" : "2017-01-03", 
 "publicationTime" : "2017-01-03T01:00:00.000" 
 }, 
 { "tenants" : [ "kabzh" ], 
 "id" : "2017-01-06", 
 "publicationTime" : "2017-01-06T01:00:00.000" 
 }, 
 …snip…
 ] 
}
```

If no results are found, the response will still be marked as a success but with an empty result set: 

>JSON
```
{ "type" : "notice" 
 "page" : 0, 
 "pageCount" : 0, 
 "resultCount" : 0, 
 "results" : [] 
}
```

**Error Response**

Standard HTTP error codes are used to describe errors. The body will contain an error description object if possible.

Code: 4xx / 5xx

The following example describes an error if a semantically error has been found in the request: 

Code: 422 

>JSON
```
{ "error" : "Unknown field: missingField" }
```

### Admin 
The admin namespace provides functionality for administration and internal functions. 

**Ping** 
The ping request is intended to be used by load balancers and monitoring tools for a quick health check. 

**URL** 

/api/v1/admin/ping 

**Method** 

GET 

**URL Parameters** 

None. 

**Data Parameters**

None.

**Request Example**

The following example describes executing a ping: 

>HTTP
```
GET /api/v1/admin/ping
```
 
>CURL
``` 
curl http://archive/api/v1/admin/ping
```

**Success Response**

Code: 200 

>JSON 

```
{ "ping" : "pong" }
```

**Error Response**

Standard HTTP error codes are used to describe errors. The body will contain an error description object if possible. 

Code: 4xx / 5xx

The following example describes an error if a document with the same ID already exists:

Code: 500 

>JSON
```
{ "error" : "The system is currently unavailable" }
```
