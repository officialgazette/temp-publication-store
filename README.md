# Overview

The API to access the SHAB archiving system is implemented using HTTP based REST. If not otherwise
specified, JSON objects are used to send and receive data from and to the archive, therefore the content
type when issuing requests should be set to application/json.

## API

### Store

The store requests allows documents to be put into the archive. In order to be able to store a consistent
package containing the metadata and the actual PDF document, a multipart/form-data PUT request is
required to store the documents. The current implementation does not allow for an existing document with
the same ID to be updated or replaced.

URL
```
/api/v1/document/:tenant/:type/:id
```

Method
```
PUT
```
URL Parameters

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| tenant | string | Yes | | Tenant to retrieve document from |
| id | string | Yes | | ID of the document to store |
| type | string | | notice | notice Document type (notice or issue) |

Data Parameters

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
