// Package nectarutil contains tools for use with nectar.
package nectarutil

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// Mkquery builds a URL query string from the ? onward based on the
// parameter=[value] items given in the map; an empty map will return an empty
// string.
func Mkquery(parameters map[string]string) string {
	query := ""
	for k, v := range parameters {
		query += url.QueryEscape(k) + "=" + url.QueryEscape(v) + "&"
	}
	if query != "" {
		return "?" + strings.TrimRight(query, "&")
	}
	return ""
}

// ResponseStub returns a fake response with the given info.
//
// Note: The Request field of the returned response will be nil; you may want
// to set the Request field if you have a specific request to reference.
func ResponseStub(statusCode int, body string) *http.Response {
	bodyBytes := []byte(body)
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		StatusCode:    statusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          ioutil.NopCloser(bytes.NewBuffer(bodyBytes)),
		ContentLength: int64(len(bodyBytes)),
		Header:        http.Header{"Content-Length": {fmt.Sprintf("%d", len(bodyBytes))}, "Content-Type": {"text/plain"}},
	}
}

// StubResponse returns a standalone response with the detail from the original
// response; the full body will be read into memory and the original response's
// Body closed. This is used to allow the response to complete and close so the
// transport can be used for another request/response.
//
// Note: Any error reading the original response's body will be ignored.
//
// Note: The Request field of the returned response will be nil; you may want
// to set the Request field if you have a specific request to reference.
func StubResponse(resp *http.Response) *http.Response {
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	header := make(http.Header, len(resp.Header))
	for headerName, headerValues := range resp.Header {
		copiedHeaderValues := make([]string, len(headerValues))
		for headerValueIndex, headerValue := range headerValues {
			copiedHeaderValues[headerValueIndex] = headerValue
		}
		header[headerName] = copiedHeaderValues
	}
	return &http.Response{
		Status:        resp.Status,
		StatusCode:    resp.StatusCode,
		Proto:         resp.Proto,
		ProtoMajor:    resp.ProtoMajor,
		ProtoMinor:    resp.ProtoMinor,
		Body:          ioutil.NopCloser(bytes.NewBuffer(bodyBytes)),
		ContentLength: int64(len(bodyBytes)),
		Header:        header,
	}
}
