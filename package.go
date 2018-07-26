// Package nectar defines client tools for Hummingbird.
package nectar

import (
	"io"
	"net/http"
)

// Client is an API interface to CloudFiles.
type Client interface {
	GetURL() string
	PutAccount(headers map[string]string) *http.Response
	PostAccount(headers map[string]string) *http.Response
	// GetAccount reads the body of the response and converts it into a
	// []*ContainerRecord while also returning the response instance itself.
	GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]*ContainerRecord, *http.Response)
	GetAccountRaw(marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) *http.Response
	HeadAccount(headers map[string]string) *http.Response
	DeleteAccount(headers map[string]string) *http.Response
	PutContainer(container string, headers map[string]string) *http.Response
	PostContainer(container string, headers map[string]string) *http.Response
	// GetContainer reads the body of the response and converts it into an
	// []*ObjectRecord while also returning the response instance itself.
	GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) ([]*ObjectRecord, *http.Response)
	GetContainerRaw(container string, marker string, endMarker string, limit int, prefix string, delimiter string, reverse bool, headers map[string]string) *http.Response
	HeadContainer(container string, headers map[string]string) *http.Response
	DeleteContainer(container string, headers map[string]string) *http.Response
	PutObject(container string, obj string, headers map[string]string, src io.Reader) *http.Response
	PostObject(container string, obj string, headers map[string]string) *http.Response
	GetObject(container string, obj string, headers map[string]string) *http.Response
	HeadObject(container string, obj string, headers map[string]string) *http.Response
	DeleteObject(container string, obj string, headers map[string]string) *http.Response
	Raw(method, urlAfterAccount string, headers map[string]string, body io.Reader) *http.Response
	SetUserAgent(string)
}

// ContainerRecord is an entry in an account listing.
type ContainerRecord struct {
	Count int64  `json:"count"`
	Bytes int64  `json:"bytes"`
	Name  string `json:"name"`
}

// *ObjectRecord is an entry in a container listing.
type ObjectRecord struct {
	Hash         string `json:"hash"`
	LastModified string `json:"last_modified"`
	Bytes        int    `json:"bytes"`
	Name         string `json:"name"`
	ContentType  string `json:"content_type"`
	Subdir       string `json:"subdir"`
}

// ClientToken is an extension to the Client interface allowing the retrieval
// of the usually internal authentication token, usually for debugging
// purposes.
type ClientToken interface {
	GetToken() string
}
