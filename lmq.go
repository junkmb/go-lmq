package lmq

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

var DefaultTimeout = 10 * time.Second

type Client interface {
	Push(string, string, io.Reader) (*PushResponse, error)
	PushAll(string, string, io.Reader) (map[string]*PushResponse, error)
	Pull(string, time.Duration) (*Message, error)
	PullAny(string, time.Duration) (*Message, error)
	Reply(*Message, ReplyType) error
	Delete(string) error
	GetProperty(string) (*Property, error)
	UpdateProperty(string, *Property) error
	DeleteProperty(string) error
	GetDefaultProperty() ([]*DefaultProperty, error)
	SetDefaultProperty([]*DefaultProperty) error
	DeleteDefaultProperty() error
}

type ReplyType int

const (
	ReplyAck ReplyType = iota
	ReplyNack
	ReplyExt
)

func (r ReplyType) String() string {
	switch r {
	case ReplyAck:
		return "ack"
	case ReplyNack:
		return "nack"
	case ReplyExt:
		return "ext"
	}
	panic("unreach")
}

type PushResponse struct {
	Accum string `json:"accum"`
}

type client struct {
	url string
	c   *http.Client
	pc  map[time.Duration]*http.Client
	pcm sync.Mutex
}

func New(url string) Client {
	return &client{
		url: strings.TrimRight(url, "/"),
		c:   newHTTPClient(0),
		pc:  make(map[time.Duration]*http.Client),
	}
}

func newHTTPClient(timeout time.Duration) *http.Client {
	c := &http.Client{}
	if timeout >= 0 {
		c.Timeout = timeout + DefaultTimeout
	}
	return c
}

func (c *client) Push(queue, bodyType string, body io.Reader) (*PushResponse, error) {
	var r PushResponse
	url := "/messages/" + queue
	err := c.push(url, bodyType, body, &r)
	return &r, err
}

func (c *client) PushAll(queue string, bodyType string, body io.Reader) (map[string]*PushResponse, error) {
	if _, err := regexp.Compile(queue); err != nil {
		return nil, err
	}
	var r map[string]*PushResponse
	url := "/messages?qre=" + queue
	err := c.push(url, bodyType, body, &r)
	return r, err
}

func (c *client) push(url, bodyType string, body io.Reader, r interface{}) error {
	resp, err := c.do("POST", url, bodyType, body)
	if err != nil {
		return err
	}
	defer discard(resp.Body)
	if err := checkStatus(resp, http.StatusOK); err != nil {
		return err
	}
	return json.NewDecoder(resp.Body).Decode(&r)
}

func (c *client) Pull(queue string, timeout time.Duration) (*Message, error) {
	url := fmt.Sprintf("/messages/%s?cf=msgpack", queue)
	return c.pull(url, timeout)
}

func (c *client) PullAny(queue string, timeout time.Duration) (*Message, error) {
	if _, err := regexp.Compile(queue); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("/messages?qre=%s&cf=msgpack", queue)
	return c.pull(url, timeout)
}

func (c *client) pull(url string, timeout time.Duration) (*Message, error) {
	if timeout < 0 {
		timeout = -1
	} else {
		url += fmt.Sprintf("&t=%d", int(timeout.Seconds()))
	}
	resp, err := do(c.pullClient(timeout), "GET", c.url+url, "", nil)
	if err != nil {
		return nil, err
	}
	defer discard(resp.Body)
	if err := checkStatus(resp, http.StatusOK); err != nil {
		return nil, err
	}
	return newMessage(resp)
}

func (c *client) pullClient(timeout time.Duration) *http.Client {
	c.pcm.Lock()
	defer c.pcm.Unlock()
	client, ok := c.pc[timeout]
	if !ok {
		client = newHTTPClient(timeout)
		c.pc[timeout] = client
	}
	return client
}

func (c *client) Reply(m *Message, r ReplyType) error {
	url := fmt.Sprintf("/messages/%s/%s?reply=%v", m.Queue, m.ID, r)
	resp, err := c.do("POST", url, "", nil)
	if err != nil {
		return err
	}
	defer discard(resp.Body)
	return checkStatus(resp, http.StatusNoContent)
}

func (c *client) Delete(queue string) error {
	return c.delete("/queues/" + queue)
}

func (c *client) delete(url string) error {
	resp, err := c.do("DELETE", url, "", nil)
	if err != nil {
		return err
	}
	defer discard(resp.Body)
	return checkStatus(resp, http.StatusNoContent)
}

func (c *client) do(method, url, bodyType string, body io.Reader) (*http.Response, error) {
	return do(c.c, method, c.url+url, bodyType, body)
}

func do(c *http.Client, method, url, bodyType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	if bodyType != "" {
		req.Header.Set("Content-Type", bodyType)
	}
	escapeQueueName(req)
	return c.Do(req)
}

func escapeQueueName(r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	n := len(parts)
	switch {
	case strings.Contains(r.URL.RawQuery, "reply="):
		if n >= 5 {
			name := strings.Join(parts[2:n-1], "%2F")
			r.URL.Opaque = fmt.Sprintf("//%s/%s/%s/%s", r.URL.Host, parts[1], name, parts[n-1])
		}
	case n >= 4:
		name := strings.Join(parts[2:], "%2F")
		r.URL.Opaque = fmt.Sprintf("//%s/%s/%s", r.URL.Host, parts[1], name)
	}
}

func checkStatus(resp *http.Response, expected int) error {
	if resp.StatusCode == expected {
		return nil
	}
	return newError(resp)
}

func discard(r io.ReadCloser) {
	io.Copy(ioutil.Discard, r)
	r.Close()
}

type Error struct {
	Code    int
	Message string
}

func newError(resp *http.Response) *Error {
	b, _ := ioutil.ReadAll(resp.Body)
	return &Error{
		Code:    resp.StatusCode,
		Message: string(b),
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("LMQ error: %d %s", e.Code, e.Message)
}

func (e *Error) IsEmpty() bool {
	return e.Code == http.StatusNoContent
}
