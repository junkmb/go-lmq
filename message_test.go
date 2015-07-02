package lmq

import (
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func must(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewMessage(t *testing.T) {
	resp := &http.Response{
		Header: make(http.Header),
		Body:   ioutil.NopCloser(strings.NewReader("body")),
	}
	resp.Header.Set("X-Lmq-Message-Id", "abc")
	resp.Header.Set("X-Lmq-Queue-Name", "q")
	resp.Header.Set("X-Lmq-Message-Type", "normal")
	resp.Header.Set("Content-Type", "text/plain")

	m, err := newMessage(resp)
	assert.Nil(t, err)
	assert.Equal(t, "abc", m.ID)
	assert.Equal(t, "q", m.Queue)
	assert.Equal(t, "normal", m.MessageType)
	assert.Equal(t, -1, m.Retry)
	assert.Equal(t, "text/plain", m.ContentType)
	assert.Equal(t, []byte("body"), m.Body)

	resp.Body = ioutil.NopCloser(strings.NewReader("body"))
	resp.Header.Set("X-Lmq-Retry-Remaining", "1")
	m, err = newMessage(resp)
	assert.Nil(t, err)
	assert.Equal(t, 1, m.Retry)
}

func TestDecodeMsgpack(t *testing.T) {
	m := &Message{
		MessageType: "normal",
		ContentType: "application/x-msgpack",
		Body:        []byte{0x81, 0xa2, 0x49, 0x44, 0x01},
	}
	var v struct {
		ID int
	}
	must(t, m.Decode(&v))
	assert.Equal(t, 1, v.ID)
	assert.Equal(t, EOF, m.Decode(&v))
}

func TestDecodeJSON(t *testing.T) {
	m := &Message{
		MessageType: "normal",
		ContentType: "application/json",
		Body:        []byte(`{"ID":1}`),
	}
	var v struct {
		ID int
	}
	must(t, m.Decode(&v))
	assert.Equal(t, 1, v.ID)
}

func TestDecodeText(t *testing.T) {
	m := &Message{MessageType: "normal", ContentType: "text/plain", Body: []byte("hello")}
	var v interface{}
	must(t, m.Decode(&v))
	assert.Equal(t, "hello", v)
}

func TestDecodeRaw(t *testing.T) {
	m := &Message{MessageType: "normal", ContentType: "application/octet-stream", Body: []byte("hello")}
	var b []byte
	must(t, m.Decode(&b))
	assert.Equal(t, []byte("hello"), b)

	m = &Message{MessageType: "normal", ContentType: "application/octet-stream", Body: []byte("world")}
	b = b[:2]
	must(t, m.Decode(b))
	assert.Equal(t, []byte("wo"), b)

	m = &Message{MessageType: "normal", ContentType: "application/octet-stream", Body: []byte("world")}
	must(t, m.Decode(&b))
	assert.Equal(t, []byte("world"), b)

	m = &Message{MessageType: "normal", ContentType: "application/octet-stream", Body: []byte("interface")}
	var v interface{}
	must(t, m.Decode(&v))
	assert.Equal(t, []byte("interface"), v)
}

func TestCompound(t *testing.T) {
	bin := []byte{
		0x92, 0x92, 0x81, 0xac, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65,
		0xb0, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x6a, 0x73, 0x6f,
		0x6e, 0xc4, 0x08, 0x7b, 0x22, 0x49, 0x44, 0x22, 0x3a, 0x31, 0x7d, 0x92, 0x81, 0xac, 0x63, 0x6f,
		0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0xb0, 0x61, 0x70, 0x70, 0x6c, 0x69,
		0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x6a, 0x73, 0x6f, 0x6e, 0xc4, 0x08, 0x7b, 0x22, 0x49,
		0x44, 0x22, 0x3a, 0x32, 0x7d,
	}
	m := &Message{MessageType: "compound", Body: bin}
	var v struct{ ID int }
	must(t, m.Decode(&v))
	assert.Equal(t, 1, v.ID)
	must(t, m.Decode(&v))
	assert.Equal(t, 2, v.ID)
	assert.Equal(t, EOF, m.Decode(&v))
}
