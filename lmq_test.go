package lmq

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yosisa/go-lmq/lmqtest"
)

var lmqURL string

func TestEscapeQueueName(t *testing.T) {
	r, _ := http.NewRequest("GET", "http://localhost:9980/messages/q", nil)
	escapeQueueName(r)
	assert.Equal(t, "http://localhost:9980/messages/q", r.URL.String())

	r, _ = http.NewRequest("GET", "http://localhost:9980/messages/q/1", nil)
	escapeQueueName(r)
	assert.Equal(t, "http://localhost:9980/messages/q%2F1", r.URL.String())

	r, _ = http.NewRequest("GET", "http://localhost:9980/messages/q/1/id?reply=ack", nil)
	escapeQueueName(r)
	assert.Equal(t, "http://localhost:9980/messages/q%2F1/id?reply=ack", r.URL.String())
}

func TestSimple(t *testing.T) {
	queue := "TestSimple"
	c := New(lmqURL)
	defer c.Delete(queue)

	r, err := c.Push(queue, "text/plain", strings.NewReader("Hello LMQ"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "no", r.Accum)

	m, err := c.Pull(queue, 0)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, queue, m.Queue)
	assert.Equal(t, "text/plain", m.ContentType)
	assert.Equal(t, "normal", m.MessageType)
	assert.Equal(t, 2, m.Retry)
	assert.Equal(t, "Hello LMQ", string(m.Body))

	assert.Nil(t, c.Reply(m, ReplyAck))

	_, err = c.Pull(queue, 0)
	assert.True(t, err.(*Error).IsEmpty())
}

func TestDelete(t *testing.T) {
	queue := "TestDelete"
	c := New(lmqURL)
	defer c.Delete(queue)

	_, err := c.Push(queue, "text/plain", strings.NewReader("Hello LMQ"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Nil(t, c.Delete(queue))
	_, err = c.Pull(queue, 0)
	assert.True(t, err.(*Error).IsEmpty())
}

func TestMulti(t *testing.T) {
	c := New(lmqURL)
	_, err := c.Pull("TestMulti:1", 0)
	assert.True(t, err.(*Error).IsEmpty())
	_, err = c.Pull("TestMulti:2", 0)
	assert.True(t, err.(*Error).IsEmpty())
	defer c.Delete("TestMulti:1")
	defer c.Delete("TestMulti:2")

	pattern := "^TestMulti:.*"
	r, err := c.PushAll(pattern, "text/plain", strings.NewReader("Multi"))
	if err != nil {
		t.Fatal(err)
	}
	if len(r) != 2 {
		t.Fatal("Unexpected number of properties")
	}
	assert.Equal(t, "no", r["TestMulti:1"].Accum)
	assert.Equal(t, "no", r["TestMulti:2"].Accum)

	rest := map[string]struct{}{"TestMulti:1": struct{}{}, "TestMulti:2": struct{}{}}
	m, err := c.PullAny(pattern, 0)
	if err != nil {
		t.Fatal(err)
	}
	delete(rest, m.Queue)
	assert.Equal(t, "text/plain", m.ContentType)
	assert.Equal(t, "normal", m.MessageType)
	assert.Equal(t, "Multi", string(m.Body))

	m, err = c.PullAny(pattern, 0)
	if err != nil {
		t.Fatal(err)
	}
	delete(rest, m.Queue)
	assert.Equal(t, "text/plain", m.ContentType)
	assert.Equal(t, "normal", m.MessageType)
	assert.Equal(t, "Multi", string(m.Body))
	assert.Equal(t, 0, len(rest))

	_, err = c.PullAny(pattern, 0)
	assert.True(t, err.(*Error).IsEmpty())
}

func TestProperty(t *testing.T) {
	queue := "TestProperty"
	c := New(lmqURL)
	defer c.Delete(queue)

	p, err := c.GetProperty(queue)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, time.Duration(0), p.Accum)
	assert.Equal(t, 2, p.Retry)
	assert.Equal(t, 30*time.Second, p.Timeout)

	p = NewProperty()
	p.Accum = 10 * time.Second
	err = c.UpdateProperty(queue, p)
	if err != nil {
		t.Fatal(err)
	}

	p, err = c.GetProperty(queue)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 10*time.Second, p.Accum)
	assert.Equal(t, 2, p.Retry)
	assert.Equal(t, 30*time.Second, p.Timeout)

	assert.Nil(t, c.DeleteProperty(queue))

	p, err = c.GetProperty(queue)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, time.Duration(0), p.Accum)
	assert.Equal(t, 2, p.Retry)
	assert.Equal(t, 30*time.Second, p.Timeout)
}

func TestDefaultProperty(t *testing.T) {
	c := New(lmqURL)
	props, err := c.GetDefaultProperty()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 0, len(props))

	p1 := NewProperty()
	p1.Timeout = time.Minute
	p2 := NewProperty()
	p2.Retry = 10
	props = []*DefaultProperty{
		{"^Test", p1},
		{".*", p2},
	}
	assert.Nil(t, c.SetDefaultProperty(props))

	props, err = c.GetDefaultProperty()
	if err != nil {
		t.Fatal(err)
	}
	if len(props) != 2 {
		t.Fatal("Unexpected number of properties")
	}
	assert.Equal(t, "^Test", props[0].Pattern)
	assert.Equal(t, time.Duration(-1), props[0].Property.Accum)
	assert.Equal(t, -1, props[0].Property.Retry)
	assert.Equal(t, time.Minute, props[0].Property.Timeout)
	assert.Equal(t, ".*", props[1].Pattern)
	assert.Equal(t, time.Duration(-1), props[1].Property.Accum)
	assert.Equal(t, 10, props[1].Property.Retry)
	assert.Equal(t, time.Duration(-1), props[1].Property.Timeout)

	assert.Nil(t, c.DeleteDefaultProperty())

	props, err = c.GetDefaultProperty()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 0, len(props))
}

func init() {
	if url := os.Getenv("LMQ_URL"); url != "" {
		lmqURL = url
		return
	}
	s := lmqtest.NewServer()
	lmqURL = s.URL
}
