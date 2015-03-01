package lmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func must(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
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

func TestDecodeRaw(t *testing.T) {
	m := &Message{MessageType: "normal", ContentType: "text/plain", Body: []byte("hello")}
	var b []byte
	must(t, m.Decode(&b))
	assert.Equal(t, []byte("hello"), b)

	m = &Message{MessageType: "normal", ContentType: "text/plain", Body: []byte("world")}
	b = b[:2]
	must(t, m.Decode(b))
	assert.Equal(t, []byte("wo"), b)

	m = &Message{MessageType: "normal", ContentType: "text/plain", Body: []byte("world")}
	must(t, m.Decode(&b))
	assert.Equal(t, []byte("world"), b)
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
