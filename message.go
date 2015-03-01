package lmq

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"reflect"

	"github.com/ugorji/go/codec"
)

type DecodeFunc func([]byte, interface{}) error

var (
	EOF       = errors.New("lmq: message reached EOF")
	ErrDecode = errors.New("lmq: message decode error")

	decoderMap = map[string]DecodeFunc{}
)

func RegisterDecoder(contentType string, f DecodeFunc) {
	decoderMap[contentType] = f
}

type Message struct {
	ID          string
	Queue       string
	MessageType string
	ContentType string
	Body        []byte
	cm          compoundMessage
	eof         error
}

// newMessage creates Message from *http.Response.
func newMessage(resp *http.Response) (*Message, error) {
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &Message{
		ID:          resp.Header.Get("X-Lmq-Message-Id"),
		Queue:       resp.Header.Get("X-Lmq-Queue-Name"),
		MessageType: resp.Header.Get("X-Lmq-Message-Type"),
		ContentType: resp.Header.Get("Content-Type"),
		Body:        b,
	}, nil
}

func (m *Message) Decode(v interface{}) error {
	if m.eof != nil {
		return m.eof
	}

	switch m.MessageType {
	case "normal":
		m.eof = EOF
		return decodeBody(m.ContentType, m.Body, v)
	case "compound":
		if m.cm == nil {
			if err := msgpackDecoder(m.Body, &m.cm); err != nil {
				return err
			}
		}
		var msg []interface{}
		msg, m.cm = m.cm[0], m.cm[1:]
		if len(m.cm) == 0 {
			m.eof = EOF
		}
		meta, body := msg[0].(map[string]interface{}), msg[1].([]byte)
		return decodeBody(meta["content-type"].(string), body, v)
	}
	return ErrDecode
}

// compoundMessage represents compounded message. Actually, its format is list
// of length 2 list (metadata, content) where type of metadata is
// map[string]interface{} and type of content is interface{}.
type compoundMessage [][]interface{}

func decodeBody(ct string, b []byte, v interface{}) error {
	if f, ok := decoderMap[ct]; ok {
		return f(b, v)
	}
	return rawDecoder(b, v)
}

func msgpackDecoder(b []byte, v interface{}) error {
	return codec.NewDecoderBytes(b, mh).Decode(v)
}

func jsonDecoder(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}

func rawDecoder(b []byte, v interface{}) error {
	switch out := v.(type) {
	case []byte:
		copy(out, b)
	case *[]byte:
		if len(b) <= cap(*out) {
			n := copy((*out)[:cap(*out)], b)
			*out = (*out)[:n]
		} else {
			tmp := make([]byte, len(b))
			copy(tmp, b)
			*out = tmp
		}
	case *interface{}:
		*out = b
	default:
		return ErrDecode
	}
	return nil
}

var mh = &codec.MsgpackHandle{RawToString: true, WriteExt: true}

func init() {
	mh.MapType = reflect.TypeOf(map[string]interface{}(nil))
	RegisterDecoder("application/x-msgpack", msgpackDecoder)
	RegisterDecoder("application/json", jsonDecoder)
}
