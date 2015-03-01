package lmqtest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"

	"code.google.com/p/go-uuid/uuid"
)

type message struct {
	ct string
	b  []byte
}

type property struct {
	Accum   float64 `json:"accum"`
	Retry   int     `json:"retry"`
	Timeout float64 `json:"timeout"`
}

func newProperty() *property {
	return &property{Accum: 0, Retry: 2, Timeout: 30}
}

type FakeLMQ struct {
	queues   map[string]chan *message
	pendings map[string]*message
	props    map[string]*property
	defProps []byte
}

func NewServer() *httptest.Server {
	return httptest.NewServer(&FakeLMQ{
		queues:   make(map[string]chan *message),
		pendings: make(map[string]*message),
		props:    make(map[string]*property),
	})
}

func (s *FakeLMQ) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/messages/"):
		s.handleSingleMessage(w, r)
	case strings.HasPrefix(r.URL.Path, "/messages"):
		s.handleMultiMessage(w, r)
	case strings.HasPrefix(r.URL.Path, "/queues/"):
		s.handleQueue(w, r)
	case strings.HasPrefix(r.URL.Path, "/properties/"):
		s.handleQueueProperty(w, r)
	case strings.HasPrefix(r.URL.Path, "/properties"):
		s.handleDefaultQueueProperty(w, r)
	}
}

func (s *FakeLMQ) handleSingleMessage(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Path[10:]
	c, ok := s.queues[queue]
	if !ok {
		c = make(chan *message, 100)
		s.queues[queue] = c
	}
	switch r.Method {
	case "GET":
		select {
		case m := <-c:
			s.writeMessage(w, r, m, queue)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	case "POST":
		switch r.URL.Query().Get("reply") {
		case "ack":
			delete(s.pendings, queue)
			w.WriteHeader(http.StatusNoContent)
		case "":
			b, _ := ioutil.ReadAll(r.Body)
			c <- &message{ct: r.Header.Get("Content-Type"), b: b}
			w.Write([]byte(`{"accum":"no"}`))
		}
	}
}

func (s *FakeLMQ) handleMultiMessage(w http.ResponseWriter, r *http.Request) {
	re, err := regexp.Compile(r.URL.Query().Get("qre"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch r.Method {
	case "GET":
		for name, c := range s.queues {
			if re.MatchString(name) {
				select {
				case m := <-c:
					s.writeMessage(w, r, m, name)
					return
				default:
				}
			}
		}
		w.WriteHeader(http.StatusNoContent)
	case "POST":
		var resp []string
		b, _ := ioutil.ReadAll(r.Body)
		for name, c := range s.queues {
			if re.MatchString(name) {
				c <- &message{ct: r.Header.Get("Content-Type"), b: b}
				resp = append(resp, fmt.Sprintf(`"%s":{"accum":"no"}`, name))
			}
		}
		fmt.Fprintf(w, `{%s}`, strings.Join(resp, ","))
	}
}

func (s *FakeLMQ) writeMessage(w http.ResponseWriter, r *http.Request, m *message, queue string) {
	id := uuid.NewRandom().String()
	s.pendings[queue+"/"+id] = m
	w.Header().Set("X-Lmq-Message-Id", id)
	w.Header().Set("X-Lmq-Queue-Name", queue)
	w.Header().Set("X-Lmq-Message-Type", "normal")
	w.Header().Set("Content-Type", m.ct)
	w.Write(m.b)
}

func (s *FakeLMQ) handleQueue(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Path[8:]
	switch r.Method {
	case "DELETE":
		if _, ok := s.queues[queue]; !ok {
			w.WriteHeader(http.StatusNotFound)
		} else {
			delete(s.queues, queue)
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

func (s *FakeLMQ) handleQueueProperty(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Path[12:]
	switch r.Method {
	case "GET":
		p, _ := s.props[queue]
		if p == nil {
			p = newProperty()
		}
		json.NewEncoder(w).Encode(p)
	case "PATCH":
		p := newProperty()
		if err := json.NewDecoder(r.Body).Decode(p); err != nil {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			s.props[queue] = p
			w.WriteHeader(http.StatusNoContent)
		}
	case "DELETE":
		delete(s.props, queue)
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *FakeLMQ) handleDefaultQueueProperty(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		if s.defProps == nil {
			w.Write([]byte("[]"))
		} else {
			w.Write(s.defProps)
		}
	case "PUT":
		b, _ := ioutil.ReadAll(r.Body)
		s.defProps = b
		w.WriteHeader(http.StatusNoContent)
	case "DELETE":
		s.defProps = nil
		w.WriteHeader(http.StatusNoContent)
	}
}
