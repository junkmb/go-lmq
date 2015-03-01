package lmq

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
	"time"
)

func NewProperty() *Property {
	p := new(Property)
	p.init()
	return p
}

type Property struct {
	Accum   time.Duration
	Retry   int
	Timeout time.Duration
}

func (p *Property) init() {
	p.Accum = -1
	p.Retry = -1
	p.Timeout = -1
}

func (p *Property) MarshalJSON() ([]byte, error) {
	v := make(map[string]interface{})
	if p.Accum >= 0 {
		v["accum"] = p.Accum.Seconds()
	}
	if p.Retry >= 0 {
		v["retry"] = p.Retry
	}
	if p.Timeout >= 0 {
		v["timeout"] = p.Timeout.Seconds()
	}
	return json.Marshal(v)
}

func (p *Property) UnmarshalJSON(b []byte) error {
	var v map[string]interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	p.init()
	if accum, ok := v["accum"].(float64); ok {
		p.Accum = time.Duration(accum * 1e+9)
	}
	if retry, ok := v["retry"].(float64); ok {
		p.Retry = int(retry)
	}
	if timeout, ok := v["timeout"].(float64); ok {
		p.Timeout = time.Duration(timeout * 1e+9)
	}
	return nil
}

type DefaultProperty struct {
	Pattern  string
	Property *Property
}

func (p *DefaultProperty) MarshalJSON() ([]byte, error) {
	if _, err := regexp.Compile(p.Pattern); err != nil {
		return nil, err
	}
	return json.Marshal([]interface{}{p.Pattern, p.Property})
}

func (p *DefaultProperty) UnmarshalJSON(b []byte) error {
	var v []json.RawMessage
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	if err := json.Unmarshal(v[0], &p.Pattern); err != nil {
		return err
	}
	p.Property = new(Property)
	if err := json.Unmarshal(v[1], p.Property); err != nil {
		return err
	}
	return nil
}

func (c *client) GetProperty(queue string) (*Property, error) {
	var p Property
	err := c.getProperty("/properties/"+queue, &p)
	return &p, err
}

func (c *client) UpdateProperty(queue string, p *Property) error {
	return c.setProperty("PATCH", "/properties/"+queue, p)
}

func (c *client) DeleteProperty(queue string) error {
	return c.delete("/properties/" + queue)
}

func (c *client) GetDefaultProperty() ([]*DefaultProperty, error) {
	var p []*DefaultProperty
	err := c.getProperty("/properties", &p)
	return p, err
}

func (c *client) SetDefaultProperty(props []*DefaultProperty) error {
	return c.setProperty("PUT", "/properties", props)
}

func (c *client) DeleteDefaultProperty() error {
	return c.delete("/properties")
}

func (c *client) getProperty(url string, r interface{}) error {
	resp, err := c.do("GET", url, "", nil)
	if err != nil {
		return err
	}
	defer discard(resp.Body)
	if err := checkStatus(resp, http.StatusOK); err != nil {
		return err
	}
	return json.NewDecoder(resp.Body).Decode(&r)
}

func (c *client) setProperty(method, url string, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	resp, err := c.do(method, url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer discard(resp.Body)
	return checkStatus(resp, http.StatusNoContent)
}
