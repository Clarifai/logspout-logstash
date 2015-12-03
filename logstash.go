package logstash

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams TCP JSON to Logstash.
type LogstashAdapter struct {
	conn  net.Conn
	route *router.Route
}

func getopt(name, dfault string) string {
	value := os.Getenv(name)
	if value == "" {
		value = dfault
	}
	return value
}

// NewLogstashAdapter creates a LogstashAdapter with TCP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	t := getopt("LOGSTASH_TRANSPORT", "tcp")
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport(t))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
	}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		var data interface{}
		if m.Data != "" {
			err := json.Unmarshal([]byte(m.Data), &data)
			if err != nil {
				// FIXME: how to safely log errors not messing up downstream schema?
				data = fmt.Sprintf("ERROR: Failed to parse json: %s", err)
			}
		}

		token := getopt("LOGSTASH_LOGZIO_TOKEN", "")

		msg := LogstashMessage{
			Data:     data,
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
			Token:    token,
		}
		js, err := json.Marshal(msg)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
		_, err = a.conn.Write(js)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
	}
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Name     string      `json:"docker.name"`
	ID       string      `json:"docker.id"`
	Image    string      `json:"docker.image"`
	Hostname string      `json:"docker.hostname"`
	Data     interface{} `json:"data,omitempty"`
	Token    string      `json:"token,omitempty"` // logz.io token
}
