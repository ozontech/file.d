package gelf

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

type transport string

const (
	transportTCP transport = "tcp"
	transportUDP transport = "udp"
)

type client struct {
	transport transport
	useTLS    bool
	stdClient net.Conn
	tlsClient *tls.Conn
}

func newClient(address string, port uint, useTLS bool, transport transport, timeout time.Duration, config *tls.Config) (*client, error) {
	endpoint := fmt.Sprintf("%s:%d", address, port)
	if useTLS {
		c, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, string(transport), endpoint, config)
		if err != nil {
			return nil, err
		}

		return &client{tlsClient: c}, nil
	} else {
		c, err := net.Dial("tcp", endpoint)
		if err != nil {
			return nil, err
		}
		return &client{stdClient: c}, nil
	}
}

func (g *client) send(data []byte) (int, error) {
	if g.useTLS {
		return g.tlsClient.Write(data)
	} else {
		return g.stdClient.Write(data)
	}
}

func (g *client) close() error {
	if g.useTLS {
		return g.tlsClient.Close()
	} else {
		return g.stdClient.Close()
	}
}
