package gelf

import (
	"crypto/tls"
	"net"
	"time"
)

type network string

const transportTCP network = "tcp"

type client struct {
	useTLS    bool
	stdClient net.Conn
	tlsClient *tls.Conn
}

func newClient(network network, address string, timeout time.Duration, useTLS bool, tlsConfig *tls.Config) (*client, error) {
	if useTLS {
		c, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, string(network), address, tlsConfig)
		if err != nil {
			return nil, err
		}

		return &client{tlsClient: c}, nil
	} else {
		c, err := net.DialTimeout(string(network), address, timeout)
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
