package gelf

import (
	"crypto/tls"
	"net"
	"time"
)

type network string

const transportTCP network = "tcp"

type client struct {
	conn    net.Conn
	timeout time.Duration
}

func newClient(network network, address string, timeout time.Duration, useTLS bool, tlsConfig *tls.Config) (c *client, err error) {
	c = &client{timeout: timeout}

	if useTLS {
		c.conn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout}, string(network), address, tlsConfig)
	} else {
		c.conn, err = net.DialTimeout(string(network), address, timeout)
	}

	return c, err
}

func (g *client) send(data []byte) (int, error) {
	if err := g.conn.SetWriteDeadline(time.Now().Add(g.timeout)); err != nil {
		return 0, err
	}
	return g.conn.Write(data)
}

func (g *client) close() error {
	return g.conn.Close()
}
