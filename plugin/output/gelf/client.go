package gelf

import (
	"crypto/tls"
	"net"
	"time"
)

const transportTCP string = "tcp"

type client struct {
	conn    net.Conn
	timeout time.Duration
}

func newClient(address string, connTimeout, writeTimeout time.Duration, useTLS bool, tlsConfig *tls.Config) (c *client, err error) {
	c = &client{timeout: writeTimeout}

	if useTLS {
		c.conn, err = tls.DialWithDialer(&net.Dialer{Timeout: connTimeout}, transportTCP, address, tlsConfig)
	} else {
		c.conn, err = net.DialTimeout(transportTCP, address, connTimeout)
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
