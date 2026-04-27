package file_socket

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

const (
	retryDelay = 250 * time.Millisecond
	delimiter  = '\n'

	networkTcp  = "tcp"
	networkUdp  = "udp"
	networkUnix = "unix"
)

type testServer struct {
	network  string
	address  string
	messages []string
	mu       sync.Mutex

	listener   net.Listener
	packetConn net.PacketConn
}

func newTestServer(network, address string) (*testServer, error) {
	s := &testServer{
		network: network,
		address: address,
	}

	switch network {
	case networkTcp, networkUnix:
		if network == networkUnix {
			_ = os.Remove(address)
		}
		ln, err := net.Listen(network, address)
		if err != nil {
			return nil, fmt.Errorf("failed to listen on %s %s: %w", network, address, err)
		}
		s.listener = ln
		go s.acceptLoop()

	case networkUdp:
		pc, err := net.ListenPacket(networkUdp, address)
		if err != nil {
			return nil, fmt.Errorf("failed to listen UDP on %s: %w", address, err)
		}
		s.packetConn = pc
		go s.readUDPLoop()

	default:
		return nil, fmt.Errorf("unsupported network: %s", network)
	}

	return s, nil
}

func (s *testServer) close() {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	if s.packetConn != nil {
		_ = s.packetConn.Close()
	}
	if s.network == networkUnix {
		_ = os.Remove(s.address)
	}
}

func (s *testServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *testServer) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	for {
		raw, err := reader.ReadBytes(delimiter)
		if err != nil {
			return
		}
		msg := bytes.TrimRight(raw, "\x00")
		if len(msg) == 0 {
			continue
		}
		s.storeMessage(string(msg))
	}
}

func (s *testServer) readUDPLoop() {
	buf := make([]byte, 65535)
	for {
		n, _, err := s.packetConn.ReadFrom(buf)
		if err != nil {
			return
		}
		for _, part := range bytes.Split(buf[:n], []byte{delimiter}) {
			if len(part) == 0 {
				continue
			}
			s.storeMessage(string(part))
		}
	}
}

func (s *testServer) storeMessage(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, msg)
}

func (s *testServer) collected() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.messages))
	copy(out, s.messages)
	return out
}

func (s *testServer) waitForMessages(minCount, retries int, delay time.Duration) ([]map[string]interface{}, error) {
	for i := 0; i < retries; i++ {
		msgs := s.collected()
		if len(msgs) >= minCount {
			return decodeMessages(msgs)
		}
		time.Sleep(delay)
	}

	msgs := s.collected()
	return nil, fmt.Errorf(
		"expected at least %d messages, got %d after %d retries",
		minCount, len(msgs), retries,
	)
}

func decodeMessages(raw []string) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0, len(raw))
	for _, s := range raw {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			return nil, fmt.Errorf("failed to decode message %q: %w", s, err)
		}
		result = append(result, m)
	}
	return result, nil
}
