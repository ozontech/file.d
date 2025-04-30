package socket

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xtls"
	"go.uber.org/zap"
)

/*{ introduction
It reads events from socket network.
}*/

/*{ examples
TCP:
```yaml
pipelines:
  example_pipeline:
    ...
    input:
      type: socket
      network: tcp
      address: ':6666'
    ...
```
---
TLS:
```yaml
pipelines:
  example_pipeline:
    ...
    input:
      type: socket
      network: tcp
      address: ':6666'
      ca_cert: './cert.pem'
      private_key: './key.pem'
    ...
```
---
UDP:
```yaml
pipelines:
  example_pipeline:
    ...
    input:
      type: socket
      network: udp
      address: '[2001:db8::1]:1234'
    ...
```
---
Unix:
```yaml
pipelines:
  example_pipeline:
    ...
    input:
      type: socket
      network: unix
      address: '/tmp/filed.sock'
    ...
```
}*/

const defaultReadBufferSize = 4096

type Plugin struct {
	config     *Config
	controller pipeline.InputPluginController
	logger     *zap.Logger

	readBuffers  *sync.Pool
	eventBuffers *sync.Pool

	stopChan chan struct{}
}

const (
	networkTcp  = "tcp"
	networkUdp  = "udp"
	networkUnix = "unix"
)

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Which network type to listen.
	Network string `json:"network" default:"tcp" options:"tcp|udp|unix"` // *

	// > @3@4@5@6
	// >
	// > An address to listen to.
	// >
	// > For example:
	// > - /tmp/filed.sock
	// > - 1.2.3.4:9092
	// > - :9092
	Address string `json:"address" required:"true"` // *

	// > @3@4@5@6
	// >
	// > CA certificate in PEM encoding. This can be a path or the content of the certificate.
	// >> Works only if `network` is set to `tcp`.
	CACert string `json:"ca_cert" default:""` // *

	// > @3@4@5@6
	// >
	// > CA private key in PEM encoding. This can be a path or the content of the key.
	// >> Works only if `network` is set to `tcp`.
	PrivateKey string `json:"private_key" default:""` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "socket",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.logger = params.Logger.Desugar()
	p.config = config.(*Config)
	p.controller = params.Controller

	p.readBuffers = &sync.Pool{
		New: func() any {
			return newWrapBuffer(
				make([]byte, defaultReadBufferSize),
				false,
			)
		},
	}
	p.eventBuffers = &sync.Pool{
		New: func() any {
			return newWrapBuffer(
				make([]byte, 0, params.PipelineSettings.AvgEventSize),
				true,
			)
		},
	}

	p.stopChan = make(chan struct{})

	var (
		ln  net.Listener
		pc  net.PacketConn
		err error
	)
	switch p.config.Network {
	case networkTcp:
		if p.config.CACert == "" && p.config.PrivateKey == "" {
			ln, err = net.Listen(p.config.Network, p.config.Address)
			break
		}

		tlsBuilder := xtls.NewConfigBuilder()
		err = tlsBuilder.AppendX509KeyPair(p.config.CACert, p.config.PrivateKey)
		if err == nil {
			ln, err = tls.Listen(p.config.Network, p.config.Address, tlsBuilder.Build())
		}
	case networkUnix:
		ln, err = net.Listen(p.config.Network, p.config.Address)
	case networkUdp:
		pc, err = net.ListenPacket(p.config.Network, p.config.Address)
	default:
		p.logger.Fatal("unknown network type")
	}
	if err != nil {
		p.logger.Fatal("can't start listen", zap.Error(err),
			zap.String("network", p.config.Network), zap.String("address", p.config.Address))
	}

	p.logger.Info("start listen", zap.String("network", p.config.Network), zap.String("address", p.config.Address))
	if ln != nil {
		go p.listen(ln)
	} else {
		go p.listenPacket(pc)
	}
}

func (p *Plugin) Stop() {
	p.logger.Info("stop listen")
	close(p.stopChan)
}

func (p *Plugin) Commit(_ *pipeline.Event) {
}

func (p *Plugin) PassEvent(_ *pipeline.Event) bool {
	return true
}

func (p *Plugin) listen(ln net.Listener) {
	wg := &sync.WaitGroup{}

	defer func() {
		wg.Wait()
		_ = ln.Close()
	}()

	go func() {
		<-p.stopChan
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			// after stopping plugin it's normal to get "use of closed network connection" error
			if !strings.Contains(err.Error(), "use of closed network connection") {
				p.logger.Error("failed to accept connection", zap.Error(err))
			}
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-p.stopChan:
				return
			}
		}

		wg.Add(1)
		go p.handleConn(conn, wg)
	}
}

func (p *Plugin) handleConn(c net.Conn, wg *sync.WaitGroup) {
	defer func() {
		_ = c.Close()
		wg.Done()
	}()

	readBuf := getWrapBuffer(p.readBuffers)
	eventBuf := getWrapBuffer(p.eventBuffers)
	defer putWrapBuffer(readBuf, p.readBuffers)
	defer putWrapBuffer(eventBuf, p.eventBuffers)

	sourceID := calcSourceID(c.RemoteAddr(), c.LocalAddr())

	for {
		n, err := c.Read(readBuf.b)
		if n == 0 {
			break
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.logger.Error("connection dropped", zap.Error(err))
			}
			break
		}

		eventBuf.b = p.processChunk(sourceID, readBuf.b[:n], eventBuf.b, false)
	}

	if len(eventBuf.b) > 0 {
		eventBuf.b = p.processChunk(sourceID, readBuf.b[:0], eventBuf.b, true)
	}
}

func (p *Plugin) listenPacket(pc net.PacketConn) {
	defer func() {
		_ = pc.Close()
	}()

	go func() {
		<-p.stopChan
		_ = pc.Close()
	}()

	buf := make([]byte, 65535) // max udp-packet size = 64kb

	for {
		n, addr, err := pc.ReadFrom(buf)
		if n == 0 {
			break
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.logger.Error("connection dropped", zap.Error(err))
			}
			break
		}

		p.handlePacket(buf[:n], addr)
	}
}

func (p *Plugin) handlePacket(packet []byte, addr net.Addr) {
	eventBuf := getWrapBuffer(p.eventBuffers)
	defer putWrapBuffer(eventBuf, p.eventBuffers)

	sourceID := calcSourceID(addr)
	eventBuf.b = p.processChunk(sourceID, packet, eventBuf.b, true)
}

func (p *Plugin) processChunk(sourceID pipeline.SourceID, readBuf, eventBuf []byte, last bool) []byte {
	pos := 0   // current position
	nlPos := 0 // new line position
	for pos < len(readBuf) {
		if readBuf[pos] != '\n' {
			pos++
			continue
		}

		if len(eventBuf) != 0 {
			eventBuf = append(eventBuf, readBuf[nlPos:pos]...)
			_ = p.controller.In(sourceID, "socket", pipeline.NewOffsets(int64(pos), nil), eventBuf, true, nil)
			eventBuf = eventBuf[:0]
		} else {
			_ = p.controller.In(sourceID, "socket", pipeline.NewOffsets(int64(pos), nil), readBuf[nlPos:pos], true, nil)
		}

		pos++
		nlPos = pos
	}

	if last {
		eventBuf = append(eventBuf, readBuf[nlPos:]...)
		_ = p.controller.In(sourceID, "socket", pipeline.NewOffsets(int64(pos), nil), eventBuf, true, nil)
		eventBuf = eventBuf[:0]
	} else {
		eventBuf = append(eventBuf, readBuf[nlPos:]...)
	}

	return eventBuf
}

func calcSourceID(addrs ...net.Addr) pipeline.SourceID {
	for _, addr := range addrs {
		if addr != nil {
			return pipeline.SourceID(xxhash.Sum64String(addr.Network() + "_" + addr.String()))
		}
	}
	return 0
}

type wrapBuffer struct {
	b         []byte
	needReset bool
}

func newWrapBuffer(b []byte, needReset bool) *wrapBuffer {
	return &wrapBuffer{
		b:         b,
		needReset: needReset,
	}
}

func (wb *wrapBuffer) reset() {
	if wb.needReset {
		wb.b = wb.b[:0]
	}
}

func getWrapBuffer(pool *sync.Pool) *wrapBuffer {
	return pool.Get().(*wrapBuffer)
}

func putWrapBuffer(wb *wrapBuffer, pool *sync.Pool) {
	wb.reset()
	pool.Put(wb)
}
