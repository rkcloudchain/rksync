package rpc

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestChannelDeMultiplexerClose(t *testing.T) {
	demux := NewChannelDemultiplexer()
	demux.Close()
	demux.DeMultiplex("msg")
}

func TestChannelDeMultiplexerBroadcasts(t *testing.T) {
	demux := NewChannelDemultiplexer()
	ch1 := demux.AddChannel(func(msg interface{}) bool { return true })
	ch2 := demux.AddChannel(func(msg interface{}) bool { return true })
	assert.Len(t, demux.channels, 2)

	go func() {
		demux.DeMultiplex("msg")
	}()

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for {
			select {
			case m1 := <-ch1:
				assert.NotNil(t, m1)
				assert.Equal(t, "msg", m1.(string))
				wg.Done()
			case m2 := <-ch2:
				assert.NotNil(t, m2)
				assert.Equal(t, "msg", m2.(string))
				wg.Done()
			}
		}
	}()

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
	case <-time.After(2 * time.Second):
		t.Failed()
	}
}

func TestUnregister(t *testing.T) {
	demux := NewChannelDemultiplexer()
	demux.AddChannelWithMAC(func(msg interface{}) bool { return true }, []byte{0})
	demux.AddChannelWithMAC(func(msg interface{}) bool { return true }, []byte{0})
	demux.AddChannelWithMAC(func(msg interface{}) bool { return true }, []byte{1})
	demux.AddChannelWithMAC(func(msg interface{}) bool { return true }, []byte{1})
	assert.Len(t, demux.channels, 4)

	demux.Unregister([]byte{0})
	assert.Len(t, demux.channels, 2)
}
