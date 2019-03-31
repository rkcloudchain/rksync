package rpc

import (
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
	ch := demux.AddChannel(func(msg interface{}) bool { return true })

	go func() {
		demux.DeMultiplex("msg")
	}()

	select {
	case m := <-ch:
		assert.NotNil(t, m)
		assert.Equal(t, "msg", m.(string))
	case <-time.After(2 * time.Second):
		t.Failed()
	}
}
