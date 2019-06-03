package fsync

import (
	"bytes"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/filter"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
)

func createAppendSyncProvider(chainMac common.ChainMac, chainID string, filename string, metadata []byte, leader bool,
	pkiID common.PKIidType, adapter Adapter) (FileSyncProvider, error) {

	mac := GenerateMAC(chainMac, filename)
	provider := &appendSyncProvider{
		Adapter:  adapter,
		chainMac: chainMac,
		chainID:  chainID,
		fmeta:    &config.FileMeta{Name: filename, Metadata: metadata, Leader: leader},
		state:    int32(0),
		stopCh:   make(chan struct{}, 1),
		pkiID:    pkiID,
	}

	provider.reqChan, _ = adapter.Accept(func(message interface{}) bool {
		msg, ok := message.(*protos.RKSyncMessage)
		if !ok {
			return false
		}
		return msg.IsDataReq() && msg.GetDataReq().GetAppend() != nil &&
			bytes.Equal(msg.ChainMac, chainMac) && bytes.Equal([]byte(msg.GetDataReq().FileName), []byte(filename))
	}, mac, false)

	provider.done.Add(1)
	go provider.processDataReq()

	if leader {
		return provider, nil
	}

	provider.msgChan, _ = adapter.Accept(func(message interface{}) bool {
		msg, ok := message.(*protos.RKSyncMessage)
		if !ok {
			return false
		}
		return msg.IsDataMsg() && msg.GetDataMsg().GetAppend() != nil &&
			bytes.Equal(msg.ChainMac, chainMac) && bytes.Equal([]byte(msg.GetDataMsg().FileName), []byte(filename))
	}, mac, false)

	start, err := provider.initPayloadBufferStart(leader)
	if err != nil {
		return nil, err
	}
	provider.payloads = NewPayloadBuffer(start)

	provider.done.Add(2)
	go provider.listen()
	go provider.periodicalInvocation(4 * time.Second)

	return provider, nil
}

type appendSyncProvider struct {
	Adapter
	chainMac common.ChainMac
	chainID  string
	fmeta    *config.FileMeta
	state    int32
	pkiID    common.PKIidType
	payloads PayloadBuffer
	msgChan  <-chan *protos.RKSyncMessage
	reqChan  <-chan *protos.RKSyncMessage
	done     sync.WaitGroup
	stopCh   chan struct{}
}

func (p *appendSyncProvider) initPayloadBufferStart(leader bool) (int64, error) {
	fs := p.GetFileSystem()
	fi, err := fs.Stat(p.chainID, *p.fmeta)
	if err == nil {
		return fi.Size(), nil
	}

	if !leader && os.IsNotExist(err) {
		logging.Debugf("Channel %s file %s does not exists, create it", p.chainMac, p.fmeta.Name)
		f, err := fs.Create(p.chainID, *p.fmeta)
		if err != nil {
			logging.Errorf("Failed creating file %s (Channel %s): %s", p.fmeta.Name, p.chainMac, err)
			return 0, err
		}
		f.Close()
		return 0, nil
	}

	return 0, errors.Errorf("Failed stating file %s (Channel %s): %s", p.fmeta.Name, p.chainMac, err)
}

// Stop stops the FileSyncProvider
func (p *appendSyncProvider) Stop() {
	logging.Info("Stopping append fsync provider")
	defer logging.Info("Stopped append fsync provider")

	p.stopCh <- struct{}{}
	p.done.Wait()
}

func (p *appendSyncProvider) listen() {
	defer p.done.Done()

	for {
		select {
		case s := <-p.stopCh:
			p.stopCh <- s
			logging.Debug("Stop listening for new messages")
			return
		case msg := <-p.msgChan:
			p.queueDataMsg(msg)
		}
	}
}

func (p *appendSyncProvider) periodicalInvocation(d time.Duration) {
	defer p.done.Done()

	for {
		select {
		case s := <-p.stopCh:
			p.stopCh <- s
			return

		case <-time.After(d):
			if !atomic.CompareAndSwapInt32(&p.state, int32(0), int32(2)) {
				continue
			}

			p.requestDataAppend()
			atomic.StoreInt32(&p.state, int32(0))

		case <-p.payloads.Ready():
			if !atomic.CompareAndSwapInt32(&p.state, int32(0), int32(2)) {
				continue
			}

			p.processPayloads()
			atomic.StoreInt32(&p.state, int32(0))
		}
	}
}

func (p *appendSyncProvider) processDataReq() {
	defer p.done.Done()
	wg := &sync.WaitGroup{}

	for {
		select {
		case msg := <-p.reqChan:
			swapped := atomic.CompareAndSwapInt32(&p.state, int32(0), int32(1))
			if !swapped {
				if atomic.LoadInt32(&p.state) != int32(1) {
					continue
				}
			}

			wg.Add(1)
			go p.handleDataReq(msg, wg)
			if swapped {
				go func() { wg.Wait(); atomic.StoreInt32(&p.state, int32(0)) }()
			}

		case s := <-p.stopCh:
			p.stopCh <- s
			wg.Wait()
			return
		}
	}
}

func (p *appendSyncProvider) processPayloads() {
	logging.Debugf("[%s] Ready to process payloads, next payload start number is = [%d]", p.fmeta.Name, p.payloads.Next())
	fs := p.GetFileSystem()
	f, err := fs.OpenFile(p.chainID, *p.fmeta, os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		logging.Errorf("Failed opening file %s (Channel %): %s", p.fmeta.Name, p.chainMac, err)
		return
	}
	defer f.Close()

	for payload := p.payloads.Peek(); payload != nil; payload = p.payloads.Peek() {
		n, err := f.Write(payload.GetAppend().Data)
		if err != nil {
			logging.Errorf("Failed appending data to file %s: %s", p.fmeta.Name, err)
			if n > 0 {
				p.payloads.Reset(int64(n))
			}
			return
		}
		p.payloads.Expire(int64(n))
	}
}

func (p *appendSyncProvider) queueDataMsg(msg *protos.RKSyncMessage) {
	if !bytes.Equal(msg.ChainMac, p.chainMac) {
		logging.Warningf("Received message for channel %s while expecting channel %s, ignoring", common.ChainMac(msg.ChainMac), p.chainMac)
		return
	}

	dataMsg := msg.GetDataMsg()
	if dataMsg != nil {
		payload := dataMsg.Payload
		if payload == nil {
			logging.Error("Given payload is nil")
			return
		}

		p.payloads.Push(dataMsg)
	} else {
		logging.Warning("RKSync message received is not of data message type, usually this should not happen.")
	}
}

func (p *appendSyncProvider) handleDataReq(msg *protos.RKSyncMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	if !bytes.Equal(p.chainMac, msg.ChainMac) {
		logging.Warningf("Received message for channel %s while expecting channel %s, ignoring", common.ChainMac(msg.ChainMac), p.chainMac)
		return
	}

	req := msg.GetDataReq()
	if !bytes.Equal([]byte(req.FileName), []byte(p.fmeta.Name)) {
		logging.Warningf("Received message for file %s while expecting file %s, ignoring", req.FileName, p.fmeta.Name)
		return
	}

	peer := p.Lookup(req.PkiId)
	if peer == nil {
		logging.Warningf("Can't find peer's information: %s", req.PkiId)
		return
	}

	appendReq := req.GetAppend()
	fi, err := p.GetFileSystem().Stat(p.chainID, *p.fmeta)
	if err != nil {
		logging.Warningf("Failed to stat file %s: %s", p.fmeta.Name, err)
		return
	}

	if appendReq.Length >= fi.Size() {
		logging.Debugf("The sender's file is newer")
		return
	}

	data := make([]byte, dataBlockSize)
	start := appendReq.Length
	var n int

	fs := p.GetFileSystem()
	f, err := fs.OpenFile(p.chainID, *p.fmeta, os.O_RDONLY, os.ModePerm)
	if err != nil {
		logging.Errorf("Failed opening file %s (Channel %s): %s", p.fmeta.Name, p.chainMac, err)
		return
	}
	defer f.Close()

	for {
		n, err = f.ReadAt(data, start)
		if n > 0 {
			data = data[:n]
		}
		if err == io.EOF {
			if n > 0 {
				sMsg, err := p.createAppendDataMsg(data, start)
				if err != nil {
					logging.Warningf("Failed creating DataMessage: %v", err)
					return
				}
				p.SendToPeer(sMsg, peer)
			}
			return
		}
		if err != nil {
			logging.Warningf("Read file %s failed: %s", p.fmeta.Name, err)
			return
		}

		sMsg, err := p.createAppendDataMsg(data, start)
		if err != nil {
			logging.Warningf("Failed creating DataMessage: %v", err)
			return
		}

		p.SendToPeer(sMsg, peer)
		start = start + int64(n)
	}
}

func (p *appendSyncProvider) createAppendDataMsg(data []byte, start int64) (*protos.SignedRKSyncMessage, error) {
	msg := &protos.RKSyncMessage{
		Nonce:    uint64(0),
		ChainMac: p.chainMac,
		Tag:      protos.RKSyncMessage_CHAN_ONLY,
		Content: &protos.RKSyncMessage_DataMsg{
			DataMsg: &protos.DataMessage{
				FileName: p.fmeta.Name,
				PkiId:    p.pkiID,
				Payload: &protos.DataMessage_Append{
					Append: &protos.AppendPayload{
						StartOffset: start,
						Data:        data,
					},
				},
			},
		},
	}

	return p.Sign(msg)
}

func (p *appendSyncProvider) requestDataAppend() {
	req, err := p.createDataAppendRequest()
	if err != nil {
		logging.Warningf("Failed creating SignedRKSyncMessage: %+v", err)
		return
	}

	endpoints := filter.SelectPeers(1, p.GetMembership(), p.IsMemberInChan)
	if len(endpoints) == 0 {
		logging.Warningf("Can't find any member in Chain: %s", p.chainMac)
		return
	}

	p.SendToPeer(req, endpoints[0])
}

func (p *appendSyncProvider) createDataAppendRequest() (*protos.SignedRKSyncMessage, error) {
	fi, err := p.GetFileSystem().Stat(p.chainID, *p.fmeta)
	if err != nil {
		logging.Warningf("Failed to stat file %s: %s", p.fmeta.Name, err)
		return nil, err
	}

	msg := &protos.RKSyncMessage{
		Nonce:    uint64(0),
		ChainMac: p.chainMac,
		Tag:      protos.RKSyncMessage_CHAN_ONLY,
		Content: &protos.RKSyncMessage_DataReq{
			DataReq: &protos.DataRequest{
				FileName: p.fmeta.Name,
				PkiId:    p.pkiID,
				Req: &protos.DataRequest_Append{
					Append: &protos.AppendRequest{
						Length: fi.Size(),
					},
				},
			},
		},
	}

	return p.Sign(msg)
}
