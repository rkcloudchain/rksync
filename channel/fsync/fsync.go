package fsync

import (
	"bytes"
	"io"
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

const (
	dataBlockSize = 512 * 1024
)

// Adapter enables the fsync to communicate with rksync channel
type Adapter interface {
	GetFileSystem() config.FileSystem
	SendToPeer(*protos.SignedRKSyncMessage, *common.NetworkMember)
	Lookup(common.PKIidType) *common.NetworkMember
	Sign(*protos.RKSyncMessage) (*protos.SignedRKSyncMessage, error)
	GetMembership() []common.NetworkMember
	IsMemberInChan(common.NetworkMember) bool
	Accept(acceptor common.MessageAcceptor, passThrough bool) (<-chan *protos.RKSyncMessage, <-chan protos.ReceivedMessage)
}

// NewFileSyncProvider creates FileSyncProvier instance
func NewFileSyncProvider(chainID, filename string, mode protos.File_Mode, leader bool, pkiID common.PKIidType,
	adapter Adapter) (*FileSyncProvier, error) {

	msgChan, _ := adapter.Accept(func(message interface{}) bool {
		return message.(*protos.RKSyncMessage).IsDataMsg() &&
			bytes.Equal(message.(*protos.RKSyncMessage).Channel, []byte(chainID)) &&
			bytes.Equal([]byte(message.(*protos.RKSyncMessage).GetDataMsg().FileName), []byte(filename))
	}, false)

	reqChan, _ := adapter.Accept(func(message interface{}) bool {
		return message.(*protos.RKSyncMessage).IsDataReq() &&
			bytes.Equal(message.(*protos.RKSyncMessage).Channel, []byte(chainID)) &&
			bytes.Equal([]byte(message.(*protos.RKSyncMessage).GetDataReq().FileName), []byte(filename))
	}, false)

	var b PayloadBuffer
	if mode == protos.File_Append {
		fi, err := adapter.GetFileSystem().Stat(filename)
		if err != nil {
			return nil, errors.Errorf("Failed stating file %s: %s", filename, err)
		}
		b = NewPayloadBuffer(fi.Size())
	} else {
		return nil, errors.Errorf("Unsupported file mode: %s", protos.File_Mode_name[int32(mode)])
	}

	p := &FileSyncProvier{
		Adapter:  adapter,
		chainID:  chainID,
		filename: filename,
		mode:     mode,
		leader:   leader,
		state:    int32(0),
		payloads: b,
		reqChan:  reqChan,
		msgChan:  msgChan,
		stopCh:   make(chan struct{}, 1),
		pkiID:    pkiID,
	}

	if !leader {
		p.done.Add(3)
		go p.processDataReq()
		go p.listen()
		go p.periodicalInvocation(5 * time.Second)
	} else {
		p.done.Add(1)
		go p.processDataReq()
	}

	return p, nil
}

// FileSyncProvier is the file synchronization handler
type FileSyncProvier struct {
	Adapter
	chainID  string
	filename string
	state    int32
	mode     protos.File_Mode
	pkiID    common.PKIidType
	leader   bool
	payloads PayloadBuffer
	msgChan  <-chan *protos.RKSyncMessage
	reqChan  <-chan *protos.RKSyncMessage
	done     sync.WaitGroup
	stopCh   chan struct{}
	once     sync.Once
}

// Stop stops the FileSyncProvider
func (p *FileSyncProvier) Stop() {
	p.once.Do(func() {
		p.stopCh <- struct{}{}
		p.done.Wait()
		close(p.stopCh)
	})
}

func (p *FileSyncProvier) listen() {
	defer p.done.Done()

	for {
		select {
		case <-p.stopCh:
			p.stopCh <- struct{}{}
			logging.Debug("Stop listening for new messages")
			return
		case msg := <-p.msgChan:
			p.queueDataMsg(msg)
		}
	}
}

func (p *FileSyncProvier) periodicalInvocation(d time.Duration) {
	defer p.done.Done()

	for {
		select {
		case <-p.stopCh:
			p.stopCh <- struct{}{}
			return
		case <-time.After(d):
			p.requestDataAppend()
		case <-p.payloads.Ready():
			p.processPayloads()
		}
	}
}

func (p *FileSyncProvier) processDataReq() {
	defer p.done.Done()
	wg := &sync.WaitGroup{}

	for {
		select {
		case msg := <-p.reqChan:
			wg.Add(1)
			go p.handleDataReq(msg, wg)
		case <-p.stopCh:
			p.stopCh <- struct{}{}
			wg.Wait()
			return
		}
	}
}

func (p *FileSyncProvier) processPayloads() {
	logging.Debugf("[%s] Ready to process payloads, next payload start number is = [%d]", p.filename, p.payloads.Next())
	for payload := p.payloads.Pop(); payload != nil; payload = p.payloads.Pop() {
		if payload.IsAppend() {
			n, err := p.GetFileSystem().Append(p.filename, payload.Data)
			if err != nil {
				logging.Fatalf("Failed appending data to file %s: %s", p.filename, err)
				return
			}

			if int64(n) != payload.GetAppend().Length {
				logging.Fatalf("Failed to write to file, actually writes %d bytes, expects to write %d bytes", n, payload.GetAppend().Length)
				return
			}
		}
	}
}

func (p *FileSyncProvier) queueDataMsg(msg *protos.RKSyncMessage) {
	if !bytes.Equal(msg.Channel, []byte(p.chainID)) {
		logging.Warningf("Received message for channel %s while expecting channel %s, ignoring", string(msg.Channel), p.chainID)
		return
	}

	dataMsg := msg.GetDataMsg()
	if dataMsg != nil {
		payload := dataMsg.Payload
		if payload == nil {
			logging.Error("Given payload is nil")
			return
		}

		p.payloads.Push(payload)
	} else {
		logging.Warning("RKSync message received is not of data message type, usually this should not happen.")
	}
}

func (p *FileSyncProvier) handleDataReq(msg *protos.RKSyncMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	if !bytes.Equal([]byte(p.chainID), msg.Channel) {
		logging.Warningf("Received message for channel %s while expecting channel %s, ignoring", string(msg.Channel), p.chainID)
		return
	}

	if atomic.LoadInt32(&p.state) != int32(1) {
		swapped := atomic.CompareAndSwapInt32(&p.state, int32(0), int32(1))
		if !swapped {
			return
		}
		defer func() { atomic.StoreInt32(&p.state, int32(0)) }()
	}

	req := msg.GetDataReq()
	if !bytes.Equal([]byte(req.FileName), []byte(p.filename)) {
		logging.Warningf("Received message for file %s while expecting file %s, ignoring", req.FileName, p.filename)
		return
	}

	peer := p.Lookup(req.PkiId)
	if peer == nil {
		logging.Warningf("Can't find peer's information: %s", req.PkiId)
		return
	}

	if req.IsAppend() {
		if p.mode != protos.File_Append {
			logging.Warningf("File %s's mode isn't Append", p.filename)
			return
		}

		appendReq := req.GetAppend()
		fi, err := p.GetFileSystem().Stat(p.filename)
		if err != nil {
			logging.Warningf("Failed to stat file %s: %s", p.filename, err)
			return
		}

		if appendReq.Length >= fi.Size() {
			logging.Debugf("The sender's file is newer")
			return
		}

		data := make([]byte, dataBlockSize)
		start := appendReq.Length
		var n int

		for {
			n, err = p.GetFileSystem().ReaderAt(p.filename, data, start)
			if err != nil {
				if err != io.EOF {
					logging.Warningf("Read file %s failed: %s", p.filename, err)
				}
				break
			}

			sMsg, err := p.createAppendDataMsg(data, n, start)
			if err != nil {
				logging.Warningf("Failed creating DataMessage: %v", err)
				return
			}

			p.SendToPeer(sMsg, peer)
			start = start + int64(n)
		}
	}
}

func (p *FileSyncProvier) createAppendDataMsg(data []byte, n int, start int64) (*protos.SignedRKSyncMessage, error) {
	if n < len(data) {
		data = data[:n]
	}

	msg := &protos.RKSyncMessage{
		Nonce:   uint64(0),
		Channel: []byte(p.chainID),
		Tag:     protos.RKSyncMessage_CHAN_ONLY,
		Content: &protos.RKSyncMessage_DataMsg{
			DataMsg: &protos.DataMessage{
				FileName: p.filename,
				Payload: &protos.Payload{
					Data: data,
					Metadata: &protos.Payload_Append{
						Append: &protos.AppendMetadata{
							Start:  start,
							Length: int64(n),
						},
					},
				},
			},
		},
	}

	return p.Sign(msg)
}

func (p *FileSyncProvier) requestDataAppend() {
	swapped := atomic.CompareAndSwapInt32(&p.state, int32(0), int32(2))
	if !swapped {
		return
	}
	defer func() { atomic.StoreInt32(&p.state, int32(0)) }()

	req, err := p.createDataAppendMsgRequest()
	if err != nil {
		logging.Warningf("Failed creating SignedRKSyncMessage: %+v", err)
		return
	}

	endpoints := filter.SelectPeers(1, p.GetMembership(), p.IsMemberInChan)
	if len(endpoints) == 0 {
		logging.Warningf("Can't find any member in Chain: %s", p.chainID)
		return
	}

	p.SendToPeer(req, endpoints[0])
}

func (p *FileSyncProvier) createDataAppendMsgRequest() (*protos.SignedRKSyncMessage, error) {
	fi, err := p.GetFileSystem().Stat(p.filename)
	if err != nil {
		logging.Warningf("Failed to stat file %s: %s", p.filename, err)
		return nil, err
	}

	if p.mode == protos.File_Append {
		msg := &protos.RKSyncMessage{
			Nonce:   uint64(0),
			Channel: []byte(p.chainID),
			Tag:     protos.RKSyncMessage_CHAN_ONLY,
			Content: &protos.RKSyncMessage_DataReq{
				DataReq: &protos.DataRequest{
					FileName: p.filename,
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

	return nil, errors.New("Unsupported file mode")
}