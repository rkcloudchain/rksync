/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

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
	"github.com/rkcloudchain/rksync/util"
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
	Accept(acceptor common.MessageAcceptor, mac []byte, passThrough bool) (<-chan *protos.RKSyncMessage, <-chan protos.ReceivedMessage)
}

// NewFileSyncProvider creates FileSyncProvier instance
func NewFileSyncProvider(chainMac common.ChainMac, chainID string, filename string, metadata []byte, mode protos.File_Mode, leader bool,
	pkiID common.PKIidType, adapter Adapter) (*FileSyncProvier, error) {

	mac := GenerateMAC(chainMac, filename)
	p := &FileSyncProvier{
		Adapter:  adapter,
		chainMac: chainMac,
		chainID:  chainID,
		filename: filename,
		metadata: metadata,
		mode:     mode,
		leader:   leader,
		state:    int32(0),
		stopCh:   make(chan struct{}, 1),
		pkiID:    pkiID,
	}

	p.reqChan, _ = adapter.Accept(func(message interface{}) bool {
		return message.(*protos.RKSyncMessage).IsDataReq() &&
			bytes.Equal(message.(*protos.RKSyncMessage).ChainMac, chainMac) &&
			bytes.Equal([]byte(message.(*protos.RKSyncMessage).GetDataReq().FileName), []byte(filename))
	}, mac, false)

	p.done.Add(1)
	go p.processDataReq()

	if p.leader {
		return p, nil
	}

	p.msgChan, _ = adapter.Accept(func(message interface{}) bool {
		return message.(*protos.RKSyncMessage).IsDataMsg() &&
			bytes.Equal(message.(*protos.RKSyncMessage).ChainMac, chainMac) &&
			bytes.Equal([]byte(message.(*protos.RKSyncMessage).GetDataMsg().FileName), []byte(filename))
	}, mac, false)

	start, err := p.initPayloadBufferStart()
	if err != nil {
		return nil, err
	}
	p.payloads = NewPayloadBuffer(start)

	p.done.Add(2)
	go p.listen()
	go p.periodicalInvocation(4 * time.Second)

	return p, nil
}

// FileSyncProvier is the file synchronization handler
type FileSyncProvier struct {
	Adapter
	chainMac common.ChainMac
	chainID  string
	filename string
	metadata []byte
	state    int32
	mode     protos.File_Mode
	pkiID    common.PKIidType
	leader   bool
	payloads PayloadBuffer
	msgChan  <-chan *protos.RKSyncMessage
	reqChan  <-chan *protos.RKSyncMessage
	done     sync.WaitGroup
	stopCh   chan struct{}
}

func (p *FileSyncProvier) initPayloadBufferStart() (int64, error) {
	fs := p.GetFileSystem()
	fi, err := fs.Stat(p.chainID, config.FileMeta{Name: p.filename, Metadata: p.metadata, Leader: p.leader})
	if err == nil {
		return fi.Size(), nil
	}

	if !p.leader && os.IsNotExist(err) {
		logging.Debugf("Channel %s file %s does not exists, create it", p.chainMac, p.filename)
		f, err := fs.Create(p.chainID, config.FileMeta{Name: p.filename, Metadata: p.metadata, Leader: p.leader})
		if err != nil {
			logging.Errorf("Failed creating file %s (Channel %s): %s", p.filename, p.chainMac, err)
			return 0, err
		}
		f.Close()
		return 0, nil
	}

	return 0, errors.Errorf("Failed stating file %s (Channel %s): %s", p.filename, p.chainMac, err)
}

// Stop stops the FileSyncProvider
func (p *FileSyncProvier) Stop() {
	logging.Info("Stopping fsync provider")
	defer logging.Info("Stopped fsync provider")

	p.stopCh <- struct{}{}
	p.done.Wait()
}

func (p *FileSyncProvier) listen() {
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

func (p *FileSyncProvier) periodicalInvocation(d time.Duration) {
	defer p.done.Done()

	for {
		select {
		case s := <-p.stopCh:
			p.stopCh <- s
			return
		case <-time.After(d):
			swapped := atomic.CompareAndSwapInt32(&p.state, int32(0), int32(2))
			if !swapped {
				continue
			}

			p.requestDataAppend()
			atomic.StoreInt32(&p.state, int32(0))

		case <-p.payloads.Ready():
			swapped := atomic.CompareAndSwapInt32(&p.state, int32(0), int32(2))
			if !swapped {
				continue
			}

			p.processPayloads()
			atomic.StoreInt32(&p.state, int32(0))
		}
	}
}

func (p *FileSyncProvier) processDataReq() {
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
			wg.Wait()
			p.stopCh <- s
			return
		}
	}
}

func (p *FileSyncProvier) processPayloads() {
	logging.Debugf("[%s] Ready to process payloads, next payload start number is = [%d]", p.filename, p.payloads.Next())
	fs := p.GetFileSystem()
	f, err := fs.OpenFile(p.chainID, config.FileMeta{Name: p.filename, Metadata: p.metadata, Leader: p.leader}, os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		logging.Errorf("Failed opening file %s (Channel %): %s", p.filename, p.chainMac, err)
		return
	}
	defer f.Close()

	f.Lock()
	defer f.Unlock()

	for payload := p.payloads.Peek(); payload != nil; payload = p.payloads.Peek() {
		if payload.IsAppend() {
			n, err := f.Write(payload.Data)
			if err != nil {
				logging.Errorf("Failed appending data to file %s: %s", p.filename, err)
				if n > 0 {
					p.payloads.Reset(int64(n))
				}
				return
			}
			p.payloads.Expire(int64(n))
		}
	}
}

func (p *FileSyncProvier) queueDataMsg(msg *protos.RKSyncMessage) {
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

		p.payloads.Push(payload)
	} else {
		logging.Warning("RKSync message received is not of data message type, usually this should not happen.")
	}
}

func (p *FileSyncProvier) handleDataReq(msg *protos.RKSyncMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	if !bytes.Equal(p.chainMac, msg.ChainMac) {
		logging.Warningf("Received message for channel %s while expecting channel %s, ignoring", common.ChainMac(msg.ChainMac), p.chainMac)
		return
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
		fi, err := p.GetFileSystem().Stat(p.chainID, config.FileMeta{Name: p.filename, Metadata: p.metadata, Leader: p.leader})
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

		fs := p.GetFileSystem()
		f, err := fs.OpenFile(p.chainID, config.FileMeta{Name: p.filename, Metadata: p.metadata, Leader: p.leader}, os.O_RDONLY, os.ModePerm)
		if err != nil {
			logging.Errorf("Failed opening file %s (Channel %s): %s", p.filename, p.chainMac, err)
			return
		}
		defer f.Close()

		f.RLock()
		defer f.RUnlock()

		for {
			n, err = f.ReadAt(data, start)
			if err == io.EOF {
				if n > 0 {
					sMsg, err := p.createAppendDataMsg(data, n, start)
					if err != nil {
						logging.Warningf("Failed creating DataMessage: %v", err)
						return
					}
					p.SendToPeer(sMsg, peer)
				}
				return
			}
			if err != nil {
				logging.Warningf("Read file %s failed: %s", p.filename, err)
				return
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
		Nonce:    uint64(0),
		ChainMac: p.chainMac,
		Tag:      protos.RKSyncMessage_CHAN_ONLY,
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
	req, err := p.createDataAppendMsgRequest()
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

func (p *FileSyncProvier) createDataAppendMsgRequest() (*protos.SignedRKSyncMessage, error) {
	fi, err := p.GetFileSystem().Stat(p.chainID, config.FileMeta{Name: p.filename, Metadata: p.metadata, Leader: p.leader})
	if err != nil {
		logging.Warningf("Failed to stat file %s: %s", p.filename, err)
		return nil, err
	}

	if p.mode == protos.File_Append {
		msg := &protos.RKSyncMessage{
			Nonce:    uint64(0),
			ChainMac: p.chainMac,
			Tag:      protos.RKSyncMessage_CHAN_ONLY,
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

// GenerateMAC returns a byte slice that is derived from the channel's mac
// and a file name
func GenerateMAC(chainMac []byte, filename string) []byte {
	raw := append(chainMac, []byte(filename)...)
	return util.ComputeSHA3256(raw)
}
