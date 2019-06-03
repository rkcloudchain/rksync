package fsync

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/gosync"
	"github.com/rkcloudchain/gosync/syncpb"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/filter"
	"github.com/rkcloudchain/rksync/lib"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/util"
	"golang.org/x/crypto/sha3"
)

const (
	maxConnectionAttempts = 20
	defaultHelloInterval  = time.Duration(5) * time.Second
)

var (
	startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)

type blockRequester struct {
	Adapter
	blockChan         <-chan *protos.RKSyncMessage
	peer              *common.NetworkMember
	reconnectInterval time.Duration
	pubsub            *lib.PubSub
	filename          string
	pkiID             common.PKIidType
	chainMac          common.ChainMac
	stopping          int32
	stopChan          chan struct{}
}

func (r *blockRequester) DoRequest(startOffset, endOffset int64) (data []byte, err error) {
	message, err := r.createDataBlockReqMessage(startOffset, endOffset)
	if err != nil {
		logging.Errorf("Failed creating data block request message: %v", err)
		return nil, err
	}

	go r.sendUntilAcked(message)

	for i := 0; i < maxConnectionAttempts && !r.isStopping(); i++ {
		select {
		case msg := <-r.blockChan:
			dataMsg := msg.GetDataMsg()
			if blockMsg := dataMsg.GetRandom(); blockMsg != nil {
				if blockMsg.StartOffset != startOffset {
					continue
				}

				r.pubsub.Publish(fmt.Sprintf("%d", msg.Nonce), msg.Nonce)
				return blockMsg.Data, nil
			}
		case <-r.stopChan:
			return nil, errors.New("File sync provider is stopping")
		}
	}

	return nil, errors.Errorf("Can't receive file %s's block with start offset %d", r.filename, startOffset)
}

func (r *blockRequester) createDataBlockReqMessage(startOffset, endOffset int64) (*protos.SignedRKSyncMessage, error) {
	msg := &protos.RKSyncMessage{
		Nonce:    util.RandomUInt64(),
		ChainMac: r.chainMac,
		Tag:      protos.RKSyncMessage_CHAN_ONLY,
		Content: &protos.RKSyncMessage_DataReq{
			DataReq: &protos.DataRequest{
				FileName: r.filename,
				PkiId:    r.pkiID,
				Req: &protos.DataRequest_BlockReq{
					BlockReq: &protos.BlockRequest{
						StartOffset: startOffset,
						EndOffset:   endOffset,
					},
				},
			},
		},
	}

	return r.Sign(msg)
}

func (r *blockRequester) sendUntilAcked(message *protos.SignedRKSyncMessage) {
	nonce := message.Nonce
	for i := 0; i < maxConnectionAttempts && !r.isStopping(); i++ {
		sub := r.pubsub.Subscribe(fmt.Sprintf("%d", nonce), time.Second*5)
		r.SendToPeer(message, r.peer)
		if _, timeoutErr := sub.Listen(); timeoutErr == nil {
			return
		}

		time.Sleep(r.reconnectInterval)
	}
}

func (r *blockRequester) setPeer(peer *common.NetworkMember) {
	r.peer = peer
}

func (r *blockRequester) isStopping() bool {
	return atomic.LoadInt32(&r.stopping) == int32(1)
}

func (r *blockRequester) stop() {
	atomic.StoreInt32(&r.stopping, int32(1))
	r.stopChan <- struct{}{}
}

func createRandomSyncProvider(chainMac common.ChainMac, chainID string, filename string, metadata []byte, leader bool,
	pkiID common.PKIidType, adapter Adapter) (FileSyncProvider, error) {

	mac := GenerateMAC(chainMac, filename)
	provider := &randomSyncProvider{
		Adapter:  adapter,
		chainID:  chainID,
		chainMac: chainMac,
		fmeta:    &config.FileMeta{Name: filename, Metadata: metadata, Leader: leader},
		state:    int32(0),
		stopCh:   make(chan struct{}, 1),
		pkiID:    pkiID,
	}

	sizeFunc := func() (int64, error) {
		fi, err := provider.Adapter.GetFileSystem().Stat(provider.chainID, *provider.fmeta)
		if err != nil {
			return 0, err
		}
		return fi.Size(), nil
	}
	provider.requester = provider.newBlockRequester()
	provider.rsync, _ = gosync.New(&gosync.Config{
		MaxRequestBlockSize: dataBlockSize,
		Requester:           provider.requester,
		StrongHasher:        sha3.New256(),
		SizeFunc:            sizeFunc,
	})

	provider.reqChan, _ = adapter.Accept(func(message interface{}) bool {
		msg, ok := message.(*protos.RKSyncMessage)
		if !ok {
			return false
		}
		return msg.IsDataReq() && (msg.GetDataReq().GetRandom() != nil || msg.GetDataReq().GetBlockReq() != nil) &&
			bytes.Equal(msg.ChainMac, chainMac) && bytes.Equal([]byte(msg.GetDataReq().FileName), []byte(filename))
	}, mac, false)

	provider.done.Add(1)
	go provider.processDataReq()

	if leader {
		return provider, nil
	}

	provider.patcherChan, _ = adapter.Accept(func(message interface{}) bool {
		msg, ok := message.(*protos.RKSyncMessage)
		if !ok {
			return false
		}
		return msg.IsDataMsg() && msg.GetDataMsg().GetPatcher() != nil &&
			bytes.Equal(msg.ChainMac, chainMac) && bytes.Equal([]byte(msg.GetDataMsg().FileName), []byte(filename))
	}, mac, false)

	err := provider.initialize()
	if err != nil {
		return nil, err
	}

	provider.done.Add(2)
	go provider.listen()
	go provider.periodicalInvocation(4 * time.Second)

	return provider, nil
}

type randomSyncProvider struct {
	Adapter
	chainID     string
	chainMac    common.ChainMac
	pkiID       common.PKIidType
	fmeta       *config.FileMeta
	rsync       gosync.GoSync
	state       int32
	stopCh      chan struct{}
	done        sync.WaitGroup
	pubsub      *lib.PubSub
	reqChan     <-chan *protos.RKSyncMessage
	patcherChan <-chan *protos.RKSyncMessage
	requester   gosync.BlockRequester
}

func (p *randomSyncProvider) initialize() error {
	_, err := p.Adapter.GetFileSystem().Stat(p.chainID, *p.fmeta)
	if os.IsNotExist(err) {
		_, err = p.Adapter.GetFileSystem().Create(p.chainID, *p.fmeta)
		if err != nil {
			return err
		}

		err = p.Adapter.GetFileSystem().Chtimes(p.chainID, *p.fmeta, startTime)
		return err
	}
	return err
}

func (p *randomSyncProvider) Stop() {
	logging.Info("Stopping random fsync provider")
	defer logging.Info("Stopped random fsync provider")

	p.stopCh <- struct{}{}
	p.done.Wait()
}

func (p *randomSyncProvider) newBlockRequester() *blockRequester {
	r := &blockRequester{
		Adapter:           p.Adapter,
		reconnectInterval: defaultHelloInterval,
		pubsub:            lib.NewPubSub(),
		filename:          p.fmeta.Name,
		pkiID:             p.pkiID,
		chainMac:          p.chainMac,
		stopping:          int32(0),
		stopChan:          make(chan struct{}, 1),
	}

	r.blockChan, _ = p.Adapter.Accept(func(message interface{}) bool {
		msg, ok := message.(*protos.RKSyncMessage)
		if !ok {
			return false
		}
		return msg.IsDataMsg() && msg.GetDataMsg().GetRandom() != nil &&
			bytes.Equal(msg.ChainMac, p.chainMac) && bytes.Equal([]byte(msg.GetDataMsg().FileName), []byte(p.fmeta.Name))
	}, p.chainMac, false)

	return r
}

func (p *randomSyncProvider) processDataReq() {
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

func (p *randomSyncProvider) listen() {
	defer p.done.Done()

	for {
		select {
		case s := <-p.stopCh:
			p.stopCh <- s
			logging.Debug("Stop listening for new messages")
			return

		case msg := <-p.patcherChan:
			p.handlePatcherMsg(msg)
		}
	}
}

func (p *randomSyncProvider) handlePatcherMsg(msg *protos.RKSyncMessage) {
	if !bytes.Equal(p.chainMac, msg.ChainMac) {
		logging.Warningf("Received message for channel %s while expecting channel %s, ignoring", common.ChainMac(msg.ChainMac), p.chainMac)
		return
	}

	dataMsg := msg.GetDataMsg()
	if !bytes.Equal([]byte(dataMsg.FileName), []byte(p.fmeta.Name)) {
		logging.Warningf("Received message for file %s while expecting file %s, ignoring", dataMsg.FileName, p.fmeta.Name)
		return
	}

	if patcher := dataMsg.GetPatcher(); patcher != nil {
		ft, err := ioutil.TempFile("", "rksync_")
		if err != nil {
			logging.Errorf("Failed creating temp file: %v", err)
			return
		}
		defer os.Remove(ft.Name())
		defer ft.Close()

		err = p.patchLocalTempFile(patcher.Patcher, ft)
		if err != nil {
			logging.Errorf("Failed patching local temp file: %s", err)
			return
		}

		writer, err := p.Adapter.GetFileSystem().OpenFile(p.chainID, *p.fmeta, os.O_WRONLY|os.O_TRUNC, os.ModePerm)
		if err != nil {
			logging.Errorf("Failed openning file %s with O_WRONLY flag: %v", p.fmeta.Name, err)
			return
		}
		defer writer.Close()

		_, err = io.Copy(writer, ft)
		if err != nil {
			logging.Errorf("Failed copying temp file to destination: %s", err)
			return
		}

		err = p.Adapter.GetFileSystem().Chtimes(p.chainID, *p.fmeta, time.Unix(0, patcher.ModTime))
		if err != nil {
			logging.Errorf("Failed changing file %s's mod time: %v", p.fmeta.Name, err)
		}
	}
}

func (p *randomSyncProvider) patchLocalTempFile(patcher *syncpb.PatcherBlockSpan, output io.Writer) error {
	reader, err := p.Adapter.GetFileSystem().OpenFile(p.chainID, *p.fmeta, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer reader.Close()

	return p.rsync.Patch(reader, patcher, output)
}

func (p *randomSyncProvider) handleDataReq(msg *protos.RKSyncMessage, wg *sync.WaitGroup) {
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

	if random := req.GetRandom(); random != nil {
		p.handleDataPatcherReq(random, peer)
		return
	}

	if block := req.GetBlockReq(); block != nil {
		p.handleDataBlockReq(block, msg.Nonce, peer)
		return
	}
}

func (p *randomSyncProvider) handleDataPatcherReq(req *protos.RandomRequest, peer *common.NetworkMember) {
	fi, err := p.GetFileSystem().Stat(p.chainID, *p.fmeta)
	if err != nil {
		logging.Warningf("Failed to stat file %s: %s", p.fmeta.Name, err)
		return
	}

	if fi.ModTime().UnixNano() <= req.ModTime {
		logging.Debug("The sender's file is newer")
		return
	}

	reader, err := p.GetFileSystem().OpenFile(p.chainID, *p.fmeta, os.O_RDONLY, os.ModePerm)
	if err != nil {
		logging.Errorf("Failed openning file %s: %s", p.fmeta.Name, err)
		return
	}
	defer reader.Close()

	patcher, err := p.rsync.Delta(reader, req.Checksums)
	if err != nil {
		logging.Errorf("Failed computing file %s delta: %s", p.fmeta.Name, err)
		return
	}

	pmsg, err := p.createRandomPatcherMsg(patcher, fi.ModTime().UnixNano())
	if err != nil {
		logging.Warningf("Failed creating patcher message: %v", err)
		return
	}

	p.SendToPeer(pmsg, peer)
}

func (p *randomSyncProvider) handleDataBlockReq(req *protos.BlockRequest, nonce uint64, peer *common.NetworkMember) {
	reader, err := p.GetFileSystem().OpenFile(p.chainID, *p.fmeta, os.O_RDONLY, os.ModePerm)
	if err != nil {
		logging.Errorf("Failed openning file %s: %s", p.fmeta.Name, err)
		return
	}
	defer reader.Close()

	if _, err = reader.Seek(req.StartOffset, io.SeekStart); err != nil {
		logging.Errorf("Failed seeking file %s to %d: %s", p.fmeta.Name, req.StartOffset, err)
		return
	}

	buffer := make([]byte, req.EndOffset-req.StartOffset+1)
	n, err := io.ReadFull(reader, buffer)
	if err != nil && err != io.ErrUnexpectedEOF {
		logging.Errorf("Failed reading file %s: %s", p.fmeta.Name, err)
		return
	}

	msg, err := p.createRandomBlockMsg(req.StartOffset, nonce, buffer[:n])
	if err != nil {
		logging.Errorf("Failed creating random block message: %s", err)
		return
	}

	p.SendToPeer(msg, peer)
}

func (p *randomSyncProvider) createRandomBlockMsg(startOffset int64, nonce uint64, data []byte) (*protos.SignedRKSyncMessage, error) {
	msg := &protos.RKSyncMessage{
		Nonce:    nonce,
		ChainMac: p.chainMac,
		Tag:      protos.RKSyncMessage_CHAN_ONLY,
		Content: &protos.RKSyncMessage_DataMsg{
			DataMsg: &protos.DataMessage{
				FileName: p.fmeta.Name,
				PkiId:    p.pkiID,
				Payload: &protos.DataMessage_Random{
					Random: &protos.RandomPayload{
						StartOffset: startOffset,
						Data:        data,
					},
				},
			},
		},
	}

	return p.Sign(msg)
}

func (p *randomSyncProvider) createRandomPatcherMsg(patcher *syncpb.PatcherBlockSpan, modTime int64) (*protos.SignedRKSyncMessage, error) {
	msg := &protos.RKSyncMessage{
		Nonce:    uint64(0),
		ChainMac: p.chainMac,
		Tag:      protos.RKSyncMessage_CHAN_ONLY,
		Content: &protos.RKSyncMessage_DataMsg{
			DataMsg: &protos.DataMessage{
				FileName: p.fmeta.Name,
				PkiId:    p.pkiID,
				Payload: &protos.DataMessage_Patcher{
					Patcher: &protos.RandomPatcher{
						ModTime: modTime,
						Patcher: patcher,
					},
				},
			},
		},
	}

	return p.Sign(msg)
}

func (p *randomSyncProvider) periodicalInvocation(d time.Duration) {
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

			p.requestDataRandom()
			atomic.StoreInt32(&p.state, int32(0))
		}
	}
}

func (p *randomSyncProvider) requestDataRandom() {
	req, err := p.createDataRandomRequest()
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

func (p *randomSyncProvider) createDataRandomRequest() (*protos.SignedRKSyncMessage, error) {
	reader, err := p.GetFileSystem().OpenFile(p.chainID, *p.fmeta, os.O_RDONLY, os.ModePerm)
	if err != nil {
		logging.Warningf("Failed to open file %s to read: %s", p.fmeta.Name, err)
		return nil, err
	}
	defer reader.Close()

	checksums := p.rsync.Sign(reader)

	fi, err := p.GetFileSystem().Stat(p.chainID, *p.fmeta)
	if err != nil {
		logging.Warningf("Failed to stat file: %s: %s", p.fmeta.Name, err)
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
				Req: &protos.DataRequest_Random{
					Random: &protos.RandomRequest{
						ModTime:   fi.ModTime().UnixNano(),
						Checksums: checksums,
					},
				},
			},
		},
	}

	return p.Sign(msg)
}
