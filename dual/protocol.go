package dual

import (
	"context"
	"errors"
	"fmt"
	"github.com/git-disl/GRING"
	"go.uber.org/zap"
	"time"
	"strings"
	"sync"
	"github.com/VictoriaMetrics/fastcache"
	"bytes"
	"encoding/json"
	"math"
	"math/rand"
)

//protocol opcode
const (
	//OP_BROADCAST                 = 0x01 //no use

	// messaging with group routing table
        OP_GROUP_MULTICAST           = 0x02 // register outbound target in group routing table
	OP_REQUEST_GROUP             = 0x03
	OP_REQUEST_GROUP_SUB         = 0x04
        OP_REQUEST_JOIN              = 0x05
        OP_JOIN_GROUP                = 0x06
        OP_FED_COMP                  = 0x07
        OP_REPORT                    = 0x08

	OP_ONE_TIME                  = 0x09
        OP_NEW_CONN                  = 0x10
        OP_DISCOVER                  = 0x11

	// hybrid fed comp
	OP_FED_COMP_PUSH             = 0x12
	OP_FED_COMP_PULL_REQ         = 0x13

	//OP_FED_COMP_PULL             = 0x14 // no use
	//OP_FED_COMP_PULL_RESP        = 0x15 // no use
	//OP_FED_COMP_PULL_RESP_DENY   = 0x16 // no use

	// general gossip messaging
	OP_GOSSIP_PUSH_ONLY          = 0x14
	OP_GOSSIP_PUSH               = 0x15
	OP_GOSSIP_PULL_REQ           = 0x16
	OP_REQUEST_REGROUP  		 = 0x17
)

//Identification state
const (
        ID_NOBODY                   = 0x00
        ID_PUBLISHER                = 0x01
        ID_INITIATOR                = 0x02
        ID_SUBLEADER                = 0x03
        ID_WORKER                   = 0x04
)

// BucketSize returns the capacity, or the total number of peer ID entries a single routing table bucket may hold.
const BucketSize int = 16

// printedLength is the total prefix length of a public key associated to a chat users ID.
const printedLength = 8

// ErrBucketFull is returned when a routing table bucket is at max capacity.
var ErrBucketFull = errors.New("bucket is full")


// Protocol implements routing/discovery portion of the Kademlia protocol with improvements suggested by the
// S/Kademlia paper. It is expected that Protocol is bound to a GRING.Node via (*GRING.Node).Bind before the node
// starts listening for incoming peers.
type Protocol struct {
	node   *GRING.Node

	logger *zap.Logger

	table_bc  *Table
	table_gr  *Table // TODO : multiple project
        group_size int // TODO : multiple project
	num_members int // # of aggregation
	tot_members int // TODO : move to PubSub. Used by grouping protocol.
	report bool

	maxNeighborsBC int
	maxNeighborsGR int
	leader string
	id_state byte

	events Events

	pingTimeout time.Duration

	mutex *sync.Mutex // for group protocol

	seen_push *fastcache.Cache
	seen_pull *fastcache.Cache
	seen_relay *fastcache.Cache
	mutex_push *sync.Mutex
	mutex_pull *sync.Mutex
	mutex_relay *sync.Mutex

        gossipmsg DownloadMessage
	currentUUID string
	inUse bool
	mutex_msg *sync.Mutex

	pubsub *PubSub
	mutex_pubsub *sync.Mutex // for group protocol

	userinfoPool [][]byte
}

// New returns a new instance of the Kademlia protocol.
func New(opts ...ProtocolOption) *Protocol {
	p := &Protocol{
		pingTimeout: 3 * time.Second,
	        mutex: new(sync.Mutex),
		maxNeighborsBC: 128,
		maxNeighborsGR: 16,
		seen_push: fastcache.New(64 << 20),
		seen_pull: fastcache.New(64 << 20),
		seen_relay: fastcache.New(64 << 20),
		mutex_push: new(sync.Mutex),
		mutex_pull: new(sync.Mutex),
		mutex_relay: new(sync.Mutex),
		mutex_msg: new(sync.Mutex),
		mutex_pubsub: new(sync.Mutex),
		report: false,
		userinfoPool: make([][]byte, 0),
	}


	for _, opt := range opts {
		opt(p)
	}

	return p
}

func powInt(x, y int) int {
	return int(math.Pow(float64(x), float64(y)))
}

func hash(id GRING.ID, data []byte) []byte {
	return append(id.ID[:], data...)
}

func(p *Protocol) MarkSeenPush(target GRING.ID, euuid []byte) {
	// unmarshal
	var uuid string
	err := json.Unmarshal(euuid, &uuid)
	if err != nil {
		fmt.Printf("Error on decode process: %v\n", err)
	}
	p.mutex_push.Lock()
	p.seen_push.Set(hash(target, []byte(uuid)), nil)
	p.mutex_push.Unlock()
}

func(p *Protocol) markSeenPull(target GRING.ID, euuid []byte) {
	// unmarshal
	var uuid string
	err := json.Unmarshal(euuid, &uuid)
	if err != nil {
		fmt.Printf("Error on decode process: %v\n", err)
	}
	p.mutex_pull.Lock()
	p.seen_pull.Set(hash(target, []byte(uuid)), nil)
	p.mutex_pull.Unlock()
}

func(p *Protocol) markSeenRelay(target GRING.ID, euuid []byte) {
	// unmarshal
	var uuid string
	err := json.Unmarshal(euuid, &uuid)
	if err != nil {
		fmt.Printf("Error on decode process: %v\n", err)
	}
	p.mutex_relay.Lock()
	p.seen_relay.Set(hash(target, []byte(uuid)), nil)
	p.mutex_relay.Unlock()
}

func(p *Protocol) HasSeenPush(target GRING.ID, euuid []byte) bool{
	// unmarshal
	var uuid string
	err := json.Unmarshal(euuid, &uuid)
	if err != nil {
		fmt.Printf("Error on decode process: %v\n", err)
	}
	p.mutex_push.Lock()
	self := hash(target, []byte(uuid))
	if p.seen_push.Has(self) {
		p.mutex_push.Unlock()
		return true
	}
	p.mutex_push.Unlock()
	return false
}

func(p *Protocol) hasSeenPull(target GRING.ID, euuid []byte) bool{
	// unmarshal
	var uuid string
	err := json.Unmarshal(euuid, &uuid)
	if err != nil {
		fmt.Printf("Error on decode process: %v\n", err)
	}
	p.mutex_pull.Lock()
	self := hash(target, []byte(uuid))
	if p.seen_pull.Has(self) {
		p.mutex_pull.Unlock()
		return true
	}
	p.mutex_pull.Unlock()
	return false
}

func(p *Protocol) hasSeenRelay(target GRING.ID, euuid []byte) bool{
	// unmarshal
	var uuid string
	err := json.Unmarshal(euuid, &uuid)
	if err != nil {
		fmt.Printf("Error on decode process: %v\n", err)
	}
	p.mutex_relay.Lock()
	self := hash(target, []byte(uuid))
	if p.seen_relay.Has(self) {
		p.mutex_relay.Unlock()
		return true
	}
	p.mutex_relay.Unlock()
	return false
}

func(p *Protocol) SetTotMembers(totmem int) {
	p.tot_members = totmem
}

func(p *Protocol) GetTotMembers() int {
	return p.tot_members
}

func(p *Protocol) SetNumMembers(mem int) {
	p.num_members = mem
}

func(p *Protocol) GetNumMembers() int {
	return p.num_members
}

func(p *Protocol) SetGroupSize(size int) {
	p.group_size = size
}

func(p *Protocol) GetGroupSize() int {
	return p.group_size
}

func(p *Protocol) SetReport(b bool) {
	p.report = b
}

func(p *Protocol) GetReport() bool {
	return p.report
}

func(p *Protocol) SetUse() {
	p.mutex_msg.Lock()
	p.inUse = true
	p.mutex_msg.Unlock()
}

func(p *Protocol) DoneUse() {
	p.mutex_msg.Lock()
	p.inUse = false
	p.mutex_msg.Unlock()
}

func(p *Protocol) SetItem(msg DownloadMessage){
	p.gossipmsg = msg
}

func(p *Protocol) GetItem() DownloadMessage {
	return p.gossipmsg
}

func(p *Protocol) HasUUID(euuid []byte) bool{
	// unmarshal
	var uuid string
	err := json.Unmarshal(euuid, &uuid)
	if err != nil {
		fmt.Printf("Error on decode process: %v\n", err)
	}
	// Comparing slice
	res := bytes.Compare([]byte(uuid), []byte(p.currentUUID))
	//res := bytes.Compare(uuid, p.currentUUID)
	if res == 0 {
		//fmt.Println("I have the item")
		return true
	} else {
		//fmt.Println("I don't have the item")
		return false
	}
}

func(p *Protocol) SetUUID(euuid []byte){
	// unmarshal
	var uuid string
	err := json.Unmarshal(euuid, &uuid)
	if err != nil {
		fmt.Printf("Error on decode process: %v\n", err)
	}
	p.currentUUID = uuid
}

// Find executes the FIND_NODE S/Kademlia RPC call to find the closest peers to some given target public key. It
// returns the IDs of the closest peers it finds.
func (p *Protocol) Find(target GRING.PublicKey, isBootstrap bool, opts ...IteratorOption) []GRING.ID {
	//return NewIterator(p.node, p.table_bc, opts...).Find(target)
	closest := NewIterator(p.node, p.table_bc, opts...).Find(target)

	if isBootstrap {
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	    for _,id := range closest {
			//fmt.Printf("Ping to found node %s\n",id.Address)
			p.Ping(ctx, id.Address, OP_DISCOVER)
		}
	}

	return closest
}

// Discover attempts to discover new peers to your node through peers your node  already knows about by calling
// the FIND_NODE S/Kademlia RPC call with your nodes ID.
func (p *Protocol) DiscoverLocal(isBootstrap bool, opts ...IteratorOption) []GRING.ID {
	return p.Find(p.node.ID().ID, isBootstrap, opts...)
}

// Discover attempts to discover new peers to your node through peers your node  already knows about by calling
// the FIND_NODE S/Kademlia RPC call with your nodes ID.
func (p *Protocol) DiscoverRandom(isBootstrap bool, opts ...IteratorOption) []GRING.ID {
	pub, _, _ := GRING.GenerateKeys(nil)
	//fmt.Printf("DiscoverRandom with ID %s\n",pub.String())
	return p.Find(pub, isBootstrap, opts...)
}

// Ping sends a ping request to addr, and returns no error if a p!!!ong is received back before ctx has expired/was
// cancelled. It also throws an error if the connection to addr intermittently drops, or if handshaking with addr
// should there be no live connection to addr yet fails.
func (p *Protocol) Ping(ctx context.Context, addr string, opcode ...byte) error {
	//fmt.Printf("protocol ping \n")
	msg, err := p.node.RequestMessage(ctx, addr, Ping{}, opcode...)
	if err != nil {
		return fmt.Errorf("failed to ping: %w", err)
	}

	if _, ok := msg.(Pong); !ok {
		return errors.New("did not get a pong back")
	}
	//fmt.Printf("receive protocol ping \n")

	return nil
}

func (p *Protocol) Lock() {
	p.mutex.Lock()
}

func (p *Protocol) Unlock() {
	p.mutex.Unlock()
}

// MaxNeighbors returns cap of routing table
func (p *Protocol) MaxNeighborsGR() int {
	return p.maxNeighborsGR
}

func (p *Protocol) SetMaxNeighborsGR(max int){
	p.maxNeighborsGR = max
}

// MaxNeighbors returns cap of routing table
func (p *Protocol) MaxNeighborsBC() int {
	return p.maxNeighborsBC
}

// GetLeader returns the address of leader node
func (p *Protocol) GetLeader() string {
	return p.leader
}

// SetLeader sets the address of leader node
func (p *Protocol) SetLeader(addr string) {
	p.leader = addr
}

// GetID returns the identification of myself
func (p *Protocol) GetID() byte {
	return p.id_state
}

// SetID sets the identification of myself
func (p *Protocol) SetID(id byte) {
	p.id_state = id
}

// Table returns this Kademlia p's routing table from your nodes perspective.
func (p *Protocol) Table_bc() *Table {
	return p.table_bc
}

func (p *Protocol) Table_gr() *Table {
	return p.table_gr
}

func (p *Protocol) PubSub() *PubSub {
	return p.pubsub
}

func (p *Protocol) RemoveConn(table *Table, id GRING.ID, isInbound bool) {
        if  table.Recorded(id.ID) {
		/*
				    if isInbound {
			                client, exists := p.node.InboundGet(id.Address)
			                if exists {
			                    client.WaitUntilClosed()
			                }
				    }else{
			                client, exists := p.node.OutboundGet(id.Address)
			                if exists {
			                    client.WaitUntilClosed()
			                }
				    }
		*/
		if entryid, deleted := table.Delete(id.ID); deleted {
			/*
				                fmt.Printf("Peer %s(%s) is deleted from routing table\n",
				                       entryid.Address,
					               entryid.ID.String()[:printedLength],
					        )
			*/
			if p.events.OnPeerEvicted != nil {
				//fmt.Printf("PeerEvicted due to RemoveConn()\n")
				p.events.OnPeerEvicted(entryid)
			}
			return
		}
		/*
		   fmt.Printf("Peer %s(%s) is not eleted\n",
		          id.Address,
		          id.ID.String()[:printedLength],
		         )
		*/
	}
}

func (p *Protocol) ReplaceConn(id GRING.ID, isInbound bool) {
        if  p.table_bc.Recorded(id.ID) == false {
		//fmt.Printf("%s(%s) is not registered yet. Register first.\n",id.Address, id.ID.String()[:printedLength])
		inserted, err := p.table_bc.Update(id)
		if err == nil {
			if inserted {
				//fmt.Printf("inserted isInbound %v peer_id %s peer_addr %s\n", isInbound, id.String()[:printedLength], id.Address)

				if p.events.OnPeerAdmitted_bc != nil {
					p.events.OnPeerAdmitted_bc(id)
				}

				//p.table_bc.SetDirection(id.ID, isInbound)
			} else {
				if p.events.OnPeerActivity != nil {
					p.events.OnPeerActivity(id)
				}
			}
		}
		/*
		   //debug log
		   for _, en := range p.table_bc.Entries() {
		       fmt.Println(en)
		   }
		*/
	}
	/*
	           p.mutex_pubsub.Lock()
	           var replaceInbound bool
	   	if p.table_bc.GetNumInBound() >= p.table_bc.GetNumOutBound() {
	   	    replaceInbound = true
	   	}else{
	   	    replaceInbound = false
	   	}
	*/
	entries := p.table_bc.KEntries(p.maxNeighborsBC)
	for {
		if len(entries) == 0 { // code should not be here.
			break
		}
		entry := entries[len(entries)-1]
		entries = entries[:len(entries)-1]
		if id.ID == entry.ID {
			continue
		}
		/*
			    isInboundDirection, found := p.table_bc.GetDirection(entry.ID)
			    if found {
			        if isInboundDirection { // true : inbound
			            if replaceInbound {
			                //do replace
			            }else{
			                continue // we have more outbound. We need to find outbound to replace
				    }
			        }else{ // false : outbound
				    if replaceInbound {
				        continue // we have more inbound. We need to find inbound to replace
				    }else{
				        //do replace
				    }
			        }
			        p.table_bc.RemoveDirection(entry.ID)
			    }else{
				//fmt.Printf("direction not found\n")
			    }
		*/

		p.RemoveConn(p.table_bc, entry, isInbound)
		break
	}
	//p.mutex_pubsub.Unlock()
}

// Ack attempts to insert a peer ID into your nodes routing table. If the routing table bucket in which your peer ID
// was expected to be inserted on is full, the peer ID at the tail of the bucket is pinged. If the ping fails, the
// peer ID at the tail of the bucket is evicted and your peer ID is inserted to the head of the bucket.
func (p *Protocol) Ack_bc(id GRING.ID, isInbound bool, opcode ...byte) {

	if p.events.OnPeerContacted != nil {
		p.events.OnPeerContacted(id)
	}

	if p.table_bc.NumEntries() >= (p.maxNeighborsBC) {
		//if len(p.table_bc.Entries()) > (p.maxNeighborsBC) {
		//fmt.Errorf("Table BC is full \n")

		if len(opcode) != 0 && (opcode[0] == byte(OP_NEW_CONN) || opcode[0] == byte(OP_DISCOVER)) {
			//fmt.Printf("replace with NEW_CONN %s %s\n",id.Address, id.ID.String()[:printedLength])
			p.ReplaceConn(id, isInbound) // register current conn req and delete one of the existing one
		}
		return
	}

	for {
		inserted, err := p.table_bc.Update(id)
		if err == nil {
			if inserted {
				//fmt.Printf("inserted bucket[%d] isInbound %v peer_id %s peer_addr %s\n",p.table_bc.getBucketIndex(id.ID), isInbound, id.String()[:printedLength], id.Address)

				//p.table_bc.SetDirection(id.ID, isInbound)

				if p.events.OnPeerAdmitted_bc != nil {
					p.events.OnPeerAdmitted_bc(id)
				}

			} else {
				if p.events.OnPeerActivity != nil {
					p.events.OnPeerActivity(id)
				}
			}
			return
		}

		last := p.table_bc.Last(id.ID)

		ctx, cancel := context.WithTimeout(context.Background(), p.pingTimeout)
		pong, err := p.node.RequestMessage(ctx, last.Address, Ping{})
		cancel()

		if err != nil {
			if id, deleted := p.table_bc.Delete(last.ID); deleted {
				//fmt.Printf("Peer was evicted from routing table_bc by failing RequestMessage\n")
				if p.events.OnPeerEvicted != nil {
					p.events.OnPeerEvicted(id)
				}
			}
			continue
		}

		if _, ok := pong.(Pong); !ok {
			if id, deleted := p.table_bc.Delete(last.ID); deleted {
				//fmt.Printf("Peer was evicted from routing table_bc by failing to be pinged.\n")
				if p.events.OnPeerEvicted != nil {
					p.events.OnPeerEvicted(id)
				}
			}
			continue
		}

		//fmt.Printf("failed to be inserted table_bc as it's intended bucket[%d] full. id %s addr %s\n",p.table_bc.getBucketIndex(id.ID), id.ID.String()[:printedLength], id.Address)
		//fmt.Printf("target: %08b\n self: %08b l:%d\n", id.ID[:], p.node.ID().ID,p.table_bc.getBucketIndex(id.ID))
		if p.events.OnPeerEvicted != nil {
			p.events.OnPeerEvicted(id)
		}

		return
	}
}


// Ack attempts to insert a peer ID into your nodes routing table. If the routing table bucket in which your peer ID
// was expected to be inserted on is full, the peer ID at the tail of the bucket is pinged. If the ping fails, the
// peer ID at the tail of the bucket is evicted and your peer ID is inserted to the head of the bucket.
func (p *Protocol) Ack_gr(id GRING.ID) {

	if p.table_gr.NumEntries() > p.maxNeighborsGR {
		//fmt.Errorf("Table GR is full \n")
		return
	}

	for {
		inserted, err := p.table_gr.Update(id)
		if err == nil {
			if inserted {
				p.logger.Debug("Peer was inserted into routing table_gr.",
					zap.String("peer_id", id.String()),
					zap.String("peer_addr", id.Address),
				)
			}

			if inserted {
				if p.events.OnPeerAdmitted_gr != nil {
					p.events.OnPeerAdmitted_gr(id)
				}
			} else {
				if p.events.OnPeerActivity != nil {
					p.events.OnPeerActivity(id)
				}
			}

			return
		}

		last := p.table_gr.Last(id.ID)

		ctx, cancel := context.WithTimeout(context.Background(), p.pingTimeout)
		pong, err := p.node.RequestMessage(ctx, last.Address, Ping{})
		cancel()

		if err != nil {
			if id, deleted := p.table_gr.Delete(last.ID); deleted {
				p.logger.Debug("Peer was evicted from routing table_gr by failing RequestMessage",
					zap.String("peer_id", id.String()),
					zap.String("peer_addr", id.Address),
					zap.Error(err),
				)

				//fmt.Printf("Peer was evicted from routing table_gr by failing RequestMessage\n")
				if p.events.OnPeerEvicted != nil {
					p.events.OnPeerEvicted(id)
				}
			}
			continue
		}

		if _, ok := pong.(Pong); !ok {
			if id, deleted := p.table_gr.Delete(last.ID); deleted {
				p.logger.Debug("Peer was evicted from routing table_gr by failing to be pinged.",
					zap.String("peer_id", id.String()),
					zap.String("peer_addr", id.Address),
					zap.Error(err),
				)

				//fmt.Printf("Peer was evicted from routing table_gr by failing to b pinged.\n")
				if p.events.OnPeerEvicted != nil {
					p.events.OnPeerEvicted(id)
				}
			}
			continue
		}

		p.logger.Debug("Peer failed to be inserted into routing table_gr as it's intended bucket is full.",
			zap.String("peer_id", id.String()),
			zap.String("peer_addr", id.Address),
		)

		//fmt.Printf("Peer was evicted from routing table_gr as it's intended bucket is full.\n")
		if p.events.OnPeerEvicted != nil {
			p.events.OnPeerEvicted(id)
		}

		return
	}
}

// Protocol returns a GRING.Protocol that may registered to a node via (*GRING.Node).Bind.
func (p *Protocol) Protocol() GRING.Protocol {
	return GRING.Protocol{
		Bind:            p.Bind,
		OnPeerConnected: p.OnPeerConnected,
		OnPingFailed:    p.OnPingFailed,
		OnMessageSent:   p.OnMessageSent,
		OnMessageRecv:   p.OnMessageRecv,
		OnExistingConnection:   p.OnExistingConnection,
	}
}

// Bind registers messages Ping, Pong, FindNodeRequest, FindNodeResponse, and handles them by registering the
// (*Protocol).Handle Handler.
func (p *Protocol) Bind(node *GRING.Node) error {
	p.node = node
	p.table_bc = NewTable(p.node.ID())
	p.table_gr = NewTable(p.node.ID())
	p.pubsub = NewPubSub()

	if p.logger == nil {
		p.logger = p.node.Logger()
	}

	node.RegisterMessage(Ping{}, UnmarshalPing)
	node.RegisterMessage(Pong{}, UnmarshalPong)
	node.RegisterMessage(FindNodeRequest{}, UnmarshalFindNodeRequest)
	node.RegisterMessage(FindNodeResponse{}, UnmarshalFindNodeResponse)
	node.RegisterMessage(P2pMessage{}, unmarshalP2pMessage)

	// PubSub
	node.RegisterMessage(ADMessage{}, UnmarshalADMessage) // no need to register for now. GossipMessage will carry this as contents
	node.RegisterMessage(SubsMessage{}, UnmarshalSubsMessage) // no need to register. RelayMessage will carry this as contents
	node.RegisterMessage(GossipMessage{}, UnmarshalGossipMessage)
	node.RegisterMessage(Request{}, UnmarshalRequest)
	node.RegisterMessage(DownloadMessage{}, UnmarshalDownloadMessage)
	node.RegisterMessage(Deny{}, UnmarshalDeny)
	node.RegisterMessage(RelayMessage{}, UnmarshalRelayMessage)

	node.Handle(p.Handle)


	return nil
}

func (p *Protocol) OnExistingConnection(isInbound bool, client *GRING.Client, opcode ...byte) {
	if len(opcode) == 0 {
		//fmt.Printf("OnExistingConnection no opcode \n")
		return
	}
	if opcode[0] == byte(OP_GROUP_MULTICAST) && !isInbound {
		//fmt.Printf("OnExistingConnection GROUP MULTICAST\n")
		p.Ack_gr(client.ID())
	}
}

// OnPeerConnected attempts to acknowledge the new peers existence by placing its entry into your nodes' routing table
// via (*Protocol).Ack.
func (p *Protocol) OnPeerConnected(isInbound bool, client *GRING.Client, opcode ...byte) {
	if len(opcode) != 0 && opcode[0] == byte(OP_ONE_TIME) {
		//fmt.Printf("One time message without registering in routing table\n")
		return
	}

	if len(opcode) != 0 && opcode[0] == byte(OP_GROUP_MULTICAST) && !isInbound {
		//fmt.Printf(" OnPeerConnected GROUP MULTICAST opcode\n")
		p.Ack_gr(client.ID())
		return
	}

	p.Ack_bc(client.ID(), isInbound, opcode...)

	return
}

// OnPingFailed evicts peers that your node has failed to dial.
func (p *Protocol) OnPingFailed(addr string, err error) {
	// TODO: table_bc and table_gr
	if id, deleted := p.table_bc.DeleteByAddress(addr); deleted {
		//fmt.Printf("Peer was evicted from routing table_bc by OnPingFailed.\n")
		if p.events.OnPeerEvicted != nil {
			p.events.OnPeerEvicted(id)
		}
	}
}

// OnMessageSent implements GRING.Protocol and attempts to push the position in which the clients ID resides in
// your nodes' routing table's to the head of the bucket it reside within.
func (p *Protocol) OnMessageSent(client *GRING.Client, opcode ...byte) {
	if len(opcode) != 0 && opcode[0] == byte(OP_ONE_TIME) {
		//fmt.Printf("One time message without registering in routing table\n")
    }else{
		p.Ack_bc(client.ID(), false, opcode...)
	}
}

// OnMessageRecv implements GRING.Protocol and attempts to push the position in which the clients ID resides in
// your nodes' routing table's to the head of the bucket it reside within.
func (p *Protocol) OnMessageRecv(client *GRING.Client, opcode ...byte) {
	if len(opcode) != 0 && opcode[0] == byte(OP_ONE_TIME) {
		//fmt.Printf("One time message without registering in routing table\n")
    }else{
		p.Ack_bc(client.ID(), true, opcode...)
	}
}

func MarshalPeers(peers []Subscriber) []byte {
	newDict := make(map[string]Subscriber)
	for _, key := range peers {
		newDict[key.Key] = key
	}
	data, _ := json.Marshal(newDict)
	return data
}

type RegroupData struct {
	Peers         map[string]Subscriber
	Publisher     string
	ModelMetadata []byte
	NextRound     int
}

// Handle implements GRING.Protocol and handles Ping and FindNodeRequest messages.
func (p *Protocol) Handle(ctx GRING.HandlerContext) error {
	msg, err := ctx.DecodeMessage()
	if err != nil {
		return nil
	}

	switch msg := msg.(type) {
	case Ping:
		if !ctx.IsRequest() {
			return errors.New("got a ping that was not sent as a request")
		}
		return ctx.SendMessage(Pong{})
	case FindNodeRequest:
		if !ctx.IsRequest() {
			return errors.New("got a find node request that was not sent as a request")
		}
		return ctx.SendMessage(FindNodeResponse{Results: p.table_bc.FindClosest(msg.Target, BucketSize)})
	case P2pMessage:
		//fmt.Printf("recv p2p msg from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)

		if msg.Opcode == OP_REQUEST_GROUP {
			// delete existing(e.g., previous) routing information in table_gr
			peers := p.table_gr.KEntries(p.maxNeighborsGR)
			for _, id := range peers {
				p.RemoveConn(p.table_gr, id, false)
			}

			// register sender as leader
			p.SetLeader(ctx.ID().Address)
			//fmt.Printf("leader : %s\n",p.GetLeader())

			p.SetID(ID_INITIATOR)

			if p.events.OnSelectPeers != nil {
				random_peers := p.events.OnSelectPeers(msg, 1)
				msg.Contents = MarshalPeers(random_peers)
			}

			//fmt.Printf("I am : %d (1:initiator, 2:subleader, 3:worker)\n",p.GetID())
			if p.events.OnRequestGroup != nil {
				p.events.OnRequestGroup(msg)
	            }else{
				p.OnRequestGroupInitiator(msg)
			}
			break
		}

		if msg.Opcode == OP_REQUEST_REGROUP {
			// delete existing(e.g., previous) routing information in table_gr
			peers := p.table_gr.KEntries(p.maxNeighborsGR)
			for _, id := range peers {
				p.RemoveConn(p.table_gr, id, false)
			}

			var regroup_data RegroupData
			err := json.Unmarshal(msg.Contents, &regroup_data)
			if err != nil {
				fmt.Printf("OP_REQUEST_GROUP: Error on decode process: %v\n", err)
			}
			fmt.Printf("OP_REQUEST_GROUP: Current Round : %d\n", regroup_data.NextRound)
			p.SetLeader(regroup_data.Publisher)
			p.SetID(ID_INITIATOR)
			// fmt.Println(regroup_data.Peers)

			data, _ := json.Marshal(regroup_data.Peers)
			msg.Contents = data

			if p.events.OnSelectPeers != nil {
				random_peers := p.events.OnSelectPeers(msg, regroup_data.NextRound)
				msg.Contents = MarshalPeers(random_peers)
			}

			if p.events.OnRequestGroup != nil {
				p.events.OnRequestGroup(msg)
			} else {
				p.OnRequestGroupInitiator(msg)
			}

			if p.events.StartTrainingAfterRegroup != nil {
				p.events.StartTrainingAfterRegroup(regroup_data.ModelMetadata)
			}

			break
		}

		if msg.Opcode == OP_REQUEST_GROUP_SUB {
			// delete existing(e.g., previous) routing information in table_gr
			peers := p.table_gr.KEntries(p.maxNeighborsGR)
			for _, id := range peers {
				p.RemoveConn(p.table_gr, id, false)
			}

			// register sender as leader
			p.SetLeader(ctx.ID().Address)
			//fmt.Printf("leader : %s\n",p.GetLeader())

			p.SetID(ID_SUBLEADER)
			//fmt.Printf("I am : %d (1:initiator, 2:subleader, 3:worker)\n",p.GetID())
			if p.events.OnRequestGroupSub != nil {
				p.events.OnRequestGroupSub(msg)
	            }else{
				p.OnRequestGroupSubleader(msg)
			}

			break
		}

                if msg.Opcode == OP_REQUEST_JOIN{
			// delete existing(e.g., previous) routing information in table_gr
			peers := p.table_gr.KEntries(p.maxNeighborsGR)
			for _, id := range peers {
				p.RemoveConn(p.table_gr, id, false)
			}

			// register sender as leader
			p.SetLeader(ctx.ID().Address)
			//fmt.Printf("leader : %s\n",p.GetLeader())

			p.SetID(ID_WORKER)
			//fmt.Printf("I am : %d (1:initiator, 2:subleader, 3:worker)\n",p.GetID())
			if p.events.OnRequestJoin != nil {
				p.events.OnRequestJoin(msg)
	            }else{
				p.OnRequestJoinWorker(msg)
			}

			break
		}

                if msg.Opcode == OP_JOIN_GROUP{
			if p.events.OnJoinGroup != nil {
				p.events.OnJoinGroup(msg)
	            }else{
				p.OnJoinGroup(msg)
			}
			break
		}

                if msg.Opcode == OP_FED_COMP{
			if p.events.OnFedComputation != nil {
				p.events.OnFedComputation(msg)
			}
			break
		}

                if msg.Opcode == OP_REPORT{
			if p.events.OnReport != nil {
				p.events.OnReport(msg)
			}
			break
		}
		break
	case GossipMessage:
               if msg.Opcode == OP_GOSSIP_PUSH_ONLY{
			//fmt.Printf("recv OP_GOSSIP_PUSH_ONLY from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)

                    p.MarkSeenPush(ctx.ID(), msg.UUID) //mark this message from sender
			if p.HasSeenPush(p.node.ID(), msg.UUID) { // check if we have received
				//fmt.Printf("skip OP_GOSSIP_PUSH from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
				return nil
			}
			p.MarkSeenPush(p.node.ID(), msg.UUID) //otherwise, mark it

			if p.events.OnRecvGossipPush != nil {
	                 p.events.OnRecvGossipPush(msg,ctx)
	            }else{
			p.On_recv_gossip_push_only(msg,ctx)
			}
			break
		}

               if msg.Opcode == OP_GOSSIP_PUSH{
			//fmt.Printf("recv OP_GOSSIP_PUSH from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)

                    p.MarkSeenPush(ctx.ID(), msg.UUID) //mark this message from sender
			if p.HasSeenPush(p.node.ID(), msg.UUID) { // check if we have received
				//fmt.Printf("skip OP_GOSSIP_PUSH from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
				return nil
			}
			p.MarkSeenPush(p.node.ID(), msg.UUID) //otherwise, mark it

			// save item for pull request
			p.SetItem(DownloadMessage{UUID: msg.UUID, Contents: msg.Contents})
			p.SetUUID(msg.UUID)

			if p.events.OnRecvGossipPush != nil {
	                 p.events.OnRecvGossipPush(msg,ctx)
	            }else{
			p.On_recv_gossip_push(msg,ctx)
			}
			break
		}

	       if msg.Opcode == OP_GOSSIP_PULL_REQ{
			//fmt.Printf("recv OP_GOSSIP_PULL_REQ from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
                    p.markSeenPull(ctx.ID(), msg.UUID) //mark this message from sender
			if p.hasSeenPull(p.node.ID(), msg.UUID) { // check if we have received
				//fmt.Printf("skip OP_GOSSIP_PULL_REQ from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
				return nil
			}
			p.markSeenPull(p.node.ID(), msg.UUID) //otherwise, mark it

			if p.HasSeenPush(p.node.ID(), msg.UUID) { // check if we have received
				//fmt.Printf("this pullreq is new but already have item by OP_GOSSIP_PUSH from %s(%s) opcode : %v skip\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
				return nil
			}

			if p.events.OnRecvGossipPullReq != nil {
                        p.events.OnRecvGossipPullReq(msg,ctx)
                    }else{
                        p.On_recv_gossip_pullreq(msg,ctx)
			}
			break
		}

               if msg.Opcode == OP_FED_COMP_PUSH{
			//fmt.Printf("recv OP_FED_COMP_PUSH from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)

                    p.MarkSeenPush(ctx.ID(), msg.UUID) //mark this message from sender
			if p.HasSeenPush(p.node.ID(), msg.UUID) { // check if we have received
				//fmt.Printf("skip OP_FED_COMP_PUSH from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
				return nil
			}
			p.MarkSeenPush(p.node.ID(), msg.UUID) //otherwise, mark it

			// save item for pull request
			p.SetItem(DownloadMessage{UUID: msg.UUID, Contents: msg.Contents})
			p.SetUUID(msg.UUID)

			if p.events.OnFedComputationPush != nil {
	                 p.events.OnFedComputationPush(msg,ctx)
			}
			break
		}

	       if msg.Opcode == OP_FED_COMP_PULL_REQ{
			//fmt.Printf("recv OP_FED_COMP_PULL_REQ from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
                    p.markSeenPull(ctx.ID(), msg.UUID) //mark this message from sender
			if p.hasSeenPull(p.node.ID(), msg.UUID) { // check if we have received
				//ignore
				//fmt.Printf("skip OP_FED_COMP_PULL_REQ from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
				return nil
			}
			p.markSeenPull(p.node.ID(), msg.UUID) //otherwise, mark it

			if p.HasSeenPush(p.node.ID(), msg.UUID) { // check if we have received
				//fmt.Printf("this pullreq is new but already have item by OP_GOSSIP_PUSH from %s(%s) opcode : %v skip\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
				return nil
			}

			if p.events.OnFedComputationPullReq != nil {
                        p.events.OnFedComputationPullReq(msg,ctx)
			}
			break
		}
		break

	case Request:
		// check if we have the item
               if p.HasUUID(msg.UUID){
			// send product
			//fmt.Printf("recv Download Request from %s(%s). send item\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength])
			item := p.GetItem()
			ctx.SendMessage(DownloadMessage{UUID: msg.UUID, Contents: item.Contents})
                }else{
			// send deny
			//fmt.Printf("recv Download Request from %s(%s). send deny\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength])
			ctx.SendMessage(Deny{})
		}
		break

	//experimental
	case RelayMessage: // TODO : problem. when there is a cycle, relay is stuck
                fmt.Printf("recv RelayMessage from %s(%s) targetID:%s\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength],msg.TargetID.ID.String()[:8])
                p.markSeenRelay(ctx.ID(), msg.UUID) //mark this message from sender
		if p.hasSeenRelay(p.node.ID(), msg.UUID) { // check if we have received
			fmt.Printf("Already passed. skip RelayMessage from %s(%s) opcode : %v\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.Opcode)
			return nil
		}
		p.markSeenRelay(p.node.ID(), msg.UUID) //otherwise, mark it

		if msg.TargetID.ID.String() == p.node.ID().ID.String() {
			// If I am the target, callback
			fmt.Printf("I am the target\n")
			if p.events.OnRecvRelayMessage != nil {
	                p.events.OnRecvRelayMessage(msg,ctx)
	            }else{
			p.On_recv_relay(msg,ctx)
			}
			return nil
	        }else{
			// If I am not the target, relay
		    fmt.Printf("I am not the target target:%s me:%s\n",msg.TargetID.ID.String()[:printedLength], p.node.ID().ID.String()[:printedLength])
			// find the closest, and send
			closest := p.table_bc.FindClosest(msg.TargetID.ID, BucketSize)
			//for _, entry := range closest {
		    for i := len(closest)-1; i >= 0; i-- {
				if closest[i].ID.String() == ctx.ID().ID.String() || closest[i].ID.String() == p.node.ID().ID.String() {
                            fmt.Printf("skip sender or me %s %s (%s)\n", closest[i].ID.String()[:printedLength], closest[i].ID.String()[:printedLength],ctx.ID().ID.String()[:printedLength])
					continue
                        }else{
                            fmt.Printf("fail to skip sender or me %s %s (%s)\n", closest[i].ID.String()[:printedLength], closest[i].ID.String()[:printedLength],ctx.ID().ID.String()[:printedLength])
				}
				ctx, cancel := context.WithCancel(context.Background())
				err := p.node.SendMessage(ctx, closest[i].Address, msg)
		        fmt.Printf("relay message to %s Target:%s\n", closest[i].Address, msg.TargetID.ID.String()[:printedLength] )
				cancel()
				if err != nil {
		            fmt.Printf("Failed to report to %s. Skipping... [error: %s]\n",closest[i].Address, err)
				}
				return nil
			}
		}
		break
	}//switch
	return nil
}

// peers prints out all peers we are already aware of.
func (p *Protocol) KPeers_bc(k int) {
	ids := p.Table_bc().KEntries(k)

	var str []string
	for _, id := range ids {
		str = append(str, fmt.Sprintf("%s(%s)", id.Address, id.ID.String()[:printedLength]))
	}

	fmt.Printf("You know %d peer(s): [%v]\n", len(ids), strings.Join(str, ", "))
}

// print a bucket
func (p *Protocol) Peers_bc() {
	ids := p.Table_bc().Peers()

	var str []string
	for _, id := range ids {
		str = append(str, fmt.Sprintf("%s(%s)", id.Address, id.ID.String()[:printedLength]))
	}

	fmt.Printf("You know %d peer(s): [%v]\n", len(ids), strings.Join(str, ", "))
}

func (p *Protocol) Peers_gr() {
	ids := p.Table_gr().Peers()

	var str []string
	for _, id := range ids {
		str = append(str, fmt.Sprintf("%s(%s)", id.Address, id.ID.String()[:printedLength]))
	}

	fmt.Printf("You know %d peer(s): [%v]\n", len(ids), strings.Join(str, ", "))
}

func (p *Protocol) GridSearchGroupSize(tot_num_sub int) int {

	//TODO : degree of the tree <= p.MaxNeighborsGR()

	groupSize := 3 // degree of tree == 2

	return groupSize
}

func sort_by_metric(list map[string]Subscriber) []Subscriber {
	arr := make([]Subscriber, 0)
	for _, tx := range list {
		arr = append(arr, tx)
	}
	/*
	   // TODO : as default, we sort nodes with basic infor when no user sorting is provided
	   sort.Slice(arr, func(i, j int) bool {
	       if arr[i].Id != arr[j].Id {
	           return arr[i].Id < arr[j].Id
	       }
	       return arr[i].Priority < arr[j].Priority
	   })
	*/
	return arr
}

// forming a group is p2p level protocol. later, we need to move it to protocol
func (p *Protocol) OnRequestGroupInitiator(msg P2pMessage) {
	//fmt.Printf("OP_REQUEST_GROUP. I am Initiator.\n")
	var sortedArr []Subscriber
    sub_list := make(map[string]Subscriber,0)
	err := json.Unmarshal(msg.Contents, &sub_list)
	if err != nil {
		fmt.Printf("OnRequestGroupInitiator: Error on decode process: %v\n", err)
	}
	//fmt.Println(sub_list)

	if p.events.OnSortByMetric != nil {
		sortedArr = p.events.OnSortByMetric(sub_list)
    }else{
		sortedArr = sort_by_metric(sub_list)
	}

	p.group_size = int(msg.Aggregation) //use Aggregation field for receiving group_size
    p.SetMaxNeighborsGR(p.group_size-1)

	// set the degree of the tree = group_size-1
    p.SetTotMembers(len(sortedArr)-1)

	// register members in table_gr
	// case A : if list -1 <= table_size : all member are workers in one level tree
	//if( len(sub_list)-1 <= p.group_size-1 ){
    if( len(sortedArr)-1 <= p.group_size-1 ){
        //fmt.Printf("Case A len_list : %d table size : %d\n",len(sub_list)-1, p.MaxNeighborsGR())
		//for _, v := range sub_list {
		for _, v := range sortedArr {
            if( v.Key == p.node.ID().ID.String()){
				//fmt.Printf("Case A : skip myself\n")
				continue // skip myself
			}
			msgctx, cancel := context.WithCancel(context.Background())
			err := p.node.SendMessage(msgctx, v.Addr, P2pMessage{Opcode: byte(OP_REQUEST_JOIN)}, OP_GROUP_MULTICAST)
			/*
				            fmt.Printf("Case A : Send message to worker %s opcode: OP_GROUP_MULTICAST mykey: %s vkey: %s\n",
				                        v.Addr,
					                p.node.ID().ID.String()[:printedLength],
					                v.Key,
					              )
			*/
			cancel()
			if err != nil {
				fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
					v.Addr,
					err,
				)
			}
			//time.Sleep(100 * time.Millisecond)
		}
		return
	}

	//fmt.Printf("Case B len_list : %d table size : %d\n",len(sub_list)-1, p.group_size-1)
	// case B : if list > table_size : split the list with round robin or level numbering
	// assign subscribers to each member
	arr := make([][]Subscriber, p.group_size-1)
    for i:=0;i<(p.group_size-1);i++ {
		arr[i] = make([]Subscriber, 0)
	}
	idx := 0

	// Option 1 Round Robin assignment
	/*
	   //for _, v := range sub_list {
	   for _, v := range arr {
	       if( v.Key == p.node.ID().ID.String()[:printedLength]){
	           //fmt.Printf("Case B : skip myself\n")
	           continue // skip myself
	       }
	       arr[idx] = append(arr[idx], v)
	       idx = (idx + 1) % p.MaxNeighborsGR()
	   }
	*/

	// Option 2 Level Numbering
    idx_cnt:=1
    level:=1
    level_cnt:=0
	//for _, v := range sub_list {
	for _, v := range sortedArr {
        if( v.Key == p.node.ID().ID.String()){
			//fmt.Printf("Case B : skip myself\n")
			continue // skip myself
		}

        arr[idx] = append(arr[idx],v)
		idx_cnt++
		level_cnt++
        if (idx_cnt > powInt(p.group_size-1,level-1)) {
            idx = (idx+1) % (p.group_size-1)
            idx_cnt=1
            if (level_cnt >=  powInt(p.group_size-1,level)){
				level++
                level_cnt=1
			}
		}
	}
	/*
	   for k, ia := range arr {
	       for _, v := range ia {
	           fmt.Printf("arr[%d] %v \n", k, v)
	       }
	   }
	*/
	// send REQUEST_GROUP or REQUEST_JOIN to members
	for _, ia := range arr {
        if( len(ia) == 0 ){
			continue
		}
        if( len(ia) > 1 ){
			// case 1 : if arr > 1 , then select sub-leader and send REQUEST_GROUP
			msgctx, cancel := context.WithCancel(context.Background())
			data, _ := json.Marshal(ia)
			//fmt.Printf("%s\n", data)
			err := p.node.SendMessage(msgctx, ia[0].Addr, P2pMessage{Aggregation: uint32(p.group_size), Opcode: byte(OP_REQUEST_GROUP_SUB), Contents: data}, OP_GROUP_MULTICAST) //use Aggregation field for passing group_size
			//fmt.Printf("Case B-1 : Send message to %s opcode: OP_REQUEST_GROUP_SUB\n",
			//            ia[0].Addr,
			//)
			cancel()
			if err != nil {
				fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
					ia[0].Addr,
					err,
				)
			}
        }else{
			// case 2 : if arr = 1 , then select sub-leader and send REQUEST_JOIN
			msgctx, cancel := context.WithCancel(context.Background())
			err := p.node.SendMessage(msgctx, ia[0].Addr, P2pMessage{Opcode: byte(OP_REQUEST_JOIN)}, OP_GROUP_MULTICAST)
			//fmt.Printf("Case B-2 : Send message to %s opcode: OP_REQUEST_JOIN\n",
			//            ia[0].Addr,
			//)
			cancel()
			if err != nil {
				fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
					ia[0].Addr,
					err,
				)
			}
        }//else
		//time.Sleep(100 * time.Millisecond)
    }//for
}

// forming a group is p2p level protocol. later, we need to move it to protocol
func (p *Protocol) OnRequestGroupSubleader(msg P2pMessage) {
    list := make([]Subscriber,0)
	err := json.Unmarshal(msg.Contents, &list)
	if err != nil {
		fmt.Printf("Error on decode process: %v\n", err)
	}
	//fmt.Printf("OP_REQUEST_GROUP_SUB. I am Sub-leader. group_size:%d\n", msg.Aggregation)
	//fmt.Println(list)
	p.group_size = int(msg.Aggregation) //use Aggregation field for receiving group_size
    p.SetMaxNeighborsGR(p.group_size-1)

	// set min_num_client for sub leader
    p.SetTotMembers(len(list)-1)

	// register members in table_gr
	// case A : if list -1 <= table_size : all member workers
	//if( len(list)-1 <= p.MaxNeighborsGR() ){
    if( len(list)-1 <= p.group_size-1 ){
		//fmt.Printf("Case A len_list : %d table size : %d\n",len(list)-1, p.MaxNeighborsGR())
		for _, v := range list {
            if( v.Key == p.node.ID().ID.String()){
				//fmt.Printf("Case A : skip myself\n")
				continue // skip myself
			}
			msgctx, cancel := context.WithCancel(context.Background())
			err := p.node.SendMessage(msgctx, v.Addr, P2pMessage{Opcode: byte(OP_REQUEST_JOIN)}, OP_GROUP_MULTICAST)
			/*
			   fmt.Printf("Case A : Send message to worker %s opcode: OP_GROUP_MULTICAST mykey: %s vkey: %s\n",
			              v.Addr,
			              p.node.ID().ID.String()[:printedLength],
			              v.Key,
			   )
			*/
			cancel()
			if err != nil {
				fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
					v.Addr,
					err,
				)
			}

		}
		return
	}

	//fmt.Printf("Case B len_list : %d group size : %d\n",len(list)-1, p.group_size-1)
	// case B : if list > table_size : split the list with round robin
	// assign subscribers to each member
	arr := make([][]Subscriber, p.group_size-1)
    for i:=0;i<(p.group_size-1);i++ {
		arr[i] = make([]Subscriber, 0)
	}
	idx := 0

	// Option 1 Round Robin
	/*
	   for _, v := range list {
	       if( v.Key == p.node.ID().ID.String()){
	           //fmt.Printf("Case B : skip myself\n")
	           continue // skip myself
	       }
	       arr[idx] = append(arr[idx], v)
	       idx = (idx + 1) % p.MaxNeighborsGR()
	   }
	*/

	// Option 2 Level Numbering
    idx_cnt:=1
    level:=1
    level_cnt:=0
	for _, v := range list {
        if( v.Key == p.node.ID().ID.String()){
			//fmt.Printf("Case B : skip myself\n")
			continue // skip myself
		}

        arr[idx] = append(arr[idx],v)
		idx_cnt++
		level_cnt++
        if (idx_cnt > powInt(p.group_size-1,level-1)) {
            idx = (idx+1) % (p.group_size-1)
            idx_cnt=1
            if (level_cnt >=  powInt(p.group_size-1,level)){
				level++
                level_cnt=1
			}
		}
	}
	/*
	   for k, ia := range arr {
	       for _, v := range ia {
	           fmt.Printf("arr[%d] %v ", k, v)
	       }
	   }
	*/
	// send REQUEST_GROUP or REQUEST_JOIN to members
	for _, ia := range arr {
        if( len(ia) == 0 ){
			continue
		}
        if( len(ia) > 1 ){
			// case 1 : if arr > 1 , then select sub-leader and send REQUEST_GROUP
			msgctx, cancel := context.WithCancel(context.Background())
			data, _ := json.Marshal(ia)
			err := p.node.SendMessage(msgctx, ia[0].Addr, P2pMessage{Aggregation: uint32(p.group_size), Opcode: byte(OP_REQUEST_GROUP_SUB), Contents: data}, OP_GROUP_MULTICAST) // use Aggregation field for passing group_size
			//fmt.Printf("Case B-1 : Send message to subleader %s opcode: OP_GROUP_MULTICAST\n",
			//            ia[0].Addr,
			//)
			cancel()
			if err != nil {
				fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
					ia[0].Addr,
					err,
				)
			}
        }else{
			// case 2 : if arr = 1 , then send REQUEST_JOIN to workers
			msgctx, cancel := context.WithCancel(context.Background())
			err := p.node.SendMessage(msgctx, ia[0].Addr, P2pMessage{Opcode: byte(OP_REQUEST_JOIN)}, OP_GROUP_MULTICAST)
			//fmt.Printf("Case B-2 : Send message to worker %s opcode: OP_GROUP_MULTICAST\n",
			//            ia[0].Addr,
			//)
			cancel()
			if err != nil {
				fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
					ia[0].Addr,
					err,
				)
			}

        }//else
    }//for
}

func (p *Protocol) OnRequestJoinWorker(msg P2pMessage) {
	//fmt.Printf("OP_REQUEST_JOIN. I am a Worker.\n")
	userinfoPool := make([][]byte, 0)

	var userinfo []byte
	// gather group information from user if exists
	if p.events.OnQueryGroupInfo != nil {
		userinfo = p.events.OnQueryGroupInfo()
	}

	userinfoPool = append(userinfoPool, userinfo)
	aggregatedinfo, _ := json.Marshal(userinfoPool)

	// send report
	msgctx, cancel := context.WithCancel(context.Background())
	//err := p.node.SendMessage(msgctx, p.GetLeader(), P2pMessage{Aggregation: uint32(1), Opcode: byte(OP_JOIN_GROUP)}, OP_JOIN_GROUP)
	err := p.node.SendMessage(msgctx, p.GetLeader(), P2pMessage{Aggregation: uint32(1), Opcode: byte(OP_JOIN_GROUP), Contents: aggregatedinfo}, OP_JOIN_GROUP)
	/*
	   fmt.Printf("Send report to leader %s opcode: OP_JOIN_GROUP\n",
	   p.GetLeader())
	*/
	cancel()
	if err != nil {
		fmt.Printf("Failed to send message to %s. Skipping... [error: %s]\n",
                   p.GetLeader(),err)
	}
	p.Lock()
	// reset the value after reporting
	p.num_members = 0
	p.report = false
	p.Unlock()
}

func (p *Protocol) OnJoinGroup(msg P2pMessage) {
	if p.GetID() == ID_PUBLISHER {

		var tempPool [][]byte

		err := json.Unmarshal(msg.Contents, &tempPool)
		if err != nil {
			fmt.Printf("Error on decode process: %v\n", err)
		}

		p.userinfoPool = append(p.userinfoPool, tempPool...)

		// gather group information of my node(sub-leader ifself)
		if p.events.OnAggregateGroupInfo != nil {
			p.events.OnAggregateGroupInfo(p.userinfoPool)
		}

		if p.events.OnGroupDone != nil {
			p.events.OnGroupDone(msg)
		}
	}

	if p.GetID() == ID_INITIATOR {
		var aggregatedinfo []byte
		var tempPool [][]byte
		//fmt.Printf("Receive OP_JOIN_GROUP as an initiator.\n")
		value := msg.Aggregation
		//fmt.Printf("OP_JOIN_GROUP value : %d\n",value)

		err := json.Unmarshal(msg.Contents, &tempPool)
		if err != nil {
			fmt.Printf("Initiator: Error on decode process: %v\n", err)
		}

		p.userinfoPool = append(p.userinfoPool, tempPool...)

		p.Lock()
		p.num_members += int(value)
		//fmt.Printf("OP_JOIN_GROUP num_members : %d, total expected : %d\n",num_members, p.GetTotMembers())
		if p.num_members == p.GetTotMembers() {
			p.report = true
			p.Unlock()
			//C.call_c_func2(callbacks.on_set_num_client, (C.int)(p.tot_members) ) //obsolete ?

			var userinfo []byte
			// gather group information of my node(sub-leader ifself)
			if p.events.OnQueryGroupInfo != nil {
				userinfo = p.events.OnQueryGroupInfo()
			}
			p.userinfoPool = append(p.userinfoPool, userinfo)
			aggregatedinfo, _ = json.Marshal(p.userinfoPool)
        }else{
			p.Unlock()
		}

		// send upper leader if it meets the total
        if(p.report){
            fmt.Printf("Report to publisher(%s) include me. num_members : %d+1, total expected : %d+1\n",p.GetLeader(),p.num_members,  p.GetTotMembers())
			msgctx, cancel := context.WithCancel(context.Background())
			//err := p.node.SendMessage(msgctx, p.GetLeader(), P2pMessage{Aggregation: uint32(p.num_members+1), Opcode: byte(OP_JOIN_GROUP), Contents: msg.Contents}, OP_JOIN_GROUP)
            err := p.node.SendMessage(msgctx, p.GetLeader(), P2pMessage{Aggregation: uint32(p.num_members+1), Opcode: byte(OP_JOIN_GROUP), Contents: aggregatedinfo}, OP_JOIN_GROUP)
			/*
			   fmt.Printf("Send report to upper leader %s opcode: OP_JOIN_GROUP value : %d\n",
			   p.GetLeader(),p.num_members+1)
			*/
			cancel()
			if err != nil {
				fmt.Printf("Failed to send report to %s. Skipping... [error: %s]\n",
                           p.GetLeader(),err)
			}

			// reset the value after reporting
			p.num_members = 0
			p.report = false
			p.userinfoPool = make([][]byte, 0)
		}
		//p.Unlock()
	}

	if p.GetID() == ID_SUBLEADER {
		var aggregatedinfo []byte
		var tempPool [][]byte
		//fmt.Printf("Receive OP_JOIN_GROUP as an sub-leader.\n")

		value := msg.Aggregation
		//fmt.Printf("OP_JOIN_GROUP value : %d\n",value)

		err := json.Unmarshal(msg.Contents, &tempPool)
		if err != nil {
			fmt.Printf("Subleader: Error on decode process: %v\n", err)
		}

		p.userinfoPool = append(p.userinfoPool, tempPool...)

		p.Lock()
		p.num_members += int(value)
		//fmt.Printf("OP_JOIN_GROUP num_members : %d, total expected : %d\n",p.num_members, p.tot_members)
		if p.num_members == p.GetTotMembers() {
			p.report = true
			p.Unlock()
			//C.call_c_func2(callbacks.on_set_num_client, (C.int)(p.tot_members) ) //obsolete ?

			var userinfo []byte
			// gather group information of my node(sub-leader ifself)
			if p.events.OnQueryGroupInfo != nil {
				userinfo = p.events.OnQueryGroupInfo()
			}
			p.userinfoPool = append(p.userinfoPool, userinfo)
			aggregatedinfo, _ = json.Marshal(p.userinfoPool)
        }else{
			p.Unlock()
		}

		// send upper leader if it meets the total
        if(p.report){
			msgctx, cancel := context.WithCancel(context.Background())
			//err := p.node.SendMessage(msgctx, p.GetLeader(), P2pMessage{Aggregation: uint32(p.num_members + 1), Opcode: byte(OP_JOIN_GROUP), Contents: msg.Contents}, OP_JOIN_GROUP)
			err := p.node.SendMessage(msgctx, p.GetLeader(), P2pMessage{Aggregation: uint32(p.num_members + 1), Opcode: byte(OP_JOIN_GROUP), Contents: aggregatedinfo}, OP_JOIN_GROUP)
			/*
			   fmt.Printf("Send report to upper leader %s opcode: OP_JOIN_GROUP value : %d\n",
			              p.GetLeader(),p.num_members+1)
			*/
			cancel()
			if err != nil {
				fmt.Printf("Failed to send report to %s. Skipping... [error: %s]\n",
                           p.GetLeader(),err)
			}

			// reset the value after reporting
			p.num_members = 0
			p.report = false
			p.userinfoPool = make([][]byte, 0)
		}
		//p.Unlock()
	}
}


func (p *Protocol) Broadcast_BC(sender GRING.ID, data []byte, opcode byte) {
	//for _, id := range p.Table_bc().KPeers(p.maxNeighborsBC) {
	for _, id := range p.Table_bc().KEntries(p.maxNeighborsBC) {
		if id.ID == sender.ID {
			//fmt.Printf("skip sender %v %v\n", id.ID.String()[:printedLength], sender.ID.String()[:printedLength])
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())
		//err := p.node.SendMessage(ctx, id.Address, P2pMessage{Opcode: opcode, Contents: data } OP_BROADCAST)
        err := p.node.SendMessage(ctx, id.Address, P2pMessage{Opcode: opcode, Contents: data })
		/*
		   fmt.Printf("Send message to %s(%s) opcode: %v\n",
		               id.Address,
		               id.ID.String()[:printedLength],
		               opcode,
		             )
		*/
		cancel()
		if err != nil {
			fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
				id.Address,
				id.ID.String()[:printedLength],
				err,
			)
			continue
		}
	}
}

func (p *Protocol) Multicast_GR(sender GRING.ID, data []byte, opcode byte) {
	//for _, id := range p.Table_gr().KPeers(p.maxNeighborsGR) {
	for _, id := range p.Table_gr().KEntries(p.maxNeighborsGR) {
        if id.ID == sender.ID || id.Address == p.GetLeader(){
			//fmt.Printf("skip sender %v %v\n", id.ID.String()[:printedLength], sender.ID.String()[:printedLength])
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())
		//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        err := p.node.SendMessage(ctx, id.Address, P2pMessage{Opcode: opcode, Contents: data }, OP_GROUP_MULTICAST)
		/*
		   fmt.Printf("Send message to %s(%s) opcode: %v\n",
		               id.Address,
		               id.ID.String()[:printedLength],
		               opcode,
		             )
		*/
		cancel()
		if err != nil {
			fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
				id.Address,
				id.ID.String()[:printedLength],
				err,
			)
			continue
		}
	}
}

func (p *Protocol) Fedcomp_GR(sender GRING.ID, data []byte) {
	for _, id := range p.Table_gr().KEntries(p.maxNeighborsGR) {
        if id.ID == sender.ID || id.Address == p.GetLeader(){
			//fmt.Printf("skip sender %v %v\n", id.ID.String()[:printedLength], sender.ID.String()[:printedLength])
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())
        err := p.node.SendMessage(ctx, id.Address, P2pMessage{Opcode:byte(OP_FED_COMP), Contents:data})
		//fmt.Printf("Send FedComp to %s(%s) \n", id.Address, id.ID.String()[:printedLength],)
		cancel()
		if err != nil {
            fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",id.Address,id.ID.String()[:printedLength],err,)
			continue
		}
	}
}

func (p *Protocol) Report_GR(sender GRING.ID, data []byte,  aggregation int) {
	ctx, cancel := context.WithCancel(context.Background())
    err := p.node.SendMessage(ctx, p.GetLeader(), P2pMessage{Aggregation: uint32(aggregation), Opcode: byte(OP_REPORT), Contents: data })
	//fmt.Printf("Report to (sub)leader %s \n", p.GetLeader())
	cancel()
	if err != nil {
        fmt.Printf("Failed to report to %s. Skipping... [error: %s]\n",p.GetLeader(), err)
	}
}

func (p *Protocol) On_recv_gossip_push_only(msg GossipMessage, ctx GRING.HandlerContext) {
	//fmt.Printf("onRecvGossipPush: OP_GOSSIP_PUSH RECEIVE ITEM count:%d\n",msg.Count)

    list := make([]string,0)
    list = append(list,ctx.ID().ID.String())

	p.__Gossip_BC_push_only(msg.UUID, msg.Contents, list, msg.Type, msg.TargetID)

	if p.events.OnRecvGossipMsg != nil {
		p.events.OnRecvGossipMsg(msg)
	}
}


func (p *Protocol) On_recv_gossip_push(msg GossipMessage, ctx GRING.HandlerContext) {
	//fmt.Printf("onRecvGossipPush: OP_GOSSIP_PUSH RECEIVE ITEM count:%d\n",msg.Count)

    list := make([]string,0)
	/*
	   // list contains pushed ones. So, it is an avoid list to push receiver
	   if msg.Count != 0{
	       err := json.Unmarshal(msg.List, &list)
	       if err != nil {
	           fmt.Printf("Error on decode process: %v\n", err)
	       }
	       //fmt.Println(list)
	   }
	*/
	//fmt.Printf("onRecvGossipPush: gossip_BC msg.Type:%v\n",msg.Type)
	// send push and pull req
    list = append(list,ctx.ID().ID.String())
	p.__Gossip_BC(msg.UUID, msg.Contents, list, msg.Type, msg.TargetID)

	if p.events.OnRecvGossipMsg != nil {
		p.events.OnRecvGossipMsg(msg)
	}

}

func (p *Protocol) On_recv_gossip_pullreq(msg GossipMessage, ctx GRING.HandlerContext) {
	//fmt.Printf("OnRecvGossipPullReq: OP_GOSSIP_PULL_REQ count:%d\n",msg.Count)

	//go func() {
	// list contains pushed ones. So, it is a perferred list to pull req receiver
        list := make([]string,0)
	/*
	   if msg.Count != 0{
	       err := json.Unmarshal(msg.List, &list)
	       if err != nil {
	           fmt.Printf("Error on decode process: %v\n", err)
	       }
	       //fmt.Println(list)
	   }
	*/

	maxtry := 10
	i := 0
	for i < maxtry {
		// RequestMessage to download data
		dmsg, rst := p.RequestDownload(msg.UUID, list)

		if rst {
			//fmt.Printf("Got Download RECEIVE ITEM\n")
			// save item for incoming pull request
			p.SetItem(dmsg)
			p.SetUUID(dmsg.UUID)

			//fmt.Printf("onRecvGossipPullReq: gossip_BC msg.Type:%v\n",msg.Type)
			// send push and pull req
                list = append(list,p.node.ID().ID.String())
                list = append(list,ctx.ID().ID.String())
                p.__Gossip_BC(msg.UUID, msg.Contents, list, msg.Type,  msg.TargetID)

			if !p.HasSeenPush(p.node.ID(), msg.UUID) { // check if we have received PUSH during this search
				p.MarkSeenPush(p.node.ID(), msg.UUID)
				if p.events.OnRecvGossipMsg != nil {
					p.events.OnRecvGossipMsg(msg)
				}
			}

			return
            }else{
			//fmt.Printf("fail to download.\n")
			time.Sleep(300 * time.Millisecond)
		}
		i++
	}
	//}()
}

func (p *Protocol) On_recv_relay(relaymsg RelayMessage, ctx GRING.HandlerContext) {
	//fmt.Printf("on_recv_relay : recv relay msg\n")
	/*
	   msg, err := UnmarshalSubsMessage(relaymsg.Contents)
	   if err != nil {
	       fmt.Printf("on_recv_relay : not ok. Fail recv app msg\n",)
	       return
	   }
	   fmt.Printf("on_recv_relay : recv relay msg SubID:%s SubAddr:%s \n", msg.SubID.ID.String()[:printedLength], msg.Addr[:printedLength])
	*/
}

func (p *Protocol) Gossip_BC(uuid string, data []byte, avoid []string, msgtype byte, targetid GRING.ID) {
	euuid, _ := json.Marshal(uuid)
	p.__Gossip_BC(euuid, data, avoid, msgtype, targetid)
}

func (p *Protocol) __Gossip_BC_push_only(euuid []byte, data []byte, avoid []string, msgtype byte, targetid GRING.ID) {
	peers := p.Table_bc().KEntries(p.maxNeighborsBC)
	if len(avoid) > 0 {
        peers = p.Remove(peers,avoid)
	}

	for _, id := range peers {
		p.Sendmsg(euuid, data, 0, nil, id, OP_GOSSIP_PUSH_ONLY, msgtype, targetid)
	}
}

func (p *Protocol) __Gossip_BC(euuid []byte, data []byte, avoid []string, msgtype byte, targetid GRING.ID) {
	peers := p.Table_bc().KEntries(p.maxNeighborsBC)
	if len(avoid) > 0 {
        peers = p.Remove(p.Table_bc().KEntries(p.maxNeighborsBC),avoid)
	}

	len_peers := len(peers)
	if len_peers == 0 {
		//fmt.Printf("gossip_BC: no peers to broadcast len_peers:%d\n",len_peers)
		return
	}

	//push := math.Sqrt(float64(len_peers))
    push := len_peers/2
	if push == 0 {
		push = 1
    }else if push == 1 {
		push = 2
	}
	pushlist, pulllist := p.FindRandMember(push, len_peers, peers)
	//fmt.Printf("gossip_BC : pushlist:%d, pulllist:%d\n",len(pushlist), len(pulllist))

	/*
	   list := make([]string,0)
	   for _, id := range pushlist {
	       list = append(list,id.ID.String())
	   }
	   list = append(list,avoid...)
	   listdata, _ := json.Marshal(list)
	   length := len(listdata)
	*/

	for _, id := range pushlist {
		//p.Sendmsg(euuid, data, length, listdata, id, OP_GOSSIP_PUSH, msgtype, targetid)
		p.Sendmsg(euuid, data, 0, nil, id, OP_GOSSIP_PUSH, msgtype, targetid)
	}

	for _, id := range pulllist {
		//p.Sendmsg(euuid, data, length, listdata, id, OP_GOSSIP_PULL_REQ, msgtype, targetid)
		p.Sendmsg(euuid, data, 0, nil, id, OP_GOSSIP_PULL_REQ, msgtype, targetid)
	}
}

func(p *Protocol) Sendmsg(euuid []byte, data []byte, length int, listdata []byte, id GRING.ID, opcode byte, msgtype byte, targetid GRING.ID) {
	ctx, cancel := context.WithCancel(context.Background())
    err := p.node.SendMessage(ctx, id.Address, GossipMessage{Opcode: opcode, Type: msgtype, UUID: euuid, Count: uint32(length),  TargetID: targetid, List: listdata, Contents: data })
	//fmt.Printf("Send message to %s(%s)\n",id.Address,id.ID.String()[:printedLength],)
	cancel()
	if err != nil {
		fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
			id.Address,
			id.ID.String()[:printedLength],
			err,
		)
		//continue
	}
}

func (p *Protocol) RequestDownload(euuid []byte, preferred []string) (DownloadMessage, bool) {
	var firstcontact []GRING.ID
	var secondcontact []GRING.ID
	peers := p.Table_bc().KEntries(p.maxNeighborsBC)
	if len(preferred) > 0 {
		// choose preferred ones first. preferred ones are firstcontact
		//firstcontact, secondcontact = p.Choose(p.Table_bc().KPeers(p.maxNeighborsBC),preferred)
        firstcontact, secondcontact = p.Choose(p.Table_bc().KEntries(p.maxNeighborsBC),preferred)
    }else{
		firstcontact = peers
	}

	for _, id := range firstcontact {
		// if I receive item during this search, I stop the search.
		if p.HasSeenPush(p.node.ID(), euuid) || p.HasUUID(euuid) { // check if we have received
			//fmt.Printf("I receive item. Stop Searching\n")
			return p.GetItem(), true
		}
		ctx, cancel := context.WithCancel(context.Background())
		//ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		/*
		   fmt.Printf("Send pull message to %s(%s) and Waiting..\n",
		               id.Address,
		               id.ID.String()[:printedLength],
		             )
		*/
		obj, err := p.node.RequestMessage(ctx, id.Address, Request{UUID: euuid})
		cancel()
		if err != nil {
			fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
				id.Address,
				id.ID.String()[:printedLength],
				err,
			)
			continue
		}
		switch msg := obj.(type) {
		case DownloadMessage:
			msg, ok := obj.(DownloadMessage)
			if !ok {
				fmt.Printf("download message broken\n")
				break
			}
			//fmt.Printf("firstcontact download hit\n")
			return msg, true
		case Deny:
			/*
				                mutex.Lock()
					        num_fail_download++
					        mutex.Unlock()
					        fmt.Printf("firstcontact download not hit : %d\n",num_fail_download)
			*/
			//fmt.Printf("firstcontact download not hit\n")
		}
	}

	for _, id := range secondcontact {
		// if I receive item during this search, I stop the search.
		if p.HasSeenPush(p.node.ID(), euuid) || p.HasUUID(euuid) { // check if we have received
			//fmt.Printf("I receive item. Stop Searching\n")
			return p.GetItem(), true
		}
		ctx, cancel := context.WithCancel(context.Background())
		//ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		/*
		   fmt.Printf("Send pull message to %s(%s) and Waiting..\n",
		               id.Address,
		               id.ID.String()[:printedLength],
		             )
		*/
		obj, err := p.node.RequestMessage(ctx, id.Address, Request{UUID: euuid})
		cancel()
		if err != nil {
			fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
				id.Address,
				id.ID.String()[:printedLength],
				err,
			)
			continue
		}
		switch msg := obj.(type) {
		case DownloadMessage:
			msg, ok := obj.(DownloadMessage)
			if !ok {
				fmt.Printf("download message broken\n")
				break
			}
			//fmt.Printf("download hit\n")
			return msg, true
		case Deny:
			/*
			   mutex.Lock()
			   num_fail_download++
			   mutex.Unlock()
			   fmt.Printf("secondcontact download not hit : %d\n",num_fail_download)
			*/
			//fmt.Printf("secondcontact download not hit\n")
		}
	}
	return DownloadMessage{}, false
}

func (p *Protocol) Remove(peers []GRING.ID, entry []string) []GRING.ID {
	//fmt.Printf(" need to remove : %v \n",entry)
	removed := 0
	for _, eid := range entry {
		for k, id := range peers {
			if id.ID.String() == eid {
				peers[k] = peers[len(peers)-1]
				removed++
			}
		}
		peers = peers[:len(peers)-removed]
        removed=0
	}
	//fmt.Printf(" after remove : %v \n",peers)

	return peers
}

func (p *Protocol) FindRandMember(push int, len_peers int, peers []GRING.ID) ([]GRING.ID,[]GRING.ID) {
	//fmt.Printf(" findRandMember len_peers:%d\n",len_peers)
	rnums := make(map[int]int, 0)
	for {
		rnum := rand.Intn(len_peers)
        rnums[rnum]=rnum
        if len(rnums) == push{
			break
		}
	}

	pushlist := make([]GRING.ID, 0)
	pulllist := make([]GRING.ID, 0)
	for k, id := range peers {
		if _, ok := rnums[k]; ok {
			pushlist = append(pushlist, id)
        }else{
			pulllist = append(pulllist, id)
		}
	}

	return pushlist, pulllist
}

func (p *Protocol) Choose(peers []GRING.ID, entry []string) ([]GRING.ID, []GRING.ID) {
	var first []GRING.ID
	var second []GRING.ID

	//fmt.Printf(" need to choose : %v \n",entry)
	for _, eid := range entry {
		for _, id := range peers {
			if id.ID.String() == eid {
                    first = append(first,id)
                }else{
                    second = append(second,id)
			}
		}
	}
	//fmt.Printf(" after choose first : %v \n",first)
	return first, second
}
