package dual

import "github.com/git-disl/GRING"

// Events comprise of callbacks that are to be called upon the encountering of various events as a node follows
// the Kademlia protocol. An Events declaration may be registered to a Protocol upon instantiation through calling
// New with the WithProtocolEvents functional option.
type Events struct {
	// OnPeerAdmitted is called when a peer is admitted to being inserted into your nodes' routing table.
	OnPeerAdmitted_bc func(id GRING.ID)

	// OnPeerAdmitted is called when a peer is admitted to being inserted into your nodes' routing table.
	OnPeerAdmitted_gr func(id GRING.ID)

	// OnPeerContacted is called when a peer is contacting this node. It is for collecting subscribers.
	OnPeerContacted func(id GRING.ID)

	// OnPeerActivity is called when your node interacts with a peer, causing the peer's entry in your nodes' routing
	// table to be bumped to the head of its respective bucket.
	OnPeerActivity func(id GRING.ID)

	// OnPeerEvicted is called when your node fails to ping/dial a peer that was previously admitted into your nodes'
	// routing table, which leads to an eviction of the peers ID from your nodes' routing table.
	OnPeerEvicted func(id GRING.ID)

	OnRequestGroup func(msg P2pMessage)

	OnRequestGroupSub func(msg P2pMessage)

	OnRequestJoin func(msg P2pMessage)

	OnSortByMetric func(list map[string]Subscriber) []Subscriber

	OnJoinGroup func(msg P2pMessage)

	OnQueryGroupInfo func() []byte

	OnAggregateGroupInfo func([][]byte)

	OnGroupDone func(msg P2pMessage)

	OnFedComputation func(msg P2pMessage)

	OnReport func(msg P2pMessage)

	OnFedComputationPush func(msg GossipMessage, ctx GRING.HandlerContext)

	OnFedComputationPullReq func(msg GossipMessage, ctx GRING.HandlerContext)

	OnFedComputationPull func(msg GossipMessage, ctx GRING.HandlerContext)

	OnRecvGossipPush func(msg GossipMessage, ctx GRING.HandlerContext)

	OnRecvGossipPullReq func(msg GossipMessage, ctx GRING.HandlerContext)

	OnRecvRelayMessage func(msg RelayMessage, ctx GRING.HandlerContext)

	OnRecvGossipMsg func(msg GossipMessage)

	OnSelectPeers func(msg P2pMessage, next_round int) []Subscriber

	StartTrainingAfterRegroup func(model_metadata []byte)
}
