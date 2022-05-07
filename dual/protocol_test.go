package dual_test

import (
	"context"
	"github.com/theued/GRING"
	"github.com/theued/GRING/dual"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"sync"
	"testing"
)

func merge(clients ...[]*GRING.Client) []*GRING.Client {
	var result []*GRING.Client

	for _, list := range clients {
		result = append(result, list...)
	}

	return result
}

func getBucketIndex(self, target GRING.PublicKey) int {
	l := dual.PrefixLen(dual.XOR(target[:], self[:]))
	if l == GRING.SizePublicKey*8 {
		return l - 1
	}

	return l
}

func TestTableEviction(t *testing.T) {
	defer goleak.VerifyNone(t)

	publicKeys := make([]GRING.PublicKey, 0, dual.BucketSize+2)
	privateKeys := make([]GRING.PrivateKey, 0, dual.BucketSize+2)

	for len(publicKeys) < cap(publicKeys) {
		pub, priv, err := GRING.GenerateKeys(nil)
		assert.NoError(t, err)

		if len(publicKeys) < 2 {
			publicKeys = append(publicKeys, pub)
			privateKeys = append(privateKeys, priv)
			continue
		}

		actualBucket := getBucketIndex(pub, publicKeys[0])
		expectedBucket := getBucketIndex(publicKeys[1], publicKeys[0])

		if actualBucket != expectedBucket {
			continue
		}

		publicKeys = append(publicKeys, pub)
		privateKeys = append(privateKeys, priv)
	}

	leader, err := GRING.NewNode(GRING.WithNodePrivateKey(privateKeys[0]))
	assert.NoError(t, err)
	defer leader.Close()

	overlay := dual.New()
	leader.Bind(overlay.Protocol())

	assert.NoError(t, leader.Listen())

	nodes := make([]*GRING.Node, 0, dual.BucketSize)

	for i := 0; i < dual.BucketSize; i++ {
		node, err := GRING.NewNode(GRING.WithNodePrivateKey(privateKeys[i+1]))
		assert.NoError(t, err)

		if i != 0 {
			defer node.Close()
		}

		node.Bind(dual.New().Protocol())
		assert.NoError(t, node.Listen())

		_, err = node.Ping(context.Background(), leader.Addr())
		assert.NoError(t, err)

		for _, client := range leader.Inbound() {
			client.WaitUntilReady()
		}

		nodes = append(nodes, node)
	}

	// Query all peer IDs that the leader node knows about.

	before := overlay.Table().Bucket(nodes[0].ID().ID)
	assert.Len(t, before, dual.BucketSize)
	assert.EqualValues(t, dual.BucketSize+1, overlay.Table().NumEntries())
	assert.EqualValues(t, overlay.Table().NumEntries(), len(overlay.Table().Entries()))

	// Close the node that is at the bottom of the bucket.

	nodes[0].Close()

	// Start a follower node that will ping the leader node, and cause an eviction of node 0's routing entry.

	follower, err := GRING.NewNode(GRING.WithNodePrivateKey(privateKeys[len(privateKeys)-1]))
	assert.NoError(t, err)
	defer follower.Close()

	follower.Bind(dual.New().Protocol())
	assert.NoError(t, follower.Listen())

	_, err = follower.Ping(context.Background(), leader.Addr())
	assert.NoError(t, err)

	for _, client := range leader.Inbound() {
		client.WaitUntilReady()
	}

	// Query all peer IDs that the leader node knows about again, and check that node 0 was evicted and that
	// the follower node has been put to the head of the bucket.

	after := overlay.Table().Bucket(nodes[0].ID().ID)
	assert.Len(t, after, dual.BucketSize)
	assert.EqualValues(t, dual.BucketSize+1, overlay.Table().NumEntries())
	assert.EqualValues(t, overlay.Table().NumEntries(), len(overlay.Table().Entries()))

	assert.EqualValues(t, after[0].Address, follower.Addr())
	assert.NotContains(t, after, nodes[0].ID())
}

func TestDiscoveryAcrossThreeNodes(t *testing.T) {
	defer goleak.VerifyNone(t)

	a, err := GRING.NewNode()
	assert.NoError(t, err)
	defer a.Close()

	b, err := GRING.NewNode()
	assert.NoError(t, err)
	defer b.Close()

	c, err := GRING.NewNode()
	assert.NoError(t, err)
	defer c.Close()

	ka := dual.New()
	a.Bind(ka.Protocol())

	kb := dual.New()
	b.Bind(kb.Protocol())

	kc := dual.New()
	c.Bind(kc.Protocol())

	assert.NoError(t, a.Listen())
	assert.NoError(t, b.Listen())
	assert.NoError(t, c.Listen())

	assert.NoError(t, kb.Ping(context.TODO(), a.Addr()))

	assert.Equal(t, len(a.Inbound())+len(a.Outbound()), 1)
	assert.Equal(t, len(b.Inbound())+len(b.Outbound()), 1)
	assert.Equal(t, len(c.Inbound())+len(c.Outbound()), 0)

	assert.NoError(t, kc.Ping(context.TODO(), a.Addr()))

	assert.Equal(t, len(a.Inbound())+len(a.Outbound()), 2)
	assert.Equal(t, len(b.Inbound())+len(b.Outbound()), 1)
	assert.Equal(t, len(c.Inbound())+len(c.Outbound()), 1)

	clients := merge(a.Inbound(), a.Outbound(), b.Inbound(), b.Outbound(), c.Inbound(), c.Outbound())

	var wg sync.WaitGroup
	wg.Add(len(clients))

	for _, client := range clients {
		client := client

		go func() {
			client.WaitUntilReady()
			wg.Done()
		}()
	}

	wg.Wait()

	assert.Len(t, ka.Discover(), 2)
	assert.Len(t, kb.Discover(), 2)
	assert.Len(t, kc.Discover(), 2)
}
