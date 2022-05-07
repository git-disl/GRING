package dual

import (
	"container/list"
	"fmt"
	"github.com/theued/GRING"
	"sync"
)

// Table represents a Kademlia routing table.
type Table struct {
	sync.RWMutex

	entries [GRING.SizePublicKey * 8]*list.List
	self    GRING.ID
	size    int

	Direction map[string]bool
	NumOutBound int
	NumInBound int
}

// NewTable instantiates a new routing table whose XOR distance metric is defined with respect to some
// given ID.
func NewTable(self GRING.ID) *Table {
	table := &Table{self: self}

	for i := 0; i < len(table.entries); i++ {
		table.entries[i] = list.New()
	}

	if _, err := table.Update(self); err != nil {
		panic(err)
	}

	table.Direction = make(map[string]bool, 0)
	table.NumOutBound = 0
	table.NumInBound = 0

	return table
}

// Self returns the ID which this routing table's XOR distance metric is defined with respect to.
func (t *Table) Self() GRING.ID {
	return t.self
}

// Last returns the last id of the bucket where target resides within.
func (t *Table) Last(target GRING.PublicKey) GRING.ID {
	t.RLock()
	defer t.RUnlock()

	return t.entries[t.getBucketIndex(target)].Back().Value.(GRING.ID)
}

// Bucket returns all entries of a bucket where target reside within.
func (t *Table) Bucket(target GRING.PublicKey) []GRING.ID {
	t.RLock()
	defer t.RUnlock()

	bucket := t.entries[t.getBucketIndex(target)]
	entries := make([]GRING.ID, 0, bucket.Len())

	for e := bucket.Front(); e != nil; e = e.Next() {
		entries = append(entries, e.Value.(GRING.ID))
	}

	return entries
}

// Update attempts to insert the target node/peer ID into this routing table. If the bucket it was expected
// to be inserted within is full, ErrBucketFull is returned. If the ID already exists in its respective routing
// table bucket, it is moved to the head of the bucket and false is returned. If the ID has yet to exist, it is
// appended to the head of its intended bucket and true is returned.
func (t *Table) Update(target GRING.ID) (bool, error) {
	if target.ID == GRING.ZeroPublicKey {
		return false, nil
	}

	t.Lock()
	defer t.Unlock()

	targetIdx := t.getBucketIndex(target.ID)

	bucket := t.entries[targetIdx]

	for e := bucket.Front(); e != nil; e = e.Next() {
		if e.Value.(GRING.ID).ID == target.ID { // Found the target ID already inside the routing table.
			bucket.MoveToFront(e)
			return false, nil
		}
	}

	//if bucket.Len() < BucketSize || targetIdx == 0 { // The bucket is not yet under full capacity.
		bucket.PushFront(target)
		t.size++
		return true, nil
	//}

	// The bucket is at full capacity. Return ErrBucketFull.

	return false, fmt.Errorf("cannot insert id %x into routing table: %w", target.ID, ErrBucketFull)
}

// Recorded returns true if target is already recorded in this routing table.
func (t *Table) Recorded(target GRING.PublicKey) bool {
	t.RLock()
	defer t.RUnlock()

	bucket := t.entries[t.getBucketIndex(target)]

	for e := bucket.Front(); e != nil; e = e.Next() {
		if e.Value.(GRING.ID).ID == target {
			return true
		}
	}

	return false
}

// Delete removes target from this routing table. It returns the id of the delted target and true if found, or
// a zero-value ID and false otherwise.
func (t *Table) Delete(target GRING.PublicKey) (GRING.ID, bool) {
	t.Lock()
	defer t.Unlock()

	bucket := t.entries[t.getBucketIndex(target)]

	for e := bucket.Front(); e != nil; e = e.Next() {
		id := e.Value.(GRING.ID)

		if id.ID == target {
			bucket.Remove(e)
			t.size--
			return id, true
		}
	}

	return GRING.ID{}, false
}

// DeleteByAddress removes the first occurrence of an id with target as its address from this routing table. It
// returns the id of the deleted target and true if found, or a zero-value ID and false otherwise.
func (t *Table) DeleteByAddress(target string) (GRING.ID, bool) {
	t.Lock()
	defer t.Unlock()

	for _, bucket := range t.entries {
		for e := bucket.Front(); e != nil; e = e.Next() {
			id := e.Value.(GRING.ID)

			if id.Address == target {
			        fmt.Printf("DeleteByAddress : %s\n",id.Address)
				bucket.Remove(e)
				t.size--

                                // remove direction too
                                isInbound, found := t.Direction[id.ID.String()]
	                        if found{
                                    if isInbound {
                                        if t.NumInBound > 0{
                                            t.NumInBound = t.NumInBound - 1
                                        }
                                    }else{
                                        if t.NumOutBound > 0{
                                            t.NumOutBound = t.NumOutBound - 1
                                        }
                                    }
                                    //fmt.Printf("RemoveDirection: %s isInbound:%d inboud:%d outbound:%d\n",
				    //           id.String()[:8], isInbound,t.NumInBound, t.NumOutBound)
                                    delete(t.Direction, id.ID.String())
				}

			        return id, true
			}
		}
	}

	return GRING.ID{}, false
}

// return k peers
func (t *Table) KPeers(k int) []GRING.ID {
	return t.KEntries(k)
}

// Peers returns BucketSize closest peer IDs to the ID which this routing table's distance metric is defined against.
func (t *Table) Peers() []GRING.ID {
	return t.FindClosest(t.self.ID, BucketSize)
}

// FindClosest returns the k closest peer IDs to target, and sorts them based on how close they are.
func (t *Table) FindClosest(target GRING.PublicKey, k int) []GRING.ID {
	var closest []GRING.ID

	f := func(bucket *list.List) {
		for e := bucket.Front(); e != nil; e = e.Next() {
			id := e.Value.(GRING.ID)

			if id.ID != target {
				closest = append(closest, id)
			}
		}
	}

	t.RLock()
	defer t.RUnlock()

	idx := t.getBucketIndex(target)

	f(t.entries[idx])

	for i := 1; len(closest) < k && (idx-i >= 0 || idx+i < len(t.entries)); i++ {
		if idx-i >= 0 {
			f(t.entries[idx-i])
		}

		if idx+i < len(t.entries) {
			f(t.entries[idx+i])
		}
	}

	closest = SortByDistance(target, closest)

	if len(closest) > k {
		closest = closest[:k]
	}

	return closest
}

// Entries returns all stored ids in this routing table.
func (t *Table) KEntries(k int) []GRING.ID {
	t.RLock()
	defer t.RUnlock()
	i := k

	entries := make([]GRING.ID, 0, t.size)

	for _, bucket := range t.entries {
		for e := bucket.Front(); e != nil; e = e.Next() {
		        if i==0 {
			    return entries
			}
			entries = append(entries, e.Value.(GRING.ID))
			i = i-1
		}
	}

	return entries
}


// Entries returns all stored ids in this routing table.
func (t *Table) Entries() []GRING.ID {
	t.RLock()
	defer t.RUnlock()

	entries := make([]GRING.ID, 0, t.size)

	for _, bucket := range t.entries {
		for e := bucket.Front(); e != nil; e = e.Next() {
			entries = append(entries, e.Value.(GRING.ID))
		}
	}

	return entries
}

// NumEntries returns the total amount of ids stored in this routing table.
func (t *Table) NumEntries() int {
	t.RLock()
	defer t.RUnlock()

	return t.size
}

func (t *Table) getBucketIndex(target GRING.PublicKey) int {
	l := PrefixLen(XOR(target[:], t.self.ID[:]))
	if l == GRING.SizePublicKey*8 {
		return l - 1
	}

	return l
}

func (t *Table) SetDirection(id GRING.PublicKey, isInbound bool) {
        t.RLock()
        defer t.RUnlock()

	_, found := t.Direction[id.String()]
	if found{
	    return
	}else{
	    t.Direction[id.String()] = isInbound
	}

	if isInbound {
	    t.NumInBound = t.NumInBound + 1
        }else{
	    t.NumOutBound = t.NumOutBound + 1
	}

        //fmt.Printf("SetDirection: %s isInbound:%d inboud:%d outbound:%d\n",id.String()[:8], isInbound,t.NumInBound, t.NumOutBound)
	return
}

func (t *Table) GetDirection(id GRING.PublicKey) (bool, bool) {
        t.RLock()
        defer t.RUnlock()

        rst, found := t.Direction[id.String()]
	return rst, found
}

func (t *Table) RemoveDirection(id GRING.PublicKey) {
        t.RLock()
        defer t.RUnlock()

        isInbound, found := t.Direction[id.String()]
	if !found{
	    return
	}
        if isInbound {
            if t.NumInBound > 0{
                t.NumInBound = t.NumInBound - 1
            }
        }else{
            if t.NumOutBound > 0{
                t.NumOutBound = t.NumOutBound - 1
            }
        }
        //fmt.Printf("RemoveDirection: %s isInbound:%d inboud:%d outbound:%d\n",id.String()[:8], isInbound,t.NumInBound, t.NumOutBound)
        delete(t.Direction, id.String())
	return
}

func (t *Table) GetNumOutBound() int {
        //fmt.Printf("GetNumOutBound : %d\n",t.NumOutBound)
        return t.NumOutBound
}

func (t *Table) IncreaseOutBound() {
        t.RLock()
        defer t.RUnlock()
        t.NumOutBound = t.NumOutBound + 1
	return
}

func (t *Table) DecreaseOutBound() {
        t.RLock()
        defer t.RUnlock()
        if t.NumOutBound > 0 {
            t.NumOutBound = t.NumOutBound - 1
        }
	return
}

func (t *Table) GetNumInBound() int {
        //fmt.Printf("GetNumInBound : %d\n",t.NumInBound)
        return t.NumInBound
}

func (t *Table) IncreaseInBound() {
        t.RLock()
        defer t.RUnlock()
        t.NumInBound = t.NumInBound + 1
	return
}

func (t *Table) DecreaseInBound() {
        t.RLock()
        defer t.RUnlock()
        if t.NumInBound > 0 {
            t.NumInBound = t.NumInBound - 1
        }
	return
}

