package linearhash

import (
	"fmt"
)

type Hashable interface {
	Equals(Hashable) bool
	Hash() uint64
}

type LHash struct {
	buckets     []*bucket
	size        int
	bucketCount int
	splitIndex  uint64
	maskHigh    uint64
	maskLow     uint64
}

const (
	bucketCapacity    = 64
	utilizationFactor = 0.75
)

func NewLHash() *LHash {
	buckets := make([]*bucket, 2, 16)
	buckets[0] = newBucket()
	buckets[1] = newBucket()
	return &LHash{
		buckets:     buckets,
		size:        0,
		bucketCount: 2,
		splitIndex:  0,
		maskHigh:    3,
		maskLow:     1,
	}
}

func (lh *LHash) String() string {
	return fmt.Sprintf("LHash with %v entries, %v buckets, %v splitIndex, %v maskHigh, %v maskLow. %v",
		lh.size, lh.bucketCount, lh.splitIndex, lh.maskHigh, lh.maskLow, lh.buckets)
}

func (lh *LHash) Find(key Hashable) interface{} {
	// lh.validate()
	return lh.buckets[lh.bucketIndex(key)].find(key)
}

func (lh *LHash) Put(key Hashable, value interface{}) {
	// lh.validate()
	idx := lh.bucketIndex(key)
	b := lh.buckets[idx]
	b, added, chainDelta := b.put(key, value)
	lh.buckets[idx] = b
	if added {
		lh.size++
	}
	lh.bucketCount += chainDelta
	if lh.needsSplit() {
		lh.split()
	}
}

func (lh *LHash) Remove(key Hashable) {
	// lh.validate()
	idx := lh.bucketIndex(key)
	b, removed, chainDelta := lh.buckets[idx].remove(key)
	lh.buckets[idx] = b
	if removed {
		lh.size--
	}
	lh.bucketCount += chainDelta
}

func (lh *LHash) Length() int {
	return lh.size
}

func (lh *LHash) bucketIndex(key Hashable) uint64 {
	h := key.Hash()
	if hl := h & lh.maskLow; hl >= lh.splitIndex {
		return hl
	} else {
		return h & lh.maskHigh
	}
}

func (lh *LHash) needsSplit() bool {
	return (float32(lh.size) / float32(bucketCapacity*lh.bucketCount)) > utilizationFactor
}

func (lh *LHash) split() {
	/*
		lh.validate()
		ufOld := (float32(lh.size) / float32(bucketCapacity*lh.bucketCount))
		ofOld := lh.bucketCount - len(lh.buckets)
	*/
	sOld := lh.splitIndex
	bOld := lh.buckets[sOld]
	bNew := newBucket()
	lh.buckets = append(lh.buckets, bNew)
	lh.bucketCount++
	lh.splitIndex++
	if 2*lh.splitIndex == uint64(len(lh.buckets)) {
		// we've split everything
		lh.splitIndex = 0
		lh.maskLow = lh.maskHigh
		lh.maskHigh = lh.maskHigh*2 + 1
	}
	var bOldPrev *bucket
	for ; bOld != nil; bOld = bOld.next {
		emptied := true
		for idx := range bOld.entries {
			e := &bOld.entries[idx]
			if e.key == nil {
				continue
			} else if lh.bucketIndex(e.key) == sOld {
				emptied = false
			} else {
				_, _, chainDelta := bNew.put(e.key, e.value)
				lh.bucketCount += chainDelta
				e.key = nil
				e.value = nil
			}
		}
		if emptied {
			if bOldPrev == nil && bOld.next != nil {
				lh.bucketCount--
				lh.buckets[sOld] = bOld.next
			} else if bOldPrev != nil {
				lh.bucketCount--
				bOldPrev.next = bOld.next
			}
		} else {
			bOldPrev = bOld
		}
	}
	/*
		ufNew := (float32(lh.size) / float32(bucketCapacity*lh.bucketCount))
		ofNew := lh.bucketCount - len(lh.buckets)
		fmt.Printf("splitting: %v in %v (%v(%v) -> %v(%v))\n", lh.size, lh.bucketCount, ufOld, ofOld, ufNew, ofNew)
		lh.validate()
	*/
}

func (lh *LHash) validate() {
	bucketCount := 0
	size := 0
	for _, b := range lh.buckets {
		for ; b != nil; b = b.next {
			bucketCount++
			for _, e := range b.entries {
				if e.key != nil {
					size++
				}
			}
		}
	}
	if bucketCount != lh.bucketCount {
		panic(fmt.Sprintf("Expected to find %v buckets, but counted %v", lh.bucketCount, bucketCount))
	}
	if size != lh.size {
		panic(fmt.Sprintf("Expected to find %v entries, but counted %v", lh.size, size))
	}
}

type entry struct {
	key   Hashable
	value interface{}
}

func (e entry) String() string {
	return fmt.Sprintf("(%v -> %v)", e.key, e.value)
}

type bucket struct {
	entries []entry
	next    *bucket
}

func (b bucket) String() string {
	return fmt.Sprintf("{bucket %v; next: %v}", b.entries, b.next)
}

func newBucket() *bucket {
	return &bucket{entries: make([]entry, bucketCapacity)}
}

func (b *bucket) find(key Hashable) interface{} {
	if b == nil {
		return nil
	}
	for _, e := range b.entries {
		if e.key == nil {
			continue
		} else if e.key.Equals(key) {
			return e.value
		}
	}
	return b.next.find(key)
}

func (b *bucket) put(key Hashable, value interface{}) (bNew *bucket, added bool, chainDelta int) {
	if b == nil {
		b = newBucket()
		e := &b.entries[0]
		e.key = key
		e.value = value
		return b, true, 1
	}
	slot := -1
	for idx := range b.entries {
		e := &b.entries[idx]
		if e.key == nil && slot == -1 {
			// we've found a hole for it, let's use it. But we can only
			// use it if we're sure it's not already in this bucket.
			slot = idx
		} else if e.key != nil && e.key.Equals(key) {
			e.value = value
			return b, false, 0
		}
	}
	if slot != -1 {
		e := &b.entries[slot]
		e.key = key
		e.value = value
		// remove from tail, in case
		removed := false
		b.next, removed, chainDelta = b.next.remove(key)
		return b, !removed, chainDelta
	}
	b.next, added, chainDelta = b.next.put(key, value)
	return b, added, chainDelta
}

func (b *bucket) remove(key Hashable) (bNew *bucket, removed bool, chainDelta int) {
	if b == nil {
		return nil, false, 0
	}
	empty := true
	for idx := range b.entries {
		e := &b.entries[idx]
		if e.key == nil {
			continue
		} else if e.key.Equals(key) {
			e.key = nil
			e.value = nil
			if !empty {
				return b, true, 0
			}
			removed = true
		} else {
			empty = false
		}
	}
	switch {
	case removed && empty:
		return b.next, true, -1
	case removed:
		return b, true, 0
	default:
		b.next, removed, chainDelta = b.next.remove(key)
		return b, removed, chainDelta
	}
}
