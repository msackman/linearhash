package linearhash

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	NewLHash()
}

// These first five tests are pretty dumb as they are dependent on
// implementation details and only really make sense by inspecting the
// log output.
func TestPutGetBucket0(t *testing.T) {
	// if we put just even numbers, we should only fill bucket 0, have no overflows, and no splits
	h := NewLHash()
	for i := HashableUInt(0); i < bucketCapacity; i += 2 {
		h.Put(i, fmt.Sprintf("hello%v", i))
	}
	t.Log(h)
	for i := HashableUInt(0); i < bucketCapacity; i += 2 {
		result := h.Find(i)
		expected := fmt.Sprintf("hello%v", i)
		if s, ok := result.(string); result == nil || !ok || s != expected {
			t.Fatalf("Failed to retrieve string value: %v", result)
		}
	}
}

func TestPutGetBucket0Overflow(t *testing.T) {
	// if we put more even numbers, we should fill bucket 0, with 1 overflow, and no splits
	h := NewLHash()
	for i := HashableUInt(0); i < 2*bucketCapacity; i += 2 {
		h.Put(i, fmt.Sprintf("hello%v", i))
	}
	t.Log(h)
	for i := HashableUInt(0); i < 2*bucketCapacity; i += 2 {
		result := h.Find(i)
		expected := fmt.Sprintf("hello%v", i)
		if s, ok := result.(string); result == nil || !ok || s != expected {
			t.Fatalf("Failed to retrieve string value: %v", result)
		}
	}
}

func TestPutGetBucket1(t *testing.T) {
	// if we put just odd numbers, we should only fill bucket 1, have no overflows, and no splits
	h := NewLHash()
	for i := HashableUInt(1); i < bucketCapacity; i += 2 {
		h.Put(i, fmt.Sprintf("hello%v", i))
	}
	t.Log(h)
	for i := HashableUInt(1); i < bucketCapacity; i += 2 {
		result := h.Find(i)
		expected := fmt.Sprintf("hello%v", i)
		if s, ok := result.(string); result == nil || !ok || s != expected {
			t.Fatalf("Failed to retrieve string value: %v", result)
		}
	}
}

func TestPutGetBucket1Overflow(t *testing.T) {
	// if we put more odd numbers, we should fill bucket 1, with 1 overflow, and no splits
	h := NewLHash()
	for i := HashableUInt(0); i < 2*bucketCapacity; i += 2 {
		h.Put(i, fmt.Sprintf("hello%v", i))
	}
	t.Log(h)
	for i := HashableUInt(0); i < 2*bucketCapacity; i += 2 {
		result := h.Find(i)
		expected := fmt.Sprintf("hello%v", i)
		if s, ok := result.(string); result == nil || !ok || s != expected {
			t.Fatalf("Failed to retrieve string value: %v", result)
		}
	}
}

func TestFillBuckets01Split(t *testing.T) {
	// if we put 0 .. 2*bucketCapacity we should fill buckets 0 and
	// 1. On 1.5*bucketCapacity, 0 should split. Then we continue with
	// adding up to 2*bucketCapacity. That should leave 1 with
	// bucketCapacity, and 0 and 2 with 0.5*bucketCapacity each.
	h := NewLHash()
	for i := HashableUInt(0); i < 2*bucketCapacity; i += 1 {
		h.Put(i, fmt.Sprintf("hello%v", i))
	}
	t.Log(h)
	for i := HashableUInt(0); i < 2*bucketCapacity; i += 1 {
		result := h.Find(i)
		expected := fmt.Sprintf("hello%v", i)
		if s, ok := result.(string); result == nil || !ok || s != expected {
			t.Fatalf("Failed to retrieve string value: %v", result)
		}
	}
}

// This test however is brilliant. I love this approach to testing.
func TestSoak(t *testing.T) {
	// Sadly undirected, but nevertheless fairly sensible way of doing
	// testing. Takes about 100 seconds for 10million ops. If you turn
	// all the log calls off, it takes 25 seconds for 10million ops!
	seed := time.Now().UnixNano()
	// seed = int64(1475356421782933095)
	t.Logf("Seed: %v", seed)
	rng := rand.New(rand.NewSource(seed))
	// we use contents to mirror the state of the LHash
	contents := make(map[HashableUInt]string)
	h := NewLHash()
	// if we fail, we want to be sure we get the final state of the
	// LHash out.
	defer func() { t.Log(h) }()
	for i := 10000000; i > 0; i-- {
		lenContents := len(contents)
		// we bias creation of new keys by 999 with 1 more for reset
		op := rng.Intn((3*lenContents)+1000) - 1000
		opClass := 0
		opArg := 0
		if lenContents > 0 {
			opClass = op / lenContents
			opArg = op % lenContents
		}
		switch {
		case op == -1: // reset
			h = NewLHash()
			contents = make(map[HashableUInt]string)
			t.Log("NewLHash")
		case op < -1: // add new key
			key := HashableUInt(lenContents)
			value := fmt.Sprintf("Hello%v-%v", i, key)
			h.Put(key, value)
			t.Logf("Put(%v, %v)", key, value)
			contents[key] = value
		case opClass == 0: // find key
			key := HashableUInt(opArg)
			value := contents[key]
			inContents := len(value) != 0
			t.Logf("Find(%v) == %v ? %v", key, value, inContents)
			result := h.Find(key)
			if s, ok := result.(string); inContents && (result == nil || !ok || s != value) {
				t.Fatalf("%v Failed to retrieve string value: %v", key, result)
			} else if !inContents && result != nil {
				t.Fatalf("Got result even after remove: %v", result)
			}
		case opClass == 1: // remove key
			key := HashableUInt(opArg)
			inContents := len(contents[key]) != 0
			t.Logf("Remove(%v) ? %v", key, inContents)
			h.Remove(key)
			if inContents {
				contents[key] = ""
			}
		case opClass == 2: // re-put existing key
			key := HashableUInt(opArg)
			value := contents[key]
			inContents := len(value) != 0
			if !inContents {
				value = fmt.Sprintf("Hello%v-%v", i, key)
				contents[key] = value
			}
			t.Logf("Put(%v, %v) ? %v", key, value, inContents)
			h.Put(key, value)
		default:
			t.Fatalf("Unexpected op %v (class: %v; arg %v)", op, opClass, opArg)
		}
	}
}

type HashableUInt uint64

func (a HashableUInt) Equals(b Hashable) bool {
	bHUInt, ok := b.(HashableUInt)
	return ok && bHUInt == a
}

func (a HashableUInt) Hash() uint64 {
	return uint64(a)
}
