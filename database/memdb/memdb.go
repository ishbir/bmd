package memdb

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"github.com/monetas/bmd/database"
	"github.com/monetas/bmutil"
	"github.com/monetas/bmutil/identity"
	"github.com/monetas/bmutil/wire"
)

const (
	panicMessage = "ALIEN INVASION IN PROGRESS. (bug)"
)

// counters type serves to enable sorting of uint64 slices using sort.Sort
// function. Implements sort.Interface.
type counters []uint64

func (c counters) Len() int {
	return len(c)
}

func (c counters) Less(i, j int) bool {
	return c[i] < c[j]
}

func (c counters) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// newShaHashFromStr converts the passed big-endian hex string into a
// wire.ShaHash.  It only differs from the one available in wire in that it
// ignores the error since it will only (and must only) be called with
// hard-coded, and therefore known good, hashes.
func newShaHashFromStr(hexStr string) *wire.ShaHash {
	sha, _ := wire.NewShaHashFromStr(hexStr)
	return sha
}

// counter includes a map to a kind of object and
type counter struct {
	// holds a mapping from counter to shahash for some object types.
	ByCounter map[uint64]*wire.ShaHash
	// keep track of current counter positions (last element added)
	CounterPos uint64
}

func (cmap *counter) Insert(hash *wire.ShaHash) {
	cmap.CounterPos += 1                   // increment, new item.
	cmap.ByCounter[cmap.CounterPos] = hash // insert to counter map
}

// MemDb is a concrete implementation of the database.Db interface which provides
// a memory-only database. Since it is memory-only, it is obviously not
// persistent and is mostly only useful for testing purposes.
type MemDb struct {
	// Embed a mutex for safe concurrent access.
	sync.Mutex

	// objectsByHash keeps track of unexpired objects by their inventory hash.
	objectsByHash map[wire.ShaHash][]byte

	// pubkeyByTag keeps track of all public keys (even expired) by their
	// tag (which can be calculated from the address).
	pubKeyByTag map[wire.ShaHash][]byte

	// counters for respective object types.
	msgCounter       *counter
	broadcastCounter *counter
	pubKeyCounter    *counter
	getPubKeyCounter *counter

	// counters for unknown objects.
	unknownObjCounter map[wire.ObjectType]*counter

	// closed indicates whether or not the database has been closed and is
	// therefore invalidated.
	closed bool
}

// getCounterMap is a helper function used to get the map which maps counter to
// object hash based on `objType'.
func (db *MemDb) getCounterMap(objType wire.ObjectType) *counter {

	switch objType {
	case wire.ObjectTypeBroadcast:
		return db.broadcastCounter
	case wire.ObjectTypeMsg:
		return db.msgCounter
	case wire.ObjectTypePubKey:
		return db.pubKeyCounter
	case wire.ObjectTypeGetPubKey:
		return db.getPubKeyCounter
	default:
		var count *counter
		var ok bool
		if count, ok = db.unknownObjCounter[objType]; !ok {
			count = &counter{make(map[uint64]*wire.ShaHash), 0}
			db.unknownObjCounter[objType] = count
		}
		return count
	}
}

// Close cleanly shuts down database.  This is part of the database.Db interface
// implementation.
//
// All data is purged upon close with this implementation since it is a
// memory-only database.
func (db *MemDb) Close() error {
	db.Lock()
	defer db.Unlock()

	if db.closed {
		return database.ErrDbClosed
	}

	db.objectsByHash = nil
	db.pubKeyByTag = nil
	db.msgCounter = nil
	db.broadcastCounter = nil
	db.pubKeyCounter = nil
	db.getPubKeyCounter = nil
	db.unknownObjCounter = nil
	db.closed = true
	return nil
}

// ExistsObject returns whether or not an object with the given inventory hash
// exists in the database. This is part of the database.Db interface
// implementation.
func (db *MemDb) ExistsObject(hash *wire.ShaHash) (bool, error) {
	db.Lock()
	defer db.Unlock()

	if db.closed {
		return false, database.ErrDbClosed
	}

	if _, exists := db.objectsByHash[*hash]; exists {
		return true, nil
	}

	return false, nil
}

// No locks here, meant to be used inside public facing functions.
func (db *MemDb) fetchObjectByHash(hash *wire.ShaHash) ([]byte, error) {
	if object, exists := db.objectsByHash[*hash]; exists {
		return object, nil
	}

	return nil, database.ErrNonexistentObject
}

// FetchObjectByHash returns an object from the database as a byte array.
// It is upto the implementation to decode the byte array. This is part of the
// database.Db interface implementation.
//
// This implementation does not use any additional cache since the entire
// database is already in memory.
func (db *MemDb) FetchObjectByHash(hash *wire.ShaHash) ([]byte, error) {
	db.Lock()
	defer db.Unlock()

	if db.closed {
		return nil, database.ErrDbClosed
	}

	return db.fetchObjectByHash(hash)
}

// FetchObjectByCounter returns an object from the database as a byte array
// based on the object type and counter value. The implementation may cache the
// underlying data if desired. This is part of the database.Db interface
// implementation.
//
// This implementation does not use any additional cache since the entire
// database is already in memory.
func (db *MemDb) FetchObjectByCounter(objType wire.ObjectType,
	counter uint64) ([]byte, error) {
	db.Lock()
	defer db.Unlock()
	if db.closed {
		return nil, database.ErrDbClosed
	}

	counterMap := db.getCounterMap(objType)
	if counterMap == nil {
		return nil, database.ErrNotImplemented
	}

	//fmt.Printf("fetching object by counter. Type %d, counter %d\n", objType, counter)
	//fmt.Println("got this counter object: ", counterMap.ByCounter)
	hash, ok := counterMap.ByCounter[counter]
	if !ok {
		return nil, database.ErrNonexistentObject
	}
	obj, err := db.fetchObjectByHash(hash)
	if err != nil {
		panic(panicMessage)
	}
	return obj, nil
}

// FetchObjectsFromCounter returns objects from the database, which have a
// counter value starting from `counter' and a type of `objType` as a slice of
// byte slices with the counter of the last object. Objects are guaranteed to be
// returned in order of increasing counter. The implementation may cache the
// underlying data if desired. This is part of the database.Db interface
// implementation.
//
// This implementation does not use any additional cache since the entire
// database is already in memory.
func (db *MemDb) FetchObjectsFromCounter(objType wire.ObjectType, counter uint64,
	count uint64) ([][]byte, uint64, error) {
	db.Lock()
	defer db.Unlock()
	if db.closed {
		return nil, 0, database.ErrDbClosed
	}

	counterMap := db.getCounterMap(objType)
	if counterMap == nil {
		return nil, 0, database.ErrNotImplemented
	}

	var c uint64 = 0 // count

	keys := make([]uint64, 0, count)

	// make a slice of keys to retrieve
	for k := range counterMap.ByCounter {
		if k < counter { // discard this element
			continue
		}
		if c >= count { // don't want any more
			break
		}
		keys = append(keys, k)
		c += 1
	}
	sort.Sort(counters(keys)) // sort retrieved keys
	var newCounter uint64
	if len(keys) == 0 {
		newCounter = 0
	} else {
		newCounter = keys[len(keys)-1] // counter value of last element
	}
	objects := make([][]byte, 0, c)

	// start fetching objects in ascending order
	for _, v := range keys {
		hash := counterMap.ByCounter[v]
		obj, err := db.fetchObjectByHash(hash)
		// error checking
		if err == database.ErrNonexistentObject {
			panic(panicMessage)
		} else if err != nil {
			return nil, 0, err // mysterious things happening
		}
		// ensure that database and returned byte arrays are separate
		objCopy := make([]byte, len(obj))
		copy(objCopy, obj)
		// append object to output
		objects = append(objects, objCopy)
	}

	return objects, newCounter, nil
}

// FetchIdentityByAddress returns identity.Public stored in the form
// of a PubKey message in the database. It needs to go through all the public
// keys in the database to find this. The implementation must thus cache
// results, if needed.
func (db *MemDb) FetchIdentityByAddress(addr *bmutil.Address) (*identity.Public,
	error) {
	return nil, database.ErrNotImplemented
	/*
		db.Lock()
		defer db.Unlock()
		if db.closed {
			return nil, database.ErrDbClosed
		}

		for k, v := range db.pubKeyByCounter {
			hash := db.pubKeyByCounter[v]
			pubkey, err := db.fetchObjectByHash(&hash)
			// error checking
			if err == database.ErrNonexistentObject {
				panic("alien invasion, not supposed to happen (BUG)")
			} else if err != nil {
				return nil, err // mysterious things happening
			}

			// try to decode pubkey based on the address
			switch addr.Version {
			case 2:
			case 3:
			case 4:
			default:
				return nil, bmutil.ErrUnknownAddressType
			}
		}
	*/
}

// ExistsGetPubKey returns whether a getpubkey request for the specified
// address exists. It needs to go through all the public key requests in the
// database to find this. The implementation must thus cache results, if needed.
func (db *MemDb) ExistsGetPubKey(*bmutil.Address) (bool, error) {
	return false, database.ErrNotImplemented
}

// FetchRandomObject returns a random unexpired object from the database.
// Useful for object propagation.
func (db *MemDb) FetchRandomObject() ([]byte, error) {
	return nil, database.ErrNotImplemented
}

// getCounterRef returns a reference to object counter of the given object type.
// It is a convenience method used by various other functions in this package.
func (db *MemDb) getCounterRef(objType wire.ObjectType) *uint64 {
	counterMap := db.getCounterMap(objType)
	if counterMap == nil { // Should not happen.
		panic(panicMessage)
	}
	return &(counterMap.CounterPos)
}

// GetCounter returns the highest value of counter that exists for objects
// of the given type.
func (db *MemDb) GetCounter(objType wire.ObjectType) (uint64, error) {
	db.Lock()
	defer db.Unlock()
	if db.closed {
		return 0, database.ErrDbClosed
	}

	if c := db.getCounterRef(objType); c != nil {
		return *c, nil
	}
	return 0, database.ErrNotImplemented // incompatible object
}

// InsertObject inserts data of the given type and hash into the database.
// It returns the calculated/stored inventory hash as well as the counter
// position (if object type has a counter associated with it).
func (db *MemDb) InsertObject(data []byte) (*wire.ShaHash, uint64, error) {
	db.Lock()
	defer db.Unlock()
	if db.closed {
		return nil, 0, database.ErrDbClosed
	}

	_, _, objType, _, _, err := wire.DecodeMsgObjectHeader(bytes.NewReader(data))
	if err != nil { // impossible, all objects must be valid
		return nil, 0, err
	}

	hash, err := wire.NewShaHash(bmutil.CalcInventoryHash(data))
	if err != nil {
		return nil, 0, err
	}

	// ensure that modifying input args doesn't change contents in database
	dataInsert := make([]byte, len(data))
	copy(dataInsert, data)

	//fmt.Println("inserting object ", dataInsert, " into counter map ", objType)

	// handle pubkeys
	if objType == wire.ObjectTypePubKey {
		msg := new(wire.MsgPubKey)
		err = msg.Decode(bytes.NewReader(data))
		if err != nil {
			return nil, 0, err
		}

		var tag []byte

		switch msg.Version {
		case wire.SimplePubKeyVersion:
			fallthrough
		case wire.ExtendedPubKeyVersion:
			id, err := identity.IdentityFromPubKeyMsg(msg)
			if err != nil { // invalid encryption/signing keys
				return nil, 0, err
			}
			tag = id.Address.Tag()
		case wire.EncryptedPubKeyVersion:
			tag = msg.Tag.Bytes() // directly included
		}
		tagH, err := wire.NewShaHash(tag)
		if err != nil {
			return nil, 0, err
		}
		db.pubKeyByTag[*tagH] = dataInsert // insert pubkey
	}

	// insert object into the object hash table
	db.objectsByHash[*hash] = dataInsert

	// get counter map
	counterMap := db.getCounterMap(objType)

	if counterMap == nil { // err because insert into db could have succeeded and yet produced an error
		return hash, 0, nil
	} else {
		counterMap.Insert(hash)
		return hash, counterMap.CounterPos, nil
	}
}

// RemoveObject removes the object with the specified hash from the database.
func (db *MemDb) RemoveObject(hash *wire.ShaHash) error {
	db.Lock()
	defer db.Unlock()
	if db.closed {
		return database.ErrDbClosed
	}

	obj, ok := db.objectsByHash[*hash]
	if !ok {
		return database.ErrNonexistentObject
	}

	// check and remove object from counter maps
	_, _, objType, _, _, err := wire.DecodeMsgObjectHeader(bytes.NewReader(obj))
	counterMap := db.getCounterMap(objType)
	if err != nil { // impossible, all objects must be valid
		panic(panicMessage)
	}
	if counterMap != nil {
		for k, v := range counterMap.ByCounter { // go through each element
			if v.IsEqual(hash) { // we got a match, so delete
				delete(counterMap.ByCounter, k)
				break
			}
		}
	}

	// remove object from object map
	delete(db.objectsByHash, *hash) // done!

	return nil
}

// RemoveObjectByCounter removes the object with the specified counter value
// from the database.
func (db *MemDb) RemoveObjectByCounter(objType wire.ObjectType,
	counter uint64) error {

	db.Lock()
	defer db.Unlock()
	if db.closed {
		return database.ErrDbClosed
	}

	counterMap := db.getCounterMap(objType)
	if counterMap == nil { // undefined operation
		return database.ErrNotImplemented
	}

	hash, ok := counterMap.ByCounter[counter]
	if !ok {
		return database.ErrNonexistentObject
	}

	delete(counterMap.ByCounter, counter) // delete counter reference
	delete(db.objectsByHash, *hash)       // delete object itself
	return nil
}

// RemoveExpiredObjects prunes all objects, except PubKey, whose expiry time
// has passed (along with a margin of 3 hours).
func (db *MemDb) RemoveExpiredObjects() error {
	db.Lock()
	defer db.Unlock()
	if db.closed {
		return database.ErrDbClosed
	}

	for hash, obj := range db.objectsByHash {
		_, expiresTime, objType, _, _, err := wire.DecodeMsgObjectHeader(bytes.NewReader(obj))
		if err != nil { // impossible, all objects must be valid
			panic(panicMessage)
		}
		// current time - 3 hours
		if time.Now().Add(-time.Hour*3).After(expiresTime) && objType != wire.ObjectTypePubKey { // expired
			counterMap := db.getCounterMap(objType)
			if counterMap != nil {
				for k, v := range counterMap.ByCounter { // go through each element
					if v.IsEqual(&hash) { // we got a match, so delete
						delete(counterMap.ByCounter, k)
						break
					}
				}
			}

			// remove object from object map
			delete(db.objectsByHash, hash)
		}
	}
	return nil
}

// RemovePubKey removes a PubKey from the PubKey store with the specified
// tag. Note that it doesn't touch the general object store and won't remove
// the public key from there.
func (db *MemDb) RemovePubKey(tag *wire.ShaHash) error {
	db.Lock()
	defer db.Unlock()
	if db.closed {
		return database.ErrDbClosed
	}

	_, ok := db.pubKeyByTag[*tag]
	if !ok {
		return database.ErrNonexistentObject
	}

	delete(db.pubKeyByTag, *tag) // remove
	return nil
}

// RollbackClose discards the recent database changes to the previously saved
// data at last Sync and closes the database. This is part of the database.Db
// interface implementation.
//
// The database is completely purged on close with this implementation since the
// entire database is only in memory. As a result, this function behaves no
// differently than Close.
func (db *MemDb) RollbackClose() error {
	// Rollback doesn't apply to a memory database, so just call Close.
	// Close handles the mutex locks.
	return db.Close()
}

// Sync verifies that the database is coherent on disk and no outstanding
// transactions are in flight. This is part of the database.Db interface
// implementation.
//
// This implementation does not write any data to disk, so this function only
// grabs a lock to ensure it doesn't return until other operations are complete.
func (db *MemDb) Sync() error {
	db.Lock()
	defer db.Unlock()

	if db.closed {
		return database.ErrDbClosed
	}

	// There is nothing extra to do to sync the memory database. However,
	// the lock is still grabbed to ensure the function does not return
	// until other operations are complete.
	return nil
}

// newMemDb returns a new memory-only database ready for block inserts.
func newMemDb() *MemDb {
	db := MemDb{
		objectsByHash:     make(map[wire.ShaHash][]byte),
		pubKeyByTag:       make(map[wire.ShaHash][]byte),
		msgCounter:        &counter{make(map[uint64]*wire.ShaHash), 0},
		broadcastCounter:  &counter{make(map[uint64]*wire.ShaHash), 0},
		pubKeyCounter:     &counter{make(map[uint64]*wire.ShaHash), 0},
		getPubKeyCounter:  &counter{make(map[uint64]*wire.ShaHash), 0},
		unknownObjCounter: make(map[wire.ObjectType]*counter),
	}
	return &db
}
