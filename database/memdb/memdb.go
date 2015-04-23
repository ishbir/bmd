package memdb

import (
	"sort"
	"sync"

	"github.com/monetas/bmd/database"
	"github.com/monetas/bmutil"
	"github.com/monetas/bmutil/identity"
	"github.com/monetas/bmutil/wire"
)

// counters type serves to enable sorting of uint64 slices using sort.Sort
// function. counters implements sort.Interface.
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

// object is used to represent stored objects in MemDb.
type object struct {
	objType wire.ObjectType
	counter uint64
	data    []byte
}

// MemDb is a concrete implementation of the database.Db interface which provides
// a memory-only database. Since it is memory-only, it is obviously not
// persistent and is mostly only useful for testing purposes.
type MemDb struct {
	// Embed a mutex for safe concurrent access.
	sync.Mutex

	// objectsByHash keeps track of objects by their inventory hash.
	objectsByHash map[wire.ShaHash]*object

	// Following fields hold a mapping from counter to shahash for respective
	// object types.
	msgByCounter       map[uint64]wire.ShaHash
	broadcastByCounter map[uint64]wire.ShaHash

	// closed indicates whether or not the database has been closed and is
	// therefore invalidated.
	closed bool
}

// getCounterMap is a helper function used to get the map which maps counter to
// object hash based on `objType'.
func (db *MemDb) getCounterMap(objType wire.ObjectType) map[uint64]wire.ShaHash {
	// map which contains the counter to check for
	var counterMap map[uint64]wire.ShaHash

	switch objType {
	case wire.ObjectTypeBroadcast:
		counterMap = db.broadcastByCounter
	case wire.ObjectTypeMsg:
		counterMap = db.msgByCounter
	default:
		counterMap = nil
	}
	return counterMap
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
	db.msgByCounter = nil
	db.broadcastByCounter = nil
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
		return object.data, nil
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

	hash, ok := counterMap[counter]
	if !ok {
		return nil, database.ErrNonexistentObject
	}
	obj, ok := db.objectsByHash[hash]
	if !ok {
		panic("alien invasion, not supposed to happen (BUG)")
	}
	return obj.data, nil
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

	counterMap := db.getCounterMap(objType)
	if counterMap == nil {
		return nil, 0, database.ErrNotImplemented
	}

	var c uint64 = 0 // count

	keys := make([]uint64, 0, count)

	// make a slice of keys to retrieve
	for k := range counterMap {
		if k < counter { // discard this element
			continue
		}
		if c >= count { // don't want any more
			break
		}
		keys = append(keys, k)
		c += 1
	}
	sort.Sort(counters(keys))       // sort retrieved keys
	newCounter := keys[len(keys)-1] // counter value of last element
	objects := make([][]byte, 0, c)

	// start storing objects in ascending order
	for _, v := range keys {
		hash := counterMap[v]
		obj, err := db.fetchObjectByHash(&hash)
		// error checking
		if err == database.ErrNonexistentObject {
			panic("alien invasion, not supposed to happen (BUG)")
		} else if err != nil {
			return nil, 0, err // mysterious things happening
		}
		// store object
		objects = append(objects, obj)
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

// GetCounter returns the highest value of counter that exists for objects
// of the given type.
func (db *MemDb) GetCounter(wire.ObjectType) (uint64, error) {
	return 0, database.ErrNotImplemented
}

// InsertObject inserts data of the given type into the database.
func (db *MemDb) InsertObject(wire.ObjectType, []byte) error {
	return database.ErrNotImplemented
}

// RemoveObject removes the object with the specified hash from the
// database.
func (db *MemDb) RemoveObject(*wire.ShaHash) error {
	return database.ErrNotImplemented
}

// RemoveObjectByCounter removes the object with the specified counter value
// from the database.
func (db *MemDb) RemoveObjectByCounter(wire.ObjectType, uint64) error {
	return database.ErrNotImplemented
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
		objectsByHash:      make(map[wire.ShaHash]*object),
		msgByCounter:       make(map[uint64]wire.ShaHash),
		broadcastByCounter: make(map[uint64]wire.ShaHash),
	}
	return &db
}
