package database

import (
	"errors"

	"github.com/monetas/bmutil"
	"github.com/monetas/bmutil/identity"
	"github.com/monetas/bmutil/wire"
)

// Errors that the various database functions may return.
var (
	ErrDbClosed          = errors.New("database is closed")
	ErrDuplicateObject   = errors.New("duplicate insert attempted")
	ErrDbDoesNotExist    = errors.New("non-existent database")
	ErrDbUnknownType     = errors.New("non-existent database type")
	ErrNotImplemented    = errors.New("method has not yet been implemented")
	ErrNonexistentObject = errors.New("object doesn't exist in database")
)

// Db defines a generic interface that is used to request and insert data into
// the database. This interface is intended to be agnostic to actual mechanism
// used for backend data storage. The AddDBDriver function can be used to add a
// new backend data storage method.
type Db interface {
	// Close cleanly shuts down the database and syncs all data.
	Close() error

	// ExistsObject returns whether or not an object with the given inventory
	// hash exists in the database.
	ExistsObject(*wire.ShaHash) (bool, error)

	// FetchObjectByHash returns an object from the database as a byte array.
	// It is upto the implementation to decode the byte array.
	FetchObjectByHash(*wire.ShaHash) ([]byte, error)

	// FetchObjectByCounter returns the corresponding object based on the
	// counter. Note that each object type has a different counter, with unknown
	// object having none. Currently, the only objects to have counters are
	// messages and broadcasts because they make little sense for anything else.
	// Counters are meant for used as a convenience method for fetching new data
	// from database since last check.
	FetchObjectByCounter(wire.ObjectType, uint64) ([]byte, error)

	// FetchObjectsFromCounter returns `count' objects which have a counter
	// position starting from `counter'. It also returns the counter value of
	// the last object. Objects are guaranteed to be returned in order of
	// increasing counter.
	FetchObjectsFromCounter(objType wire.ObjectType, counter uint64,
		count uint64) ([][]byte, uint64, error)

	// FetchIdentityByAddress returns identity.Public stored in the form
	// of a PubKey message in the database.
	FetchIdentityByAddress(*bmutil.Address) (*identity.Public, error)

	// ExistsGetPubKey returns whether a getpubkey request for the specified
	// address exists.
	ExistsGetPubKey(*bmutil.Address) (bool, error)

	// FetchRandomObject returns a random unexpired object from the database.
	// Useful for object propagation.
	FetchRandomObject() ([]byte, error)

	// GetCounter returns the highest value of counter that exists for objects
	// of the given type.
	GetCounter(wire.ObjectType) (uint64, error)

	// InsertObject inserts data of the given type into the database.
	InsertObject(wire.ObjectType, []byte) error

	// RemoveObject removes the object with the specified hash from the
	// database.
	RemoveObject(*wire.ShaHash) error

	// RemoveObjectByCounter removes the object with the specified counter value
	// from the database.
	RemoveObjectByCounter(wire.ObjectType, uint64) error

	// RollbackClose discards the recent database changes to the previously
	// saved data at last Sync and closes the database.
	RollbackClose() (err error)

	// Sync verifies that the database is coherent on disk and no
	// outstanding transactions are in flight.
	Sync() (err error)
}

// DriverDB defines a structure for backend drivers to use when they registered
// themselves as a backend which implements the Db interface.
type DriverDB struct {
	DbType   string
	CreateDB func(args ...interface{}) (pbdb Db, err error)
	OpenDB   func(args ...interface{}) (pbdb Db, err error)
}

// driverList holds all of the registered database backends.
var driverList []DriverDB

// AddDBDriver adds a back end database driver to available interfaces.
func AddDBDriver(instance DriverDB) {
	for _, drv := range driverList {
		if drv.DbType == instance.DbType {
			return
		}
	}
	driverList = append(driverList, instance)
}

// CreateDB intializes and opens a database.
func CreateDB(dbtype string, args ...interface{}) (pbdb Db, err error) {
	for _, drv := range driverList {
		if drv.DbType == dbtype {
			return drv.CreateDB(args...)
		}
	}
	return nil, ErrDbUnknownType
}

// OpenDB opens an existing database.
func OpenDB(dbtype string, args ...interface{}) (pbdb Db, err error) {
	for _, drv := range driverList {
		if drv.DbType == dbtype {
			return drv.OpenDB(args...)
		}
	}
	return nil, ErrDbUnknownType
}

// SupportedDBs returns a slice of strings that represent the database drivers
// that have been registered and are therefore supported.
func SupportedDBs() []string {
	var supportedDBs []string
	for _, drv := range driverList {
		supportedDBs = append(supportedDBs, drv.DbType)
	}
	return supportedDBs
}
