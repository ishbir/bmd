package memdb_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/monetas/bmd/database"
	"github.com/monetas/bmd/database/memdb"
	"github.com/monetas/bmutil"
	"github.com/monetas/bmutil/wire"
)

func msgToBytes(msg wire.Message) []byte {
	buf := &bytes.Buffer{}
	msg.Encode(buf)
	return buf.Bytes()
}

func makeMemDB(t *testing.T) database.Db {
	driver := database.DriverDB{DbType: "memdb", CreateDB: memdb.CreateDB, OpenDB: memdb.OpenDB}
	database.AddDBDriver(driver)

	db, err := database.CreateDB("memdb")
	if err != nil {
		t.Fatalf("Failed to open test database %v", err)
		return nil
	}

	return db
}

var expires time.Time = time.Now().Add(10 * time.Minute)
var expired time.Time = time.Now().Add(-10 * time.Minute).Add(-3 * time.Hour)

// A set of pub keys to create fake objects for testing the database.
var pubkey []wire.PubKey = []wire.PubKey{
	wire.PubKey([wire.PubKeySize]byte{
		23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
		39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
		55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
		71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86}),
	wire.PubKey([wire.PubKeySize]byte{
		87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102,
		103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118,
		119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134,
		135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150}),
	wire.PubKey([wire.PubKeySize]byte{
		54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
		70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85,
		86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101,
		102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117}),
	wire.PubKey([wire.PubKeySize]byte{
		118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133,
		134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
		150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165,
		166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181}),
}

var shahash []wire.ShaHash = []wire.ShaHash{
	wire.ShaHash([wire.HashSize]byte{
		98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113,
		114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129}),
	wire.ShaHash([wire.HashSize]byte{
		100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
		116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131}),
}

var ripehash []wire.RipeHash = []wire.RipeHash{
	wire.RipeHash([wire.RipeHashSize]byte{
		78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97}),
	wire.RipeHash([wire.RipeHashSize]byte{
		80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99}),
}

// Some bitmessage objects that we use for testing. Two of each.
var testObj [][]wire.Message = [][]wire.Message{
	[]wire.Message{
		wire.NewMsgGetPubKey(654, expires, 4, 1, &ripehash[0], &shahash[0]),
		wire.NewMsgGetPubKey(654, expired, 4, 1, &ripehash[1], &shahash[1]),
	},
	[]wire.Message{
		wire.NewMsgPubKey(543, expires, 4, 1, 2, &pubkey[0], &pubkey[1], 3, 5,
			[]byte{4, 5, 6, 7, 8, 9, 10}, &shahash[0], []byte{11, 12, 13, 14, 15, 16, 17, 18}),
		wire.NewMsgPubKey(543, expired, 4, 1, 2, &pubkey[2], &pubkey[3], 3, 5,
			[]byte{4, 5, 6, 7, 8, 9, 10}, &shahash[1], []byte{11, 12, 13, 14, 15, 16, 17, 18}),
	},
	[]wire.Message{
		wire.NewMsgMsg(765, expires, 1, 1,
			[]byte{90, 87, 66, 45, 3, 2, 120, 101, 78, 78, 78, 7, 85, 55, 2, 23},
			1, 1, 2, &pubkey[0], &pubkey[1], 3, 5, &ripehash[0], 1,
			[]byte{21, 22, 23, 24, 25, 26, 27, 28},
			[]byte{20, 21, 22, 23, 24, 25, 26, 27},
			[]byte{19, 20, 21, 22, 23, 24, 25, 26}),
		wire.NewMsgMsg(765, expired, 1, 1,
			[]byte{90, 87, 66, 45, 3, 2, 120, 101, 78, 78, 78, 7, 85, 55},
			1, 1, 2, &pubkey[2], &pubkey[3], 3, 5, &ripehash[1], 1,
			[]byte{21, 22, 23, 24, 25, 26, 27, 28, 79},
			[]byte{20, 21, 22, 23, 24, 25, 26, 27, 79},
			[]byte{19, 20, 21, 22, 23, 24, 25, 26, 79}),
	},
	[]wire.Message{
		wire.NewMsgBroadcast(876, expires, 1, 1, &shahash[0],
			[]byte{90, 87, 66, 45, 3, 2, 120, 101, 78, 78, 78, 7, 85, 55, 2, 23},
			1, 1, 2, &pubkey[0], &pubkey[1], 3, 5, &ripehash[1], 1,
			[]byte{27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41},
			[]byte{42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56}),
		wire.NewMsgBroadcast(876, expired, 1, 1, &shahash[1],
			[]byte{90, 87, 66, 45, 3, 2, 120, 101, 78, 78, 78, 7, 85, 55},
			1, 1, 2, &pubkey[2], &pubkey[3], 3, 5, &ripehash[0], 1,
			[]byte{27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40},
			[]byte{42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55}),
	},
	[]wire.Message{
		wire.NewMsgUnknownObject(345, expires, wire.ObjectType(4), 1, 1, []byte{77, 82, 53, 48, 96, 1}),
		wire.NewMsgUnknownObject(987, expired, wire.ObjectType(4), 1, 1, []byte{1, 2, 3, 4, 5, 0, 6, 7, 8, 9, 100}),
	},
	[]wire.Message{
		wire.NewMsgUnknownObject(7288, expires, wire.ObjectType(5), 1, 1, []byte{0, 0, 0, 0, 1, 0, 0}),
		wire.NewMsgUnknownObject(7288, expired, wire.ObjectType(5), 1, 1, []byte{0, 0, 0, 0, 0, 0, 0, 99, 98, 97}),
	},
}

// TestClosed ensures that the correct errors are returend when the public functions
// are called on a closed database.
func TestClosed(t *testing.T) {
	db := makeMemDB(t)

	data := msgToBytes(testObj[4][0])
	hash, count, err := db.InsertObject(data)
	if err != nil {
		t.Fatalf("Object not inserted.")
	}

	db.RollbackClose()

	err = db.Sync()
	if err != database.ErrDbClosed {
		t.Errorf("Sinc failed to reurned db closed error.")
	}

	err = db.Close()
	if err != database.ErrDbClosed {
		t.Errorf("Close failed to reurned db closed error.")
	}

	data = msgToBytes(testObj[4][1])
	_, _, err = db.InsertObject(data)
	if err != database.ErrDbClosed {
		t.Errorf("InsertObject failed to reurned db closed error.")
	}

	_, err = db.ExistsObject(hash)
	if err != database.ErrDbClosed {
		t.Errorf("ExistsObject failed to reurned db closed error.")
	}

	err = db.RemoveObject(hash)
	if err != database.ErrDbClosed {
		t.Errorf("RemoveObject failed to reurned db closed error.")
	}

	_, err = db.FetchObjectByHash(hash)
	if err != database.ErrDbClosed {
		t.Errorf("FetchObjectByHash failed to reurned db closed error.")
	}

	err = db.RemoveExpiredObjects()
	if err != database.ErrDbClosed {
		t.Errorf("RemoveExpiredObjects failed to reurned db closed error.")
	}

	_, err = db.FetchObjectByCounter(wire.ObjectType(4), count)
	if err != database.ErrDbClosed {
		t.Errorf("FetchObjectByCounter failed to reurned db closed error.")
	}

	_, _, err = db.FetchObjectsFromCounter(wire.ObjectType(4), count, 10)
	if err != database.ErrDbClosed {
		t.Errorf("FetchObjectsFromCounter failed to reurned db closed error.")
	}

	_, err = db.GetCounter(wire.ObjectType(4))
	if err != database.ErrDbClosed {
		t.Errorf("GetCounter failed to reurned db closed error.")
	}

	err = db.RemoveObjectByCounter(wire.ObjectType(4), count-1)
	if err != database.ErrDbClosed {
		t.Errorf("RemoveObjectByCounter failed to reurned db closed error.")
	}

	err = db.RemovePubKey(&shahash[0])
	if err != database.ErrDbClosed {
		t.Errorf("RemovePubKey failed to reurned db closed error.")
	}

	/*err = db.FetchIdentityByAddress()
	if err != database.ErrDbClosed {
		t.Errorf("FetchIdentityByAddress failed to reurned db closed error.")
	}*/
}

func TestSync(t *testing.T) {
	db := makeMemDB(t)

	err := db.Sync()
	if err != nil {
		t.Errorf("Error returned: %s", err)
	}
}

// TestObject tests InsertObject, ExistsObject, FetchObjectByHash, and RemoveObject
func TestObject(t *testing.T) {
	db := makeMemDB(t)

	for i, object := range testObj {
		data := msgToBytes(object[0])
		_, _, err := db.InsertObject(data)
		if err != nil {
			t.Errorf("Error returned inserting object %d: %s", i, err)
		}

		hash, _ := wire.NewShaHash(bmutil.CalcInventoryHash(msgToBytes(object[0])))

		exists, err := db.ExistsObject(hash)
		if err != nil {
			t.Errorf("Error returned checking for object %d: %s", i, err)
		}
		if !exists {
			t.Errorf("Object %d should be in db but it is not.", i)
		}

		test_data, err := db.FetchObjectByHash(hash)
		if err != nil {
			t.Errorf("Error returned checking for object %d: %s", i, err)
		}
		if !bytes.Equal(data, test_data) {
			t.Errorf("data from object %d returned but does not match.", i)
		}

		err = db.RemoveObject(hash)
		if err != nil {
			t.Errorf("Error returned removing object %d: %s", i, err)
		}

		err = db.RemoveObject(hash)
		if err == nil {
			t.Errorf("Error not returned for removing nonexistant object %d: %s", i, err)
		}

		exists, err = db.ExistsObject(hash)
		if err != nil {
			t.Errorf("Error returned checking for object %d: %s", i, err)
		}
		if exists {
			t.Errorf("Object %d should not be in db but it is.", i)
		}

		_, err = db.FetchObjectByHash(hash)
		if err == nil {
			t.Errorf("Error not returned checking for nonexistent object %d: %s", i, err)
		}
	}
}

// TestCounter tests FetchObjectByCounter, FetchObjectsFromCounter,
// RemoveObjectByCounter, and GetCounter
func TestCounter(t *testing.T) {

	for i := 0; i < len(testObj)-1; i++ {
		db := makeMemDB(t)

		var offsetA int = i
		var offsetB int = i + 1
		var objTypeA wire.ObjectType = wire.ObjectType(offsetA)
		var objTypeB wire.ObjectType = wire.ObjectType(offsetB)

		// Test that the counters start at zero.
		count, err := db.GetCounter(objTypeB)
		if err != nil {
			t.Errorf("Error returned getting counter for object type %d.", objTypeB)
		}
		if count != 0 {
			t.Errorf("Count error: expected 0, got %d", count)
		}
		count, err = db.GetCounter(objTypeA)
		if err != nil {
			t.Errorf("Error returned getting counter for object type %d.", objTypeA)
		}
		if count != 0 {
			t.Errorf("Count error: expected 0, got %d", count)
		}

		//Try to grab an element that is not there.
		_, err = db.FetchObjectByCounter(objTypeA, 1)
		if err == nil {
			t.Error("No error returned for fetching nonexistent object.")
		}

		err = db.RemoveObjectByCounter(objTypeA, 1)
		if err == nil {
			t.Errorf("Error expected for removing object that does not exist", objTypeA)
		}

		// Insert an element and make sure the counter comes out correct.
		dataA0 := msgToBytes(testObj[offsetA][0])
		_, countA0, err := db.InsertObject(dataA0)
		if err != nil {
			t.Errorf("Error returned inserting object %d, %d, type %d : %s", offsetA, 0, objTypeA, err)
		}
		if countA0 != 1 {
			t.Errorf("Count error: expected 1, got %d", countA0)
		}

		// Try to fetch a nonexistent object with a higher counter value.
		_, err = db.FetchObjectByCounter(objTypeA, 2)
		if err == nil {
			t.Errorf("No error returned for fetching nonexistent object")
		}

		// Try to fetch an object that should be there now.
		test_data, err := db.FetchObjectByCounter(objTypeA, 1)
		if err != nil {
			t.Errorf("Error returned for fetching existent object A of type %d: %s", objTypeA, err)
		}
		if !bytes.Equal(test_data, dataA0) {
			t.Error("Data returned does not match data entered.")
		}

		dataA1 := msgToBytes(testObj[offsetA][1])
		_, count, err = db.InsertObject(dataA1)
		if err != nil {
			t.Errorf("Error returned inserting object A0: %s", err)
		}
		if count != 2 {
			t.Errorf("Count error for type %d: expected 2, got %d", objTypeA, count)
		}

		// Try fetching the new object.
		test_data, err = db.FetchObjectByCounter(objTypeA, 2)
		if err != nil {
			t.Errorf("Error returned for fetching existent object A of type %d: %s", objTypeA, err)
		}
		if !bytes.Equal(test_data, dataA1) {
			t.Error("Data returned does not match data entered.")
		}

		// Test that the counter has incremented.
		count, err = db.GetCounter(objTypeA)
		if err != nil {
			t.Errorf("Error returned getting counter for object type %d.", objTypeA)
		}
		if count != 2 {
			t.Errorf("Count error for type %d: expected 2, got %d", objTypeA, count)
		}

		//Try to grab an element that is not there.
		_, err = db.FetchObjectByCounter(objTypeB, 1)
		if err == nil {
			t.Error("No error returned for fetching nonexistent object of type ", objTypeB)
		}

		dataB0 := msgToBytes(testObj[offsetB][0])
		_, count, err = db.InsertObject(dataB0)
		if err != nil {
			t.Errorf("Error returned inserting object A0: %s", err)
		}
		if count != 1 {
			t.Errorf("Count error for type %d: expected 1, got %d", objTypeB, count)
		}

		// Test FetchObjectsFromCounter for various input values.
		fetch, n, err := db.FetchObjectsFromCounter(objTypeA, 3, 2)
		if err != nil {
			t.Errorf("Error returned for what should have returned an empty slice.")
		}
		if len(fetch) != 0 {
			t.Errorf("Incorrect slice size returned.")
		}
		if n != 0 {
			t.Errorf("Incorrect counter value returned.")
		}

		fetch, n, err = db.FetchObjectsFromCounter(objTypeA, 1, 3)
		if err != nil {
			t.Errorf("Error returned fetching objects of type %d", objTypeA)
		}
		if len(fetch) != 2 {
			t.Errorf("Incorrect slice size returned.")
		}
		if n != 2 {
			t.Errorf("Incorrect counter value returned.")
		}

		fetch, n, err = db.FetchObjectsFromCounter(objTypeA, 1, 1)
		if err != nil {
			t.Errorf("Error returned fetching objects of type %d", objTypeA)
		}
		if len(fetch) != 1 {
			t.Errorf("Incorrect slice size returned.")
		}
		if n != 1 {
			t.Errorf("Incorrect counter value returned.")
		}

		fetch, n, err = db.FetchObjectsFromCounter(objTypeA, 2, 3)
		if err != nil {
			t.Errorf("Error returned fetching objects of type %d", objTypeA)
		}
		if len(fetch) != 1 {
			t.Errorf("Incorrect slice size returned.")
		}
		if n != 2 {
			t.Errorf("Incorrect counter value returned.")
		}

		dataB1 := msgToBytes(testObj[offsetB][1])
		_, count, err = db.InsertObject(dataB1)

		// Test that objects can be removed after being added.
		err = db.RemoveObjectByCounter(objTypeA, 1)
		if err != nil {
			t.Errorf("Error returned removing object of type ", objTypeA)
		}

		err = db.RemoveObjectByCounter(objTypeB, 2)
		if err != nil {
			t.Errorf("Error returned removing object of type ", objTypeB)
		}

		err = db.RemoveObjectByCounter(objTypeA, 1)
		if err == nil {
			t.Errorf("Error expected for removing object that was already removed of type", objTypeA)
		}

		err = db.RemoveObjectByCounter(objTypeA, 3)
		if err == nil {
			t.Errorf("Error expected for removing object with too high a counter value.", objTypeA)
		}

		// Test that objects cannot be fetched after being removed.
		_, err = db.FetchObjectByCounter(objTypeA, 1)
		if err == nil {
			t.Errorf("No error returned for fetching a nonexistent object of type ", objTypeA)
		}

		// Test that the counter values returned by FetchObjectsFromCounter are correct
		// after some objects have been removed.
		fetch, n, err = db.FetchObjectsFromCounter(objTypeA, 1, 3)
		if err != nil {
			t.Errorf("Error returned fetching objects of type %d", objTypeA)
		}
		if len(fetch) != 1 {
			t.Errorf("Incorrect slice size returned.")
		}
		if n != 2 {
			t.Errorf("Incorrect counter value returned.")
		}

		fetch, n, err = db.FetchObjectsFromCounter(objTypeB, 1, 3)
		if err != nil {
			t.Errorf("Error returned fetching objects of type %d", objTypeA)
		}
		if len(fetch) != 1 {
			t.Errorf("Incorrect slice size returned: expected 1, got %d", len(fetch))
		}
		if n != 1 {
			t.Errorf("Incorrect counter value returned: expected 1, got %d", n)
		}
	}
}

// TestPubKey tests inserting public key messages, ExistsPubKey,
// FetchIdentityByAddress, and RemovePubKey
func TestPubKey(t *testing.T) {
	/*db := makeMemDB(t)

	// test FetchIdentityByAddress for an address that does not exist.
	_, err := db.FetchIdentityByAddress()
	if err == nil {
		t.Errorf("Error expected for fetching a pub key that does not exist. ")
	}*/

	// test ExistsPubKey for an address that does not exist.

	// test RemovePubKey for an address that does not exist.

	// test FetchIdentityByAddress for an address that exists in the database.

	// test ExistsPubKey for an address that does exist in the database.

	// test RemovePubKey for an address that exists in the database.

	// test FetchIdentityByAddress for an address that was removed.

	// test RemovePubKey for an address that was removed.

	// test ExistsPubKey for an address that does exist in the database.

}

/*func TestFetchRandomObject(t *testing.T) {
	db := makeMemDB(t)

}*/

func TestRemoveExpiredObjects(t *testing.T) {
	db := makeMemDB(t)

	for _, messages := range testObj {
		for _, message := range messages {
			_, _, _ = db.InsertObject(msgToBytes(message))
		}
	}

	db.RemoveExpiredObjects()

	for i, messages := range testObj {
		for j, message := range messages {
			hash, _ := wire.NewShaHash(bmutil.CalcInventoryHash(msgToBytes(message)))

			exists, _ := db.ExistsObject(hash)

			// The object should have been deleted
			if j == 1 && i != 1 {
				if exists {
					t.Errorf("Message %d %d should have been deleted, but was not.", i, j)
				}
			} else {
				if !exists {
					t.Errorf("Message %d %d should not have been deleted, but was.", i, j)
				}
			}
		}
	}
}
