package database_test

import (
	"fmt"

	"github.com/monetas/bmd/database"
	_ "github.com/monetas/bmd/database/memdb"
)

// This example demonstrates creating a new database and inserting a few objects
// into it.
func ExampleCreateDB() {
	// Notice in these example imports that the memdb driver is loaded.
	// Ordinarily this would be whatever driver(s) your application
	// requires.
	// import (
	//	"github.com/monetas/bmd/database"
	// 	_ "github.com/monetas/bmd/database/memdb"
	// )

	// Create a database and schedule it to be closed on exit.  This example
	// uses a memory-only database to avoid needing to write anything to
	// the disk.  Typically, you would specify a persistent database driver
	// such as "leveldb" and give it a database name as the second
	// parameter.
	db, err := database.CreateDB("memdb")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	// TODO
}
