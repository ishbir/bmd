/*
Package database provides a database interface for handling Bitmessage objects.

Basic Design

The basic design of this package is to provide two classes of items in a
database; blocks and transactions (tx) where the block number increases
monotonically.  Each transaction belongs to a single block although a block can
have a variable number of transactions.  Along with these two items, several
convenience functions for dealing with the database are provided as well as
functions to query specific items that may be present in a block or tx.

Usage

At the highest level, the use of this packages just requires that you import it,
setup a database, insert some data into it, and optionally, query the data back.
*/
package database
