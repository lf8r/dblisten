// Copyright (C) Subhajit DasGupta 2022

package trigger

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func problemHandler(ev pq.ListenerEventType, err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}

var events []string = make([]string, 0)

func notificationHandler(l *pq.Listener, abort <-chan struct{}, checkConnInterval time.Duration) bool {
	for {
		select {
		case n := <-l.Notify:
			// nolint
			fmt.Println("Received data from channel [", n.Channel, "] :")

			events = append(events, string(n.Extra))

			// Prepare notification payload for pretty print
			var prettyJSON bytes.Buffer

			err := json.Indent(&prettyJSON, []byte(n.Extra), "", "\t")
			if err != nil {
				// nolint
				fmt.Println("Error processing JSON: ", err)

				return false
			}

			// nolint
			fmt.Println(string(prettyJSON.Bytes()))

			return false

		case <-time.After(checkConnInterval):
			// nolint
			fmt.Println("Received no events for 90 seconds, checking connection")

			go func() {
				l.Ping()
			}()

			return false

		case <-abort:
			return true
		}
	}
}

// TestDBListener creates a new DBListener, creates two temporary data tables,
// adds one row each to each table, and verifies that the listener is notified
// for both records.
func TestDBListener(t *testing.T) {
	assert := require.New(t)

	// dbname=exampledb user=webapp password=webapp
	connectStr := "dbname=postgres user=postgres host=localhost"
	abort := make(chan struct{}, 0)

	// Create a new DBChangeListener
	dbListener := NewDBChangeListener(connectStr, notificationHandler, problemHandler, 10*time.Second, time.Minute, 90*time.Second)
	assert.NoError(dbListener.Init())

	// Install the listening function in the DB.
	assert.NoError(dbListener.InstallListener())

	// Start the listener.
	started := sync.Mutex{}
	started.Lock()
	running := sync.Mutex{}

	go dbListener.Start(&started, &running, abort)
	started.Lock()

	// Create a couple of test tables named "test1" and "test2", with the
	// objective of manipulating some test data.
	db, err := sql.Open("postgres", dbListener.DBConnectStr)
	assert.NoError(err)
	assert.NoError(db.Ping())
	createTestTables(assert, db)

	defer func() {
		removeTestTables(assert, db)
	}()

	// Register these tables with DBListener.
	assert.NoError(dbListener.RegisterTable("test1"))
	assert.NoError(dbListener.RegisterTable("test2"))

	// Create a row of data in test1
	createRow(assert, db, "test1", "subhajit", "dasgupta")
	// Create a row of data in test2
	createRow(assert, db, "test1", "chuck", "hudson")

	// Sleep for a bit to allow the notification goroutine to make progress.
	time.Sleep(100 * time.Millisecond)
	close(abort)
	running.Lock()

	for _, event := range events {
		// nolint
		fmt.Println(event)
	}

	assert.Equal(2, len(events))
}

var createTable1 = `CREATE TABLE if not exists test1(
	id varchar,
	key varchar,
	value varchar,
	PRIMARY KEY(id)
	);`

var createTable2 = `CREATE TABLE if not exists test2(
	id varchar,
	key varchar,
	value varchar,
	PRIMARY KEY(id)
	);`

func createTestTables(assert *require.Assertions, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	assert.NoError(err)

	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), createTable1)
	assert.NoError(err)

	_, err = conn.ExecContext(context.Background(), createTable2)
	assert.NoError(err)
}

func removeTestTables(assert *require.Assertions, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	assert.NoError(err)

	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), "drop table test1 cascade;")
	assert.NoError(err)

	_, err = conn.ExecContext(context.Background(), "drop table test2 cascade;")
	assert.NoError(err)
}

func createRow(assert *require.Assertions, db *sql.DB, table, key, value string) {
	conn, err := db.Conn(context.Background())
	assert.NoError(err)

	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), fmt.Sprintf("insert into %s values('%s', '%s','%s');", table, uuid.New(), key, value))
	assert.NoError(err)
}
