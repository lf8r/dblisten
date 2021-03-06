// Copyright (C) Subhajit DasGupta 2022

package trigger

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// problemHandler is called back when there is a problem receiving change
// notifications.
func problemHandler(ev pq.ListenerEventType, err error) {
	if err != nil {
		// nolint
		fmt.Println(err.Error())
	}
}

var once sync.Once = sync.Once{}

type receivedEvents struct {
	sync.Mutex
	events []DataChange
}

var events receivedEvents

func AppendEvent(event *DataChange) {
	once.Do(func() {
		events = receivedEvents{}
	})

	events.Lock()
	defer events.Unlock()

	events.events = append(events.events, *event)
}

func DrainEvents() []DataChange {
	once.Do(func() {
		events = receivedEvents{}
	})

	events.Lock()
	defer events.Unlock()

	ret := make([]DataChange, len(events.events))
	copy(ret, events.events)

	events.events = make([]DataChange, 0)

	return ret
}

// var events []string = make([]string, 0)

// changeHandler is called back on changes to the data in any of the registered
// tables.
func changeHandler(dataChange *DataChange) bool {
	AppendEvent(dataChange)

	return false
}

func TestDBListener(t *testing.T) {
	assert := require.New(t)

	// dbname=exampledb user=webapp password=webapp
	connectStr := "dbname=postgres user=postgres host=localhost"

	// Create a couple of test tables named "test1" and "test2", with the
	// objective of manipulating some test data.
	db, err := sql.Open("postgres", connectStr)
	assert.NoError(err)
	assert.NoError(db.Ping())
	createTestTables(assert, db)

	defer func() {
		removeTestTables(assert, db)
	}()

	// Create a new DBChangeListener
	dbListener, err := ListenAndNotify(connectStr, changeHandler, "test1", "test2")
	assert.NoError(err)

	// Create a row of data in test1
	createRow(assert, db, "test1", "subhajit", "dasgupta")
	// Create a row of data in test2
	createRow(assert, db, "test1", "chuck", "hudson")

	// Yield to allow the notification goroutine to make progress.
	time.Sleep(100 * time.Millisecond)

	// Check the recorded events.
	recordedEvents := DrainEvents()
	assert.Equal(2, len(recordedEvents))

	for _, event := range recordedEvents {
		// nolint
		fmt.Printf("%s\t%s\t%s\n", event.Table, event.Type, event.Data)
	}

	dbListener.Shutdown()
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
