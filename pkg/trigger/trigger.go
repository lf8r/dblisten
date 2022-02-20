// Copyright (C) Subhajit DasGupta

package trigger

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
)

type DataChangeHandler func(n *pq.Notification) bool

type ReconnectFailureHandler pq.EventCallbackType

// DBChangeListener is a listener for data changes to tables registered with it.
//  DBConnectStr is a connect string to the DB (e.g. "dbname=postgres user=postgres host=localhost")
//  Handler is a DataChange Handler, which is called back any time data changes are notified by the DB.
//  ReconnectStrategy is a user defined strategy for DB reconnects on extended periods of inactivity.
type DBChangeListener struct {
	sync.Mutex
	DBConnectStr         string
	Handler              DataChangeHandler
	ReconnectionStrategy DBReconnectStrategy

	// Internal state.
	listener *pq.Listener
	db       *sql.DB
	abort    chan struct{}
	running  *sync.Mutex
}

// defaultReconnectFailureHandler is the default failure handler when the
// listener fails to reconnect to the DB.
func defaultReconnectFailureHandler(ev pq.ListenerEventType, err error) {
	if err != nil {
		// nolint
		fmt.Println(err.Error())
	}
}

// DBReconnectStrategy describes a reconnection strategy for the DB
// connection used to listen for data changes.
type DBReconnectStrategy struct {
	FailureHandler ReconnectFailureHandler
	MinInterval    time.Duration
	MaxInterval    time.Duration
	KeepAlive      time.Duration
}

type Event struct {
	Table  string
	Action string
	Data   map[string]interface{}
}

// DefaultReconnectStrategy provides a default reconnection strategy as a convenience.
func DefaultReconnectStrategy() *DBReconnectStrategy {
	return &DBReconnectStrategy{
		FailureHandler: defaultReconnectFailureHandler,
		MinInterval:    10 * time.Second,
		MaxInterval:    time.Minute,
		KeepAlive:      90 * time.Second,
	}
}

// ListenAndNotify sets up a listener on the DB identified by dbConnectStr, and
// notifies changes to the tables to the dataChangeHandler. It uses the default
// reconnectStrategy to restablish connections with the DB if needed.
func ListenAndNotify(dbConnectStr string, notificationHandler DataChangeHandler, tables ...string) (*DBChangeListener, error) {
	return ListenAndNotifyWithReconnectStrategy(dbConnectStr, notificationHandler, DefaultReconnectStrategy(), tables...)
}

// ListenAndNotifyWithReconnectStrategy sets up a listener on the DB identified
// by dbConnectStr, and notifies changes to the tables to the dataChangeHandler.
// It uses the given reconnectStrategy to restablish connections with the DB if
// needed.
func ListenAndNotifyWithReconnectStrategy(dbConnectStr string, dataChangeHandler DataChangeHandler, reconnectStrategy *DBReconnectStrategy, tables ...string) (*DBChangeListener, error) {
	dbListener := NewDBChangeListener(dbConnectStr,
		dataChangeHandler,
		reconnectStrategy)

	// Init the listener.
	if err := dbListener.initListener(); err != nil {
		return nil, fmt.Errorf("init: %w", err)
	}

	// Start the listener, and wait for it to complete starting.
	started := sync.Mutex{}
	started.Lock()
	running := sync.Mutex{}

	abort := make(chan struct{}, 0)
	go dbListener.start(&started, &running, abort)
	started.Lock()

	for _, table := range tables {
		dbListener.RegisterTable(table)
	}

	dbListener.running = &running
	dbListener.abort = abort

	return dbListener, nil
}

// Shutdown shuts down the listener.
func (l *DBChangeListener) Shutdown() {
	close(l.abort)
	l.running.Lock()
}

// RegisterTable adds the table after the listener has started to the list of
// tables for which the listener is notified when changes occur in the table.
func (l *DBChangeListener) RegisterTable(table string) error {
	addTriggerStatement := fmt.Sprintf("CREATE TRIGGER %s_notify_event AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE notify_event();", table, table)

	conn, err := l.db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}

	defer conn.Close()

	if _, err = conn.ExecContext(context.Background(), addTriggerStatement); err != nil {
		return fmt.Errorf("create trigger (%s): %w", addTriggerStatement, err)
	}

	return nil
}

// NewDBChangeListener creates a new DBListener.
func NewDBChangeListener(dbConnectStr string, notificationHandler DataChangeHandler, reconnectionStrategy *DBReconnectStrategy) *DBChangeListener {
	dbListener := DBChangeListener{
		DBConnectStr:         dbConnectStr,
		Handler:              notificationHandler,
		ReconnectionStrategy: *reconnectionStrategy,
	}

	dbListener.listener = pq.NewListener(dbConnectStr,
		reconnectionStrategy.MinInterval,
		reconnectionStrategy.MaxInterval,
		pq.EventCallbackType(reconnectionStrategy.FailureHandler))

	return &dbListener
}

var listenFunc = `CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$

DECLARE 
	data json;
	notification json;

BEGIN

	-- Convert the old or new row to JSON, based on the kind of action.
	-- Action = DELETE?             -> OLD row
	-- Action = INSERT or UPDATE?   -> NEW row
	IF (TG_OP = 'DELETE') THEN
		data = row_to_json(OLD);
	ELSE
		data = row_to_json(NEW);
	END IF;
	
	-- Contruct the notification as a JSON string.
	notification = json_build_object(
					  'table',TG_TABLE_NAME,
					  'action', TG_OP,
					  'data', data);
	
					
	-- Execute pg_notify(channel, notification)
	PERFORM pg_notify('events',notification::text);
	
	-- Result is ignored since this is an AFTER trigger
	RETURN NULL; 
END;

$$ LANGUAGE plpgsql;`

// initListener initializes the listener.
func (l *DBChangeListener) initListener() error {
	db, err := sql.Open("postgres", l.DBConnectStr)
	if err != nil {
		return fmt.Errorf("sql open: %w", err)
	}

	l.db = db

	if err := l.db.Ping(); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	defer conn.Close()

	result, err := conn.ExecContext(context.Background(), listenFunc)
	if err != nil {
		return fmt.Errorf("install trigger: (%v) %w", result, err)
	}

	return nil
}

// start starts the listener thread.
func (l *DBChangeListener) start(started, running *sync.Mutex, abort <-chan struct{}) error {
	running.Lock()
	started.Unlock()

	err := l.listener.Listen("events")
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	for {
		if l.defaultNotificationHandler(l.listener, abort, l.ReconnectionStrategy.KeepAlive) {
			running.Unlock()

			return nil
		}
	}
}

// defaultNotificationHandler handles notifications arriving from the database.
// It returns until the abort channel becomes readable. It checks the connection
// if no notifications are received for checkConnInterval.
func (l *DBChangeListener) defaultNotificationHandler(pl *pq.Listener, abort <-chan struct{}, checkConnInterval time.Duration) bool {
	for {
		select {
		case n := <-pl.Notify:
			l.Lock()
			defer l.Unlock()

			if l.Handler(n) {
				return true
			}

			return false

		case <-time.After(checkConnInterval):
			// nolint
			fmt.Println("Received no events for 90 seconds, checking connection")

			go func() {
				pl.Ping()
			}()

			return false

		case <-abort:
			return true
		}
	}
}
