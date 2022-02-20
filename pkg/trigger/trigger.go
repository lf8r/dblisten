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

type NotificationHandler func(*pq.Listener, <-chan struct{}, time.Duration) bool

type ProblemHandler pq.EventCallbackType

// DBChangeListener is a listener for data changes to tables registered with it.
type DBChangeListener struct {
	DBConnectStr          string
	ChangeHandler         NotificationHandler
	ProblemHandler        ProblemHandler
	MinConnectionInterval time.Duration
	MaxConnectionInterval time.Duration
	KeepaliveInterval     time.Duration
	listener              *pq.Listener
	db                    *sql.DB
}

// NewDBChangeListener creates a new DBListener.
func NewDBChangeListener(dbConnectStr string, changeHandler NotificationHandler, problemHandler ProblemHandler, minInterval, maxInterval, keepAliveInterval time.Duration) *DBChangeListener {
	dbListener := DBChangeListener{
		DBConnectStr:          dbConnectStr,
		ChangeHandler:         changeHandler,
		ProblemHandler:        problemHandler,
		MinConnectionInterval: minInterval,
		MaxConnectionInterval: maxInterval,
		KeepaliveInterval:     keepAliveInterval,
	}

	dbListener.listener = pq.NewListener(dbConnectStr, minInterval, maxInterval, pq.EventCallbackType(problemHandler))

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

// Init initializes the listener.
func (l *DBChangeListener) Init() error {
	db, err := sql.Open("postgres", l.DBConnectStr)
	if err != nil {
		return fmt.Errorf("sql open: %w", err)
	}

	l.db = db

	if err := l.db.Ping(); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}

	return nil
}

// InstallListener installs a listener function into the DB. Once installed, the
// listener must be enabled for individual tables to take effect.
func (l *DBChangeListener) InstallListener() error {
	db, err := sql.Open("postgres", l.DBConnectStr)
	if err != nil {
		return fmt.Errorf("install listener: %w", err)
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

// Start starts the listener thread.
func (l *DBChangeListener) Start(started, running *sync.Mutex, abort <-chan struct{}) error {
	running.Lock()
	started.Unlock()

	err := l.listener.Listen("events")
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	for {
		if l.ChangeHandler(l.listener, abort, l.KeepaliveInterval) {
			running.Unlock()

			return nil
		}
	}
}

// RegisterTable adds the table to the list of tables for which the listener is
// notified when changes occur in the table.
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
