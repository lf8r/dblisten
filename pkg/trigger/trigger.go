// Copyright (C) Subhajit DasGupta

package trigger

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
)

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

func waitForNotification(l *pq.Listener, abort <-chan struct{}, checkConnInterval time.Duration) bool {
	for {
		select {
		case n := <-l.Notify:
			fmt.Println("Received data from channel [", n.Channel, "] :")
			// Prepare notification payload for pretty print
			var prettyJSON bytes.Buffer
			err := json.Indent(&prettyJSON, []byte(n.Extra), "", "\t")
			if err != nil {
				fmt.Println("Error processing JSON: ", err)
				return false
			}
			fmt.Println(string(prettyJSON.Bytes()))

			return false

		case <-time.After(checkConnInterval):
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

// StartListening connects to the DB using the connectStr and installs a
// listener (trigger). It unlocks started when it completes starting, and
// running when it finishes running.
func StartListening(connectStr string, started, running *sync.Mutex, abort <-chan struct{}) error {
	db, err := sql.Open("postgres", connectStr)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	// Install the listener.
	if err := installListener(db); err != nil {
		return fmt.Errorf("install listener: %w", err)
	}

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	listener := pq.NewListener(connectStr, 10*time.Second, time.Minute, reportProblem)

	running.Lock()
	started.Unlock()

	for {
		if waitForNotification(listener, abort, 90*time.Second) {
			running.Unlock()
			return nil
		}
	}
}

func installListener(db *sql.DB) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	result, err := conn.ExecContext(context.Background(), listenFunc)
	if err != nil {
		return fmt.Errorf("install trigger: (%v) %w", result, err)
	}

	return nil
}
