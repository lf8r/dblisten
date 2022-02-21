// Copyright (C) Subhajit DasGupta 2022

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/lf8r/dblisten/pkg/trigger"
)

const (
	sleepInterval = 10 * time.Second
)

func changeHandler(dataChange *trigger.DataChange) bool {
	// nolint
	fmt.Printf("%s\t%s\t%s\n", dataChange.Table, dataChange.Type, dataChange.Data)

	return false
}

// main demonstrates the use of a simple daemon program to listen in to data
// changes on a given database.
func main() {
	connect := ""
	flag.StringVar(&connect, "connect", "", "DB connection string, e.g. \"dbname=postgres user=postgres host=localhost\"")

	tablesStr := ""
	flag.StringVar(&tablesStr, "tables", "", "comma-separated list of databases to listen for data changes")

	flag.Parse()

	if connect == "" {
		// nolint
		fmt.Printf("connect parameter not given\n")

		os.Exit(1)
	}

	if tablesStr == "" {
		// nolint
		fmt.Printf("tables parameter not given\n")

		os.Exit(1)
	}

	tables := strings.Split(tablesStr, ",")

	dbListener, err := trigger.ListenAndNotify(connect, changeHandler, tables...)
	if err != nil {
		// nolint
		fmt.Printf("listen and notify failed: %v\n", err)

		os.Exit(1)
	}

	ctrlC := make(chan os.Signal, 1)
	signal.Notify(ctrlC, os.Interrupt, os.Kill)

	abort := make(chan struct{}, 0)

	go func() {
		for sig := range ctrlC {
			switch sig {
			case os.Interrupt, os.Kill:
				dbListener.Shutdown()
				close(abort)

				return
			}
		}
	}()

	for {
		select {
		case <-abort:
			return
		}
	}
}
