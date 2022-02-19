// Copyright (C) Subhajit DasGupta 2022

package trigger

import (
	"sync"
	"testing"
)

func TestSetup(t *testing.T) {
	// dbname=exampledb user=webapp password=webapp
	connectStr := "dbname=postgres user=postgres"
	abort := make(chan struct{}, 0)

	started := sync.Mutex{}
	started.Lock()

	running := sync.Mutex{}

	go StartListening(connectStr, &started, &running, abort)
	started.Lock()

	close(abort)
	running.Lock()
}
