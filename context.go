package main

import "time"

type Context struct {
	flagStarted, flagFinished bool
	startTime                 time.Time
	//debugCount int
}
