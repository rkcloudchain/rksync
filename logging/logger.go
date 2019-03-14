/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package logging

import (
	"io"
	"io/ioutil"
	"log"
	"os"
)

// Logger does underlying logging work for rksync
type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warning(args ...interface{})
	Warningf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

// SetLogger sets logger that is used in rksync
func SetLogger(l Logger) {
	logger = l
}

const (
	debugLog int = iota
	infoLog
	warningLog
	errorLog
	fatalLog
)

var severityName = []string{
	debugLog:   "DEBUG",
	infoLog:    "INFO",
	warningLog: "WARNING",
	errorLog:   "ERROR",
	fatalLog:   "FATAL",
}

type loggerT struct {
	m []*log.Logger
}

// newLogger creates a Logger to be used as default logger.
func newLogger() Logger {
	errorW := ioutil.Discard
	warningW := ioutil.Discard
	infoW := ioutil.Discard
	debugW := ioutil.Discard

	logLevel := os.Getenv("RKSYNC_GO_LOG_LEVEL")
	switch logLevel {
	case "ERROR", "error":
		errorW = os.Stderr
	case "WARNING", "warning":
		warningW = os.Stderr
	case "", "INFO", "info":
		infoW = os.Stderr
	case "DEBUG", "debug":
		debugW = os.Stderr
	}

	var m []*log.Logger
	m = append(m, log.New(debugW, severityName[debugLog]+": ", log.LstdFlags))
	m = append(m, log.New(io.MultiWriter(debugW, infoW), severityName[infoLog]+": ", log.LstdFlags))
	m = append(m, log.New(io.MultiWriter(debugW, infoW, warningW), severityName[warningLog]+": ", log.LstdFlags))

	ew := io.MultiWriter(debugW, infoW, warningW, errorW)
	m = append(m, log.New(ew, severityName[errorLog]+": ", log.LstdFlags))
	m = append(m, log.New(ew, severityName[fatalLog]+": ", log.LstdFlags))
	return &loggerT{m: m}
}

func (g *loggerT) Debug(args ...interface{}) {
	g.m[debugLog].Println(args...)
}

func (g *loggerT) Debugf(format string, args ...interface{}) {
	g.m[debugLog].Printf(format, args...)
}

func (g *loggerT) Info(args ...interface{}) {
	g.m[infoLog].Println(args...)
}

func (g *loggerT) Infof(format string, args ...interface{}) {
	g.m[infoLog].Printf(format, args...)
}

func (g *loggerT) Warning(args ...interface{}) {
	g.m[warningLog].Println(args...)
}

func (g *loggerT) Warningf(format string, args ...interface{}) {
	g.m[warningLog].Printf(format, args...)
}

func (g *loggerT) Error(args ...interface{}) {
	g.m[errorLog].Println(args...)
}

func (g *loggerT) Errorf(format string, args ...interface{}) {
	g.m[errorLog].Printf(format, args...)
}

func (g *loggerT) Fatal(args ...interface{}) {
	g.m[fatalLog].Fatalln(args...)
}

func (g *loggerT) Fatalf(format string, args ...interface{}) {
	g.m[fatalLog].Fatalf(format, args...)
}
