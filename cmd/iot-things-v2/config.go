package main

import (
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

type flagType int
type flagMap map[flagType]string

const (
	listenAddress flagType = iota
	servicePort
	controlPort
	enableTracing

	dbHost
	dbUser
	dbPassword
	dbPort
	dbName
	dbSSLMode

	policiesFile
	authzAccessObject
	thingsFile
	configFile
	migrateBindings

	logLevel
)

type appConfig struct{}

var oninit = servicerunner.OnInit[appConfig]
var onstarting = servicerunner.OnStarting[appConfig]
var onshutdown = servicerunner.OnShutdown[appConfig]
var webserver = servicerunner.WithHTTPServeMux[appConfig]
var muxinit = servicerunner.OnMuxInit[appConfig]
var listen = servicerunner.WithListenAddr[appConfig]
var port = servicerunner.WithPort[appConfig]
var pprof = servicerunner.WithPPROF[appConfig]
var liveness = servicerunner.WithK8SLivenessProbe[appConfig]
var readiness = servicerunner.WithK8SReadinessProbes[appConfig]
var tracing = servicerunner.WithTracing[appConfig]
