package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/infrastructure/storage"
	"github.com/diwise/iot-things-v2/internal/presentation/api"
	k8shandlers "github.com/diwise/service-chassis/pkg/infrastructure/net/http/handlers"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

const serviceName string = "iot-things-v2"

func defaultFlags() flagMap {
	return flagMap{
		listenAddress: "0.0.0.0",
		servicePort:   "8080",
		controlPort:   "8000",
		enableTracing: "true",

		dbHost:     "",
		dbUser:     "",
		dbPassword: "",
		dbPort:     "5432",
		dbName:     "diwise",
		dbSSLMode:  "disable",

		policiesFile:      "/opt/diwise/config/authz.rego",
		authzAccessObject: "false",
		thingsFile:        "/opt/diwise/config/things.csv",
		configFile:        "/opt/diwise/config/config.yaml",
		migrateBindings:   "true",

		logLevel: "debug",
	}
}

func main() {
	ctx, flags := parseExternalConfig(context.Background(), defaultFlags())

	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(ctx, serviceName, serviceVersion, "json")
	defer cleanup()

	logging.SetLogLevel(parseLogLevel(flags[logLevel]))

	policies, err := os.Open(flags[policiesFile])
	exitIf(err, logger, "unable to open opa policy file")

	things, err := os.Open(flags[thingsFile])
	exitIf(err, logger, "unable to open things file")

	config, err := os.Open(flags[configFile])
	exitIf(err, logger, "unable to open config file")

	cfg := &appConfig{}

	runner, err := initialize(ctx, flags, cfg, policies, things, config)
	exitIf(err, logger, "failed to initialize service runner")

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags flagMap, cfg *appConfig, policiesFile, thingsFile, configFile io.ReadCloser) (servicerunner.Runner[appConfig], error) {
	log := logging.GetFromContext(ctx)

	probes := readinessProbes()

	var msgCtx messaging.MsgContext
	var app application.ThingsApp
	var mgr *application.Manager

	// Backend-val i oninit: tom POSTGRES_HOST ger minnesbackend (dev/test),
	// satt värd ger PostgreSQL (drill/prod). Samma RuntimeStore-gräns i
	// båda fallen; stängning via ownedResources.
	var backend interface{ Close() }
	var runtimeStore application.RuntimeStore
	var reader application.ThingsReader
	var writer application.ThingsWriter

	owned := &ownedResources{}

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		),
		webserver("public", listen(flags[listenAddress]), port(flags[servicePort]), tracing(tracingEnabled(flags)),
			muxinit(func(ctx context.Context, identifier string, port string, appCfg *appConfig, handler *http.ServeMux) error {
				defer policiesFile.Close()
				log.Debug("register api handlers...")
				return api.RegisterHandlers(ctx, handler, mgr, policiesFile, api.WithAccessObjectAuthorization(accessObjectEnabled(flags)))
			}),
		),
		oninit(func(ctx context.Context, ac *appConfig) error {
			log.Debug("initializing servicerunner")

			defer configFile.Close()
			defer thingsFile.Close()

			var err error

			if flags[dbHost] == "" {
				log.Info("using memory storage backend (POSTGRES_HOST empty)")
				var mem *storage.Storage
				mem, err = storage.New(ctx, storageConfigFromFlags(flags))
				if err != nil {
					return fmt.Errorf("could not configure storage: %w", err)
				}
				backend = mem
				runtimeStore = storage.NewRuntimeAdapter(mem)
				reader, writer = mem, mem
			} else {
				log.Info("using PostgreSQL storage backend", "host", flags[dbHost])
				var pg *storage.PgStore
				pg, err = storage.NewPostgres(ctx, storageConfigFromFlags(flags))
				if err != nil {
					return fmt.Errorf("could not configure storage: %w", err)
				}
				backend = pg
				runtimeStore = pg
				reader, writer = pg, pg
			}

			messengerConfig, err := messaging.LoadConfiguration(ctx, serviceName, log)
			if err != nil {
				// Backend redan skapad ovan: stäng för att undvika läcka
				// (tidigare returnerade denna väg utan Close).
				if backend != nil {
					backend.Close()
					backend = nil
				}
				owned.storage = nil
				return fmt.Errorf("messaging configuration error: %w", err)
			}

			msgCtx, err = messaging.Initialize(ctx, messengerConfig)
			if err != nil {
				if backend != nil {
					backend.Close()
					backend = nil
				}
				owned.storage = nil
				return fmt.Errorf("failed to init messenger: %w", err)
			}

			owned.messenger = msgCtx
			owned.storage = backend

			log.Debug("creating application...")
			app, mgr, err = newApp(ctx, reader, writer, msgCtx, runtimeStore, configFile)
			if err != nil {
				if backend != nil {
					backend.Close()
					backend = nil
				}
				owned.storage = nil
				return fmt.Errorf("unable to initialize app: %w", err)
			}

			log.Debug("seeding things...")
			err = seed(ctx, thingsFile, app)
			if err != nil {
				if backend != nil {
					backend.Close()
					backend = nil
				}
				owned.storage = nil
				return fmt.Errorf("unable to seed things: %w", err)
			}

			if migrateBindingsEnabled(flags) {
				n, err := app.MigrateBindings(ctx)
				if err != nil {
					if backend != nil {
						backend.Close()
						backend = nil
					}
					owned.storage = nil
					return fmt.Errorf("bindings migration failed: %w", err)
				}
				if n > 0 {
					log.Info("migrated things to bindings", "count", n)
				}
			} else if pending, err := app.HasUnmigratedThings(ctx); err != nil {
				if backend != nil {
					backend.Close()
					backend = nil
				}
				owned.storage = nil
				return fmt.Errorf("could not check for unmigrated things: %w", err)
			} else if pending {
				if backend != nil {
					backend.Close()
					backend = nil
				}
				owned.storage = nil
				return fmt.Errorf("things without bindings remain and THINGS_MIGRATE_BINDINGS is disabled")
			}

			return nil
		}),
		onstarting(func(ctx context.Context, appCfg *appConfig) (err error) {
			log.Debug("starting servicerunner")

			// OnStarting failures bypass OnShutdown in the runner, so
			// clean up acquired resources on every error path below.
			defer func() {
				if err != nil {
					owned.close(ctx)
				}
			}()

			log.Debug("starting messaging...")
			err = msgCtx.Start(ctx)
			if err != nil {
				return fmt.Errorf("failed to start messenger: %w", err)
			}

			log.Debug("register topic handler...")
			err = msgCtx.RegisterTopicMessageHandler("message.accepted", application.NewMeasurementsHandler(ctx, app))
			if err != nil {
				return fmt.Errorf("unable to register message handler: %w", err)
			}

			log.Debug("draining pending outbox batches...")
			n, err := app.DrainPending(ctx)
			if err != nil {
				return fmt.Errorf("unable to drain outbox: %w", err)
			}
			if n > 0 {
				log.Info("republished pending outbox batches", "count", n)
			}

			return nil
		}),
		onshutdown(func(ctx context.Context, appCfg *appConfig) error {
			log.Debug("shutdown servicerunner")

			owned.close(ctx)

			return nil
		}),
	)

	return runner, nil
}

// tracingEnabled is the minimal production seam for the tracing toggle.
// Only the exact string "true" enables tracing; ParseBool spellings
// such as "TRUE" or "1" intentionally do not.
func tracingEnabled(flags flagMap) bool {
	return flags[enableTracing] == "true"
}

// accessObjectEnabled is the minimal production seam for the
// access-object toggle, using strconv semantics: invalid values
// silently keep the legacy tenants model.
func accessObjectEnabled(flags flagMap) bool {
	v, _ := strconv.ParseBool(flags[authzAccessObject])
	return v
}

// migrateBindingsEnabled styr uppstartsmigreringen till bindningar. Endast
// den exakta strängen "true" aktiverar den.
func migrateBindingsEnabled(flags flagMap) bool {
	return flags[migrateBindings] == "true"
}

// readinessProbes returns the named readiness stubs. Per harmonization
// standard they always report OK and never call any dependency.
func readinessProbes() map[string]k8shandlers.ServiceProber {
	return map[string]k8shandlers.ServiceProber{
		"rabbitmq":  func(context.Context) (string, error) { return "ok", nil },
		"timescale": func(context.Context) (string, error) { return "ok", nil },
	}
}

// shutdownTimeout är en egen övre gräns för att stoppa inflöde och dränera
// pågående arbete. Runnerns shutdown-hook får ingen egen timeout, så tjänsten
// sätter sin egen så att en blockerad leverans inte håller nedstängningen
// obegränsat.
const shutdownTimeout = 10 * time.Second

// ownedResources tracks the resources created during OnInit so shutdown
// is nil-safe, ordered and idempotent via the sync.Once guard, so the
// messenger is shut down at most once.
//
// Shutdown order: stop inflow (messenger) and let it drain in-flight
// deliveries within budget, then close storage. The messaging library
// acknowledges only after the handler returns and drains on Shutdown, so no
// separate handler tracker is needed. HTTP servers stay live until after
// OnShutdown returns (runner behavior).
type ownedResources struct {
	once      sync.Once
	messenger messaging.MsgContext
	storage   interface{ Close() }
}

func (o *ownedResources) close(ctx context.Context) {
	o.once.Do(func() {
		if o.messenger != nil {
			// Egen budget, oberoende av en redan avbruten stoppsignal.
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
			defer cancel()

			if err := o.messenger.Shutdown(stopCtx); err != nil {
				logging.GetFromContext(ctx).Debug("failed to shut down messenger", "err", err.Error())
			}
		}
		if o.storage != nil {
			o.storage.Close()
		}
	})
}

// storageConfigFromFlags builds the storage configuration explicitly
// from cmd-owned flags. This is the only path used to configure
// storage; storage.LoadConfiguration was removed as dead code
// (THINGS-001).
func storageConfigFromFlags(flags flagMap) storage.Config {
	return storage.NewConfig(flags[dbHost], flags[dbUser], flags[dbPassword], flags[dbPort], flags[dbName], flags[dbSSLMode])
}

func parseExternalConfig(ctx context.Context, flags flagMap) (context.Context, flagMap) {
	// Allow environment variables to override certain defaults
	envOrDef := env.GetVariableOrDefault

	flags[listenAddress] = envOrDef(ctx, "LISTEN_ADDRESS", flags[listenAddress])
	flags[controlPort] = envOrDef(ctx, "CONTROL_PORT", flags[controlPort])
	flags[servicePort] = envOrDef(ctx, "SERVICE_PORT", flags[servicePort])

	flags[policiesFile] = envOrDef(ctx, "POLICIES_FILE", flags[policiesFile])
	flags[authzAccessObject] = envOrDef(ctx, "AUTHZ_ACCESS_OBJECT_ENABLED", flags[authzAccessObject])
	flags[thingsFile] = envOrDef(ctx, "THINGS_FILE", flags[thingsFile])
	flags[configFile] = envOrDef(ctx, "CONFIG_FILE", flags[configFile])
	flags[migrateBindings] = envOrDef(ctx, "THINGS_MIGRATE_BINDINGS", flags[migrateBindings])

	flags[dbHost] = envOrDef(ctx, "POSTGRES_HOST", flags[dbHost])
	flags[dbPort] = envOrDef(ctx, "POSTGRES_PORT", flags[dbPort])
	flags[dbName] = envOrDef(ctx, "POSTGRES_DBNAME", flags[dbName])
	flags[dbUser] = envOrDef(ctx, "POSTGRES_USER", flags[dbUser])
	flags[dbPassword] = envOrDef(ctx, "POSTGRES_PASSWORD", flags[dbPassword])
	flags[dbSSLMode] = envOrDef(ctx, "POSTGRES_SSLMODE", flags[dbSSLMode])

	flags[logLevel] = envOrDef(ctx, "LOG_LEVEL", flags[logLevel])

	apply := func(f flagType) func(string) error {
		return func(value string) error {
			flags[f] = value
			return nil
		}
	}

	// Allow command line arguments to override defaults and environment variables
	flag.Func("policies", "an authorization policy file", apply(policiesFile))
	flag.Func("authz-access-object", "enable access-object authorization policy result model", apply(authzAccessObject))
	flag.Func("things", "list of known things", apply(thingsFile))
	flag.Func("config", "a yaml file with configuration", apply(configFile))
	flag.Func("loglevel", "set the log level", apply(logLevel))
	flag.Parse()

	return ctx, flags
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelDebug
	}
}

func newApp(ctx context.Context, r application.ThingsReader, w application.ThingsWriter, m messaging.MsgContext, store application.RuntimeStore, cfg io.Reader) (application.ThingsApp, *application.Manager, error) {
	// Runtime över storet (minne eller PostgreSQL, valt av anroparen):
	// atomära batchar, dedup, outbox. Samma store delas vid omstart
	// (dränering utan omberäkning). Manager: katalog + objektlivscykel.
	rt := application.NewRuntime(store)
	pub := &application.TopicPublisher{MsgCtx: m}
	cat := catalog.New()
	var opts []application.ManagerOption
	if cs, ok := store.(catalog.Store); ok {
		// Beständig katalog: återställ publicerade versioner så att
		// saker överlever omstart med sina mallreferenser intakta.
		cat.Restore(cs.LoadTemplates(), cs.LoadVariants())
		opts = append(opts, application.WithCatalogStore(cs))
	}
	mgr := application.NewManager(cat, rt, store, opts...)
	a := application.New(r, w, m, rt, pub)
	err := a.LoadConfig(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to load config: %w", err)
	}

	return a, mgr, nil
}

func seed(ctx context.Context, fp io.Reader, a application.ThingsApp) error {
	return a.Seed(ctx, fp)
}

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	l := logger
	if l == nil {
		l = slog.Default()
	}
	if err != nil {
		l.With(args...).Error(msg, "err", err.Error())
		time.Sleep(2 * time.Second)
		os.Exit(1)
	}
}
