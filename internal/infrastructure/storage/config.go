package storage

import (
	"net/url"
)

// Config mirrors iot-things storage configuration. It is owned by cmd:
// the only path to configure storage is storageConfigFromFlags in
// cmd/iot-things-v2 (same rule as THINGS-001 in iot-things).
type Config struct {
	host     string
	user     string
	password string
	port     string
	dbname   string
	sslmode  string
}

func NewConfig(host, user, password, port, dbname, sslmode string) Config {
	return Config{
		host:     host,
		user:     user,
		password: password,
		port:     port,
		dbname:   dbname,
		sslmode:  sslmode,
	}
}

func (c Config) ConnStr() string {
	// URL-koda användare/lösenord: specialtecken (@:/?# m.fl.) i lösenord
	// bryter annars anslutningssträngen (tidigare fmt.Sprintf utan escaping).
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.user, c.password),
		Host:   c.host + ":" + c.port,
		Path:   "/" + c.dbname,
	}
	q := u.Query()
	if c.sslmode != "" {
		q.Set("sslmode", c.sslmode)
		u.RawQuery = q.Encode()
	}
	return u.String()
}
