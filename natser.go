/*===----------- natser.go - nats debug utility written in go  -------------===
 * Part of the tracker project @ https://github.com/dioptre/tracker
 *
 *
 * This file is licensed under the Apache 2 License. See LICENSE for details.
 *
 *  Copyright (c) 2018 Andrew Grosser. All Rights Reserved.
 *
 *                                     `...
 *                                    yNMMh`
 *                                    dMMMh`
 *                                    dMMMh`
 *                                    dMMMh`
 *                                    dMMMd`
 *                                    dMMMm.
 *                                    dMMMm.
 *                                    dMMMm.               /hdy.
 *                  ohs+`             yMMMd.               yMMM-
 *                 .mMMm.             yMMMm.               oMMM/
 *                 :MMMd`             sMMMN.               oMMMo
 *                 +MMMd`             oMMMN.               oMMMy
 *                 sMMMd`             /MMMN.               oMMMh
 *                 sMMMd`             /MMMN-               oMMMd
 *                 oMMMd`             :NMMM-               oMMMd
 *                 /MMMd`             -NMMM-               oMMMm
 *                 :MMMd`             .mMMM-               oMMMm`
 *                 -NMMm.             `mMMM:               oMMMm`
 *                 .mMMm.              dMMM/               +MMMm`
 *                 `hMMm.              hMMM/               /MMMm`
 *                  yMMm.              yMMM/               /MMMm`
 *                  oMMm.              oMMMo               -MMMN.
 *                  +MMm.              +MMMo               .MMMN-
 *                  +MMm.              /MMMo               .NMMN-
 *           `      +MMm.              -MMMs               .mMMN:  `.-.
 *          /hys:`  +MMN-              -NMMy               `hMMN: .yNNy
 *          :NMMMy` sMMM/              .NMMy                yMMM+-dMMMo
 *           +NMMMh-hMMMo              .mMMy                +MMMmNMMMh`
 *            /dMMMNNMMMs              .dMMd                -MMMMMNm+`
 *             .+mMMMMMN:              .mMMd                `NMNmh/`
 *               `/yhhy:               `dMMd                 /+:`
 *                                     `hMMm`
 *                                     `hMMm.
 *                                     .mMMm:
 *                                     :MMMd-
 *                                     -NMMh.
 *                                      ./:.
 *
 *===----------------------------------------------------------------------===
 */
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/gocql/gocql"
	"github.com/nats-io/go-nats"
)

////////////////////////////////////////
// Get the system setup from the config.json file:
////////////////////////////////////////
type session interface {
	connect() error
	close() error
	write(w *WriteArgs) error
	listen() error
}

type KeyValue struct {
	Key   string
	Value interface{}
}

type Field struct {
	Type    string
	Id      string
	Default string
}

type Query struct {
	Statement string
	QueryType string
	Fields    []Field
}

type Filter struct {
	Type    string
	Alias   string
	Id      string
	Queries []Query
}

type WriteArgs struct {
	WriteType int
	Values    *map[string]interface{}
	IsServer  bool
	IP        string
	Browser   string
	Language  string
	URI       string
	EventID   gocql.UUID
}

type NatsService struct { //Implements 'session'
	nc            *nats.Conn
	ec            *nats.EncodedConn
	Configuration *Configuration
}

type Configuration struct {
	Debug        bool
	Service      string
	Hosts        []string
	Secure       bool
	Critical     bool
	CACert       string
	Cert         string
	Key          string
	Format       string
	QueueGroup   string
	MessageLimit int
	BytesLimit   int
	Filter       []Filter
	ForwardTopic string
	Session      session
}

////////////////////////////////////////
// Start here
////////////////////////////////////////
func main() {
	fmt.Println("\n\n//////////////////////////////////////////////////////////////")
	fmt.Println("Natser.")
	fmt.Println("Software to debug nats")
	fmt.Println("https://github.com/dioptre/natser")
	fmt.Println("(c) Copyright 2018 SF Product Labs LLC.")
	fmt.Println("Use of this software is subject to the LICENSE agreement.")
	fmt.Println("//////////////////////////////////////////////////////////////\n\n")

	//////////////////////////////////////// LOAD CONFIG
	fmt.Println("Starting services...")
	configFile := "config.json"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}
	fmt.Println("Configuration file: ", configFile)
	file, _ := os.Open(configFile)
	defer file.Close()
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err := decoder.Decode(&configuration)
	if err != nil {
		fmt.Println("error:", err)
	}

	fmt.Printf("Connecting to NATS Cluster: %s\n", &configuration.Hosts)
	gonats := NatsService{
		Configuration: &configuration,
	}
	err = gonats.connect()
	if err != nil {
		panic(fmt.Sprintf("[CRITICAL] Could not connect to NATS Cluster. %s\n", err))
	} else {
		fmt.Printf("NATS Consumer Connected.\n")
	}
	configuration.Session.listen()

	runtime.Goexit()

}

//////////////////////////////////////// NATS
// Connect initiates the primary connection to the range of provided URLs
func (i *NatsService) connect() error {
	err := fmt.Errorf("Could not connect to NATS")

	certFile := i.Configuration.Cert
	keyFile := i.Configuration.Key
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("[ERROR] Parsing X509 certificate/key pair: %v", err)
	}

	rootPEM, err := ioutil.ReadFile(i.Configuration.CACert)

	pool := x509.NewCertPool()
	ok := pool.AppendCertsFromPEM([]byte(rootPEM))
	if !ok {
		log.Fatalln("[ERROR] Failed to parse root certificate.")
	}

	config := &tls.Config{
		//ServerName:         i.Configuration.Hosts[0],
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: i.Configuration.Secure, //TODO: SECURITY THREAT
	}

	if i.nc, err = nats.Connect(strings.Join(i.Configuration.Hosts[:], ","), nats.Secure(config)); err != nil {
		fmt.Println("[ERROR] Connecting to NATS:", err)
		return err
	}
	if i.ec, err = nats.NewEncodedConn(i.nc, nats.JSON_ENCODER); err != nil {
		fmt.Println("[ERROR] Encoding NATS:", err)
		return err
	}
	i.Configuration.Session = i
	return nil
}

//////////////////////////////////////// NATS
// Close
//will terminate the session to the backend, returning error if an issue arises
func (i *NatsService) close() error {
	i.ec.Drain()
	i.ec.Close()
	i.nc.Drain()
	i.nc.Close()
	return nil
}

//////////////////////////////////////// NATS
// Write
func (i *NatsService) write(w *WriteArgs) error {
	// sendCh := make(chan *map[string]interface{})
	// i.ec.Publish(i.Configuration.Context, w.Values)
	// i.ec.BindSendChan(i.Configuration.Context, sendCh)
	// sendCh <- w.Values
	return i.ec.Publish(i.Configuration.ForwardTopic, w.Values)
}

//////////////////////////////////////// NATS
// Listen
func (i *NatsService) listen() error {
	for idx := range i.Configuration.Filter {
		f := &i.Configuration.Filter[idx]
		if i.Configuration.QueueGroup != "" {
			i.nc.QueueSubscribe(f.Id, i.Configuration.QueueGroup, func(m *nats.Msg) {
				fmt.Printf("Subject: %s Queue:%s Reply:%s Message:%s\n", m.Subject, m.Sub.Queue, m.Reply, m.Data)
				if i.Configuration.ForwardTopic != "" {
					i.nc.Publish(i.Configuration.ForwardTopic, m.Data)
				}
			})
		} else {
			i.nc.Subscribe(f.Id, func(m *nats.Msg) {
				fmt.Printf("Subject: %s Reply:%s Message:%s\n", m.Subject, m.Reply, m.Data)
				if i.Configuration.ForwardTopic != "" {
					i.nc.Publish(i.Configuration.ForwardTopic, m.Data)
				}
			})
		}
	}
	return nil
}
