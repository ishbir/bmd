// Copyright (c) 2015 Monetas.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"crypto/tls"
	"encoding/base64"
	"log"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cenkalti/rpc2"
	"github.com/cenkalti/rpc2/jsonrpc"
	"github.com/gorilla/websocket"
	"github.com/monetas/bmd/peer"
	"github.com/monetas/bmutil/wire"
)

const (
	rpcLoc  = "wss://localhost:8442/"
	rpcUser = "admin"
	rpcPass = "admin"
)

func runner(client *rpc2.Client, t *testing.T) {
	err := client.Call("SendObject", "Y=", nil)
	if !strings.Contains(err.Error(), "access") { // auth failure
		t.Errorf("for SendObject expected auth failure got %v", err)
	}

	var success bool
	authArgs := &RPCAuthParams{
		Username: rpcUser,
		Password: rpcPass,
	}
	err = client.Call("Authenticate", authArgs, &success)
	if err != nil {
		t.Fatal("Authentication error: ", err)
	}
	if !success {
		t.Fatal("Authentication didn't succeed.")
	}

	// random string
	err = client.Call("SendObject", "YWxrZmpsc2FranNsZGthamtzbGZqbGFsa2Zqa2xkYWZsZGpsZmpkc2xmbGRzZmtsZHNrZmxkc2pmb3Bpc2RvaWZqZHNvaWZqb2RzaWZqZG9zamZvZHNmam9pZGpmZGZrZHNqZmo=", nil)
	if err == nil {
		t.Error("Invalid object accepted by SendObject")
	}

	subscribeArgs := &RPCSubscribeArgs{
		FromCounter: 0,
	}

	client.Handle("ReceivePubkey", func(client *rpc2.Client, args *RPCReceiveArgs,
		_ *struct{}) error {
		b, _ := base64.StdEncoding.DecodeString(args.Object)
		t.Logf("Received pubkey: counter=%d, byte length=%d\n", args.Counter,
			len(b))
		return nil
	})

	err = client.Call("SubscribePubkeys", subscribeArgs, nil)
	if err != nil {
		t.Error("SubscribePubkeys failed: ", err)
	}
}

func TestRPCConnection(t *testing.T) {
	// Address for mock listener to pass to server. The server
	// needs at least one listener or it won't start so we mock it.
	remoteAddr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8442}

	// Generate config.
	cfg = &config{
		MaxPeers:      0,
		RPCPass:       "admin",
		RPCUser:       "admin",
		DisableRPC:    false,
		RPCKey:        filepath.Join(bmdHomeDir, "rpc.key"),
		RPCCert:       filepath.Join(bmdHomeDir, "rpc.cert"),
		LogDir:        filepath.Join(bmdHomeDir, defaultLogDirname),
		RPCMaxClients: 1,
	}

	// Load rpc listeners.
	addrs, err := net.LookupHost("localhost")
	if err != nil {
		t.Fatalf("Could not look up localhost.")
	}
	cfg.RPCListeners = make([]string, 0, len(addrs))
	for _, addr := range addrs {
		addr = net.JoinHostPort(addr, defaultRPCPort)
		cfg.RPCListeners = append(cfg.RPCListeners, addr)
	}
	setLogLevels("trace")
	defer backendLog.Flush()

	// Create a server.
	listeners := []string{net.JoinHostPort("", "8445")}
	serv, err := newServer(listeners, getMemDb([]wire.Message{}),
		MockListen([]*MockListener{
			NewMockListener(remoteAddr, make(chan peer.Connection), make(chan struct{}, 1))}))

	if err != nil {
		t.Fatalf("Server creation failed: %s", err)
	}

	serv.start([]*DefaultPeer{})

	log.Println("Dialing...")
	dialer := new(websocket.Dialer)
	dialer.TLSClientConfig = new(tls.Config)
	dialer.TLSClientConfig.InsecureSkipVerify = true
	ws, _, err := dialer.Dial(rpcLoc, nil)
	if err != nil {
		log.Fatalln(err)
	}

	client := rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(ws.UnderlyingConn()))

	go runner(client, t)

	client.Run()
	serv.Stop()
	serv.WaitForShutdown()
}
