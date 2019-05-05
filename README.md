# CloudChain RKSync

[![Build Status](https://travis-ci.org/rkcloudchain/rksync.svg?branch=master)](https://travis-ci.org/rkcloudchain/rksync)
[![codecov](https://codecov.io/gh/rkcloudchain/rksync/branch/master/graph/badge.svg)](https://codecov.io/gh/rkcloudchain/rksync)
[![Go Report Card](https://goreportcard.com/badge/github.com/rkcloudchain/rksync)](https://goreportcard.com/report/github.com/rkcloudchain/rksync)

CloucChain rksync is a distributed file synchronization protocol. It securely synchronizes files between specified network members. Each of these network members has a digital identity encapsulated in an X.509 digital certificate. All network members constitute a private "subnet" called `channel`. Channel specifies all authorized network members and the files that need to be synchronized. Files can only be synchronized between network members joining the same channel.

A channel is created by a network member, the creator is called `leader`. Only leader can add other network members to the channel, or specify files that need to be synchronized. The channel information is synchronized between all network members through the gossip protocol.

Any non-leader node will randomly selects a neighboring node for file data synchronization. The node periodically ask the neighbor node for file updates. and receives updated data for the file from the node if the file has been updated.

## Goals

Rksync is inspired by [Hyperledger fabric](https://github.com/hyperledger/fabric) block data synchronization algorithm. Here are the goals:

1. **Security**

    First of all, protecting the user's data is paramount. Regardless of our other goals we must never allow the user's data to be susceptible to eavesdropping or modification by unauthorized parties.

2. **Easy**

    A high-level interface designed to reduce the work of implementing file transfer in your application.

3. **High availability**

    Component failures are the norm rather than the exception, therefor, fault tolerance and automatic recovery must be integral to the system.

4. **Automatic**

    User interaction should be required only when absolutely necessary.

## Install

```Go
go get -u github.com/rkcloudchain/rksync
```

## Example

First, you need to start the rksync service:

```Go
package main

import (
    "net"

    "github.com/rkcloudchain/rksync"
    "github.com/rkcloudchain/rksync/config"
)

func main() {
    cfg := &config.Config{
        HomeDir: "/pth/to/home/directory",
        Gossip: &config.GossipConfig{
            FileSystem:     &MyFileSystem{},
            BootstrapPeers: []string{"localhost:8053"},
            Endpoint:       "localhost:9053",
        },
        Identity: &config.IdentityConfig{
            ID: "nodeID",
        },
    }

    lis, err := net.Listen("tcp", "0.0.0.0:9053")
    if err != nil {
        panic(err)
    }

    srv, err := rksync.Serve(lis, cfg)
    if err != nil {
        panic(err)
    }
    defer srv.Stop()
}
```

### Note

1. **You need to implement the FileSystem interface yourself**

    ```Go
    // FileSystem enables the rksync to communicate with file system.
    type FileSystem interface {
        // Create creates the named file
        Create(chainID string, fmeta FileMeta) (File, error)

        // OpenFile opens a file using the given flags and the given mode.
        OpenFile(chainID string, fmeta FileMeta, flag int, perm os.FileMode) (File, error)

        // Stat returns a FileInfo describing the named file.
        Stat(chainID string, fmeta FileMeta) (os.FileInfo, error)
    }

    // File represents a file in the filesystem
    type File interface {
        io.Closer
        io.ReaderAt
        io.Writer
    }
    ```

2. **The x.509 certificate file representing the digital identity should be placed in the HomeDir directory.**

    The directory structure is:

    ```text
    /HomeDir
      /csp
        /signcerts // Store public key certificate
        /keystore  // Store private key
        /cacerts   // Store trusted CA certificates
    ```

3. **BootstrapPeers**

    The seed node list needs to be added to the BootstrapPeers configuration item. This configuration of all nodes should be consistent.

Once the service is started, you can do the corresponding operation:

* CreateChannel

    ```Go
    err = srv.CreateChannel("testchannel", []common.FileSyncInfo{
        common.FileSyncInfo{Path: "file1.txt", Mode: "Append", Metadata: []byte{...}},
        common.FileSyncInfo{Path: "file2.txt", Mode: "Append", Metadata: []byte{...}},
    })
    ```

* AddMemberToChan

    ```Go
    err = srv.AddMemberToChan("testchannel", "node2ID", node2Cert)
    ```

* AddFileToChan

    ```Go
    err = srv.AddFileToChan("testchannel", "file3.txt", "Append", []byte{...})
    ```

## Current State

RKSync is still in development and the API may be changed. Therefore, we do not guarantee the backward compatibility of the library for the time being.

## License

RKSync is under the Apache 2.0 license. See the [LICENSE](https://github.com/rkcloudchain/rksync/blob/master/LICENSE) file for details.