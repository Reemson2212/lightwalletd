// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .
package common

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/asherda/lightwalletd/parser"
	"github.com/asherda/lightwalletd/walletrpc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// 'make build' will overwrite this string with the output of git-describe (tag)
var Version = "v0.0.0.0-dev"
var GitCommit = ""
var BuildDate = ""
var BuildUser = ""

type Options struct {
	GRPCBindAddr      string `json:"grpc_bind_address,omitempty"`
	HTTPBindAddr      string `json:"http_bind_address,omitempty"`
	TLSCertPath       string `json:"tls_cert_path,omitempty"`
	TLSKeyPath        string `json:"tls_cert_key,omitempty"`
	LogLevel          uint64 `json:"log_level,omitempty"`
	LogFile           string `json:"log_file,omitempty"`
	ZcashConfPath     string `json:"zcash_conf,omitempty"`
	NoTLSVeryInsecure bool   `json:"no_tls_very_insecure,omitempty"`
	Redownload        bool   `json:"redownload"`
	DataDir           string `json:"data-dir"`
	Darkside          bool   `json:"darkside"`
}

// RawRequest points to the function to send a an RPC request to zcashd;
// in production, it points to btcsuite/btcd/rpcclient/rawrequest.go:RawRequest();
// in unit tests it points to a function to mock RPCs to zcashd.
var RawRequest func(method string, params []json.RawMessage) (json.RawMessage, error)

// Sleep allows a request to time.Sleep() to be mocked for testing;
// in production, it points to the standard library time.Sleep();
// in unit tests it points to a mock function.
var Sleep func(d time.Duration)

// Log as a global variable simplifies logging
var Log *logrus.Entry

// GetSaplingInfo returns the result of the getblockchaininfo RPC to zcashd
func GetSaplingInfo() (int, int, string, string) {
	// This request must succeed or we can't go on; give zcashd time to start up
	var f interface{}
	retryCount := 0
	for {
		result, rpcErr := RawRequest("getblockchaininfo", []json.RawMessage{})
		if rpcErr == nil {
			if retryCount > 0 {
				Log.Warn("getblockchaininfo RPC successful")
			}
			err := json.Unmarshal(result, &f)
			if err != nil {
				Log.Fatalf("error parsing JSON getblockchaininfo response: %v", err)
			}
			break
		}
		retryCount++
		if retryCount > 10 {
			Log.WithFields(logrus.Fields{
				"timeouts": retryCount,
			}).Fatal("unable to issue getblockchaininfo RPC call to zcashd node")
		}
		Log.WithFields(logrus.Fields{
			"error": rpcErr.Error(),
			"retry": retryCount,
		}).Warn("error with getblockchaininfo rpc, retrying...")
		Sleep(time.Duration(10+retryCount*5) * time.Second) // backoff
	}

	chainName := f.(map[string]interface{})["chain"].(string)

	upgradeJSON := f.(map[string]interface{})["upgrades"]

	// If the sapling consensus branch doesn't exist, it must be regtest
	saplingHeight := float64(0)
	if saplingJSON, ok := upgradeJSON.(map[string]interface{})["76b809bb"]; ok { // Sapling ID
		saplingHeight = saplingJSON.(map[string]interface{})["activationheight"].(float64)
	}

	blockHeight := f.(map[string]interface{})["headers"].(float64)

	consensus := f.(map[string]interface{})["consensus"]

	branchID := consensus.(map[string]interface{})["nextblock"].(string)

	return int(saplingHeight), int(blockHeight), chainName, branchID
}

func getBlockFromRPC(height int) (*walletrpc.CompactBlock, error) {
	params := make([]json.RawMessage, 2)
	params[0] = json.RawMessage("\"" + strconv.Itoa(height) + "\"")
	params[1] = json.RawMessage("0") // non-verbose (raw hex)
	result, rpcErr := RawRequest("getblock", params)

	// For some reason, the error responses are not JSON
	if rpcErr != nil {
		// Check to see if we are requesting a height the zcashd doesn't have yet
		if (strings.Split(rpcErr.Error(), ":"))[0] == "-8" {
			return nil, nil
		}
		return nil, errors.Wrap(rpcErr, "error requesting block")
	}

	var blockDataHex string
	err := json.Unmarshal(result, &blockDataHex)
	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}

	blockData, err := hex.DecodeString(blockDataHex)
	if err != nil {
		return nil, errors.Wrap(err, "error decoding getblock output")
	}

	block := parser.NewBlock()
	rest, err := block.ParseFromSlice(blockData)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing block")
	}
	if len(rest) != 0 {
		return nil, errors.New("received overlong message")
	}

	// TODO COINBASE-HEIGHT: restore this check after coinbase height is fixed
	if false && block.GetHeight() != height {
		return nil, errors.New("received unexpected height block")
	}

	return block.ToCompact(), nil
}

// BlockIngestor runs as a goroutine and polls zcashd for new blocks, adding them
// to the cache. The repetition count, rep, is nonzero only for unit-testing.
func BlockIngestor(c *BlockCache, rep int) {
	lastLog := time.Now()
	reorgCount := 0
	lastHeightLogged := 0
	retryCount := 0
	wait := true

	// Start listening for new blocks
	for i := 0; rep == 0 || i < rep; i++ {
		height := c.GetNextHeight()
		block, err := getBlockFromRPC(height)
		if err != nil {
			Log.WithFields(logrus.Fields{
				"height": height,
				"error":  err,
			}).Warn("error zcashd getblock rpc")
			retryCount++
			if retryCount > 10 {
				Log.WithFields(logrus.Fields{
					"timeouts": retryCount,
				}).Fatal("unable to issue RPC call to zcashd node")
			}
			// Delay then retry the same height.
			c.Sync()
			Sleep(10 * time.Second)
			wait = true
			continue
		}
		retryCount = 0
		if block == nil {
			// No block at this height.
			if wait {
				// Wait a bit then retry the same height.
				c.Sync()
				if lastHeightLogged+1 != height {
					Log.Info("Ingestor waiting for block: ", height)
				}
				Sleep(10 * time.Second)
				wait = false
				continue
			}
		}
		if block == nil || c.HashMismatch(block.PrevHash) {
			// This may not be a reorg; it may be we're at the tip
			// and there's no new block yet, but we want to back up
			// so we detect a reorg in which the new chain is the
			// same length or shorter.
			reorgCount += 1
			if reorgCount > 100 {
				Log.Fatal("Reorg exceeded max of 100 blocks! Help!")
			}
			// Print the hash of the block that is getting reorg-ed away
			// as 'phash', not the prevhash of the block we just received.
			if block != nil {
				Log.WithFields(logrus.Fields{
					"height": height,
					"hash":   displayHash(block.Hash),
					"phash":  displayHash(c.GetLatestHash()),
					"reorg":  reorgCount,
				}).Warn("REORG")
			} else if reorgCount > 1 {
				Log.WithFields(logrus.Fields{
					"height": height,
					"phash":  displayHash(c.GetLatestHash()),
					"reorg":  reorgCount,
				}).Warn("REORG")
			}
			// Try backing up
			c.Reorg(height - 1)
			Sleep(1 * time.Second)
			continue
		}
		// We have a valid block to add.
		wait = true
		reorgCount = 0
		if err := c.Add(height, block); err != nil {
			Log.Fatal("Cache add failed:", err)
		}
		// Don't log these too often.
		if time.Now().Sub(lastLog).Seconds() >= 4 && c.GetNextHeight() == height+1 && height != lastHeightLogged {
			lastLog = time.Now()
			lastHeightLogged = height
			Log.Info("Ingestor adding block to cache: ", height)
		}
	}
}

// GetBlock returns the compact block at the requested height, first by querying
// the cache, then, if not found, will request the block from zcashd. It returns
// nil if no block exists at this height.
func GetBlock(cache *BlockCache, height int) (*walletrpc.CompactBlock, error) {
	// First, check the cache to see if we have the block
	block := cache.Get(height)
	if block != nil {
		return block, nil
	}

	// Not in the cache, ask zcashd
	block, err := getBlockFromRPC(height)
	if err != nil {
		return nil, err
	}
	if block == nil {
		// Block height is too large
		return nil, errors.New("block requested is newer than latest block")
	}
	return block, nil
}

// GetBlockRange returns a sequence of consecutive blocks in the given range.
func GetBlockRange(cache *BlockCache, blockOut chan<- walletrpc.CompactBlock, errOut chan<- error, start, end int) {
	// Go over [start, end] inclusive
	for i := start; i <= end; i++ {
		block, err := GetBlock(cache, i)
		if err != nil {
			errOut <- err
			return
		}
		blockOut <- *block
	}
	errOut <- nil
}

func displayHash(hash []byte) string {
	rhash := make([]byte, len(hash))
	copy(rhash, hash)
	// Reverse byte order
	for i := 0; i < len(rhash)/2; i++ {
		j := len(rhash) - 1 - i
		rhash[i], rhash[j] = rhash[j], rhash[i]
	}
	return hex.EncodeToString(rhash)
}

// Identity

// Registers a name commitment, which is required as a source for the name to be used when registering an identity. The name commitment hides the name itself
// while ensuring that the miner who mines in the registration cannot front-run the name unless they have also registered a name commitment for the same name or
// are willing to forfeit the offer of payment for the chance that a commitment made now will allow them to register the name in the future.
func RegisterNameCommitment(request *walletrpc.RegisterNameCommitmentRequest) (response *walletrpc.RegisterNameCommitmentResponse, err error) {
	paramCount := 2
	if request.Referralidentity != "" {
		paramCount = 3
	}
	params := make([]json.RawMessage, paramCount)
	params[0] = json.RawMessage("\"" + request.GetName() + "\"")
	params[1] = json.RawMessage("\"" + request.GetControllingaddress() + "\"")
	if request.Referralidentity != "" {
		params[2] = json.RawMessage("\"" + request.GetReferralidentity() + "\"")
	}
	result, rpcErr := RawRequest("registernamecommitment", params)

	// For some reason, the error responses are not JSON
	if rpcErr != nil {
		return nil, rpcErr
	}

	err = json.Unmarshal(result, &response)
	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}

	return response, nil
}

func RegisterIdentity(request *walletrpc.RegisterIdentityRequest) (response *walletrpc.RegisterIdentityResponse, err error) {

	params := make([]json.RawMessage, 1)
	requestBytes, err := json.Marshal(&request)
	params[0] = json.RawMessage(string(requestBytes))
	if err != nil {
		return nil, errors.Wrap(err, "error reading request")
	}
	result, rpcErr := RawRequest("registeridentity", params)

	if rpcErr != nil {
		return nil, rpcErr
	}

	var txid string
	err = json.Unmarshal(result, &txid)
	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}
	return &walletrpc.RegisterIdentityResponse{
		Txid: txid,
	}, nil
}

func RevokeIdentity(request *walletrpc.RevokeIdentityRequest) (*walletrpc.RevokeIdentityResponse, error) {

	params := make([]json.RawMessage, 1)
	params[0] = json.RawMessage("\"" + request.GetIdentity() + "\"")
	result, rpcErr := RawRequest("revokeidentity", params)

	if rpcErr != nil {
		return nil, rpcErr
	}

	var txid string
	err := json.Unmarshal(result, txid)
	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}

	return &walletrpc.RevokeIdentityResponse{
		Txid: txid,
	}, nil
}

func RecoverIdentity(request *walletrpc.RecoverIdentityRequest) (*walletrpc.RecoverIdentityResponse, error) {

	params := make([]json.RawMessage, 1)
	requestBytes, err := json.Marshal(request.GetIdentity())
	params[0] = json.RawMessage(string(requestBytes))

	if err != nil {
		return nil, errors.Wrap(err, "error reading request")
	}

	result, rpcErr := RawRequest("recoveridentity", params)

	if rpcErr != nil {
		return nil, rpcErr
	}

	var txid string
	err = json.Unmarshal(result, txid)
	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}

	return &walletrpc.RecoverIdentityResponse{
		Txid: txid,
	}, nil
}

func UpdateIdentity(request *walletrpc.UpdateIdentityRequest) (*walletrpc.UpdateIdentityResponse, error) {

	params := make([]json.RawMessage, 1)
	requestBytes, err := json.Marshal(request.GetIdentity())
	params[0] = json.RawMessage(string(requestBytes))

	result, rpcErr := RawRequest("updateidentity", params)

	if rpcErr != nil {
		return nil, rpcErr
	}

	response := &walletrpc.UpdateIdentityResponse{}
	err = json.Unmarshal(result, response.Txid)
	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}

	return response, nil
}

func GetIdentity(request *walletrpc.GetIdentityRequest) (*walletrpc.GetIdentityResponse, error) {

	params := make([]json.RawMessage, 1)
	params[0] = json.RawMessage("\"" + request.GetIdentity() + "\"")
	result, rpcErr := RawRequest("getidentity", params)

	if rpcErr != nil {
		return nil, rpcErr
	}

	response := &walletrpc.GetIdentityResponse{}
	err := json.Unmarshal(result, &response.Identityinfo)

	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}

	return response, nil
}

func VerifyMessage(request *walletrpc.VerifyMessageRequest) (*walletrpc.VerifyMessageResponse, error) {
	params := make([]json.RawMessage, 4)
	params[0] = json.RawMessage("\"" + request.Signer + "\"")
	params[1] = json.RawMessage("\"" + request.Signature + "\"")
	params[2] = json.RawMessage("\"" + request.Message + "\"")
	params[3] = json.RawMessage("\"" + strconv.FormatBool(request.Checklatest) + "\"")

	result, rpcErr := RawRequest("verifymessage", params)

	if rpcErr != nil {
		return nil, rpcErr
	}

	var signatureisvalid bool
	err := json.Unmarshal(result, &signatureisvalid)

	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}

	return &walletrpc.VerifyMessageResponse{
		Signatureisvalid: signatureisvalid,
	}, err
}

func VerifyHash(request *walletrpc.VerifyHashRequest) (*walletrpc.VerifyHashResponse, error) {
	params := make([]json.RawMessage, 4)
	params[0] = json.RawMessage("\"" + request.Signer + "\"")
	params[1] = json.RawMessage("\"" + request.Signature + "\"")
	params[2] = json.RawMessage("\"" + request.Hash + "\"")
	params[3] = json.RawMessage("\"" + strconv.FormatBool(request.Checklatest) + "\"")

	result, rpcErr := RawRequest("verifyhash", params)

	if rpcErr != nil {
		return nil, rpcErr
	}

	var signatureisvalid bool
	err := json.Unmarshal(result, &signatureisvalid)

	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}

	return &walletrpc.VerifyHashResponse{
		Signatureisvalid: signatureisvalid,
	}, err
}
