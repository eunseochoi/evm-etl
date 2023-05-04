package main

import (
	"encoding/json"
	"fmt"
	"github.com/coherentopensource/chain-interactor/client/node"
	protos "github.com/coherentopensource/chain-interactor/protos/go/protos/chains/base"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	resp := `{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "baseFeePerGas": "0x31",
    "difficulty": "0x0",
    "extraData": "0x",
    "gasLimit": "0x17d7840",
    "gasUsed": "0x24a6b",
    "hash": "0x7edba9bf0ac05b8f5b97f31bc54c54b0891414023b2e6ec27beef93c9925c298",
    "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    "miner": "0x4200000000000000000000000000000000000011",
    "mixHash": "0x619401af27f98a08806d0356debd276fe31908f4fac1705162a19e1ce76932e3",
    "nonce": "0x0000000000000000",
    "number": "0x1d76",
    "parentHash": "0xfa5a432730100381fd6d5caf4d26d8d4ed4860f320028a0175883c4965d2c85b",
    "receiptsRoot": "0x6bd5f551e880df15417b7936de8e9a6380e55095bb0ebc4426dacccc43ef3f3f",
    "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
    "size": "0x555",
    "stateRoot": "0x69e9ff4914270906009127813dfae935ae86bcad6d2fe832055e5bdbd799e6be",
    "timestamp": "0x63d9a7fc",
    "totalDifficulty": "0x0",
    "transactions": [
      {
        "blockHash": "0x7edba9bf0ac05b8f5b97f31bc54c54b0891414023b2e6ec27beef93c9925c298",
        "blockNumber": "0x1d76",
        "from": "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001",
        "gas": "0x8f0d180",
        "gasPrice": null,
        "hash": "0x490a4493f0603641f9620c0f63c6c0edb611b28755a3371ef9677c37bf44542e",
        "input": "0x015d8eb90000000000000000000000000000000000000000000000000000000000805b6c0000000000000000000000000000000000000000000000000000000063d9a790000000000000000000000000000000000000000000000000000000000000000a35788dad109bed9d3527dc1547dea6a9709fa46349d18f61aaa95a11204b0dcc00000000000000000000000000000000000000000000000000000000000000100000000000000000000000002d679b567db6187c0c8323fa982cfb88b74dbcc7000000000000000000000000000000000000000000000000000000000000083400000000000000000000000000000000000000000000000000000000000f4240",
        "nonce": "0x0",
        "to": "0x4200000000000000000000000000000000000015",
        "transactionIndex": "0x0",
        "value": "0x0",
        "type": "0x7e",
        "v": null,
        "r": null,
        "s": null,
        "sourceHash": "0x2574d039d25462de6065077c5c8f2fc27fcd48e3d8bda1b1b3c7163872c46573",
        "mint": "0x0",
        "isSystemTx": true
      },
      {
        "blockHash": "0x7edba9bf0ac05b8f5b97f31bc54c54b0891414023b2e6ec27beef93c9925c298",
        "blockNumber": "0x1d76",
        "from": "0x21856935e5689490c72865f34cc665d0ff25664b",
        "gas": "0x21dee",
        "gasPrice": "0xb2d05e31",
        "maxFeePerGas": "0xb2d05e62",
        "maxPriorityFeePerGas": "0xb2d05e00",
        "hash": "0xfb06256cfd36ecb25ff5675154b0590ffd354b3ca14b1451f4778531aaf96512",
        "input": "0x608060405234801561001057600080fd5b5060f78061001f6000396000f3fe6080604052348015600f57600080fd5b5060043610603c5760003560e01c80633fb5c1cb1460415780638381f58a146053578063d09de08a14606d575b600080fd5b6051604c3660046083565b600055565b005b605b60005481565b60405190815260200160405180910390f35b6051600080549080607c83609b565b9190505550565b600060208284031215609457600080fd5b5035919050565b60006001820160ba57634e487b7160e01b600052601160045260246000fd5b506001019056fea264697066735822122011622203a9c30f4b42b6454e5be5be123ac648b6381896fdd352122fae296da364736f6c634300080f0033",
        "nonce": "0x1",
        "to": null,
        "transactionIndex": "0x1",
        "value": "0x0",
        "type": "0x2",
        "accessList": [],
        "chainId": "0x14a33",
        "v": "0x1",
        "r": "0x1befa588abf78d57c1aee1a715fefbfa64b6718bdcc59bc99c2a8ef13d972f60",
        "s": "0x2060ec7b3f4f1fb1d23cdf6962e52c0356226cc83fe3766b19516270456ad7ad"
      },
      {
        "blockHash": "0x7edba9bf0ac05b8f5b97f31bc54c54b0891414023b2e6ec27beef93c9925c298",
        "blockNumber": "0x1d76",
        "from": "0x21856935e5689490c72865f34cc665d0ff25664b",
        "gas": "0xea2e",
        "gasPrice": "0xb2d05e31",
        "maxFeePerGas": "0xb2d05e62",
        "maxPriorityFeePerGas": "0xb2d05e00",
        "hash": "0xdb3e1f70e5b5d8ed94706468d4fbe8005af9051f321a7fe4e01931c37fdf4dad",
        "input": "0xd09de08a",
        "nonce": "0x2",
        "to": "0xbb89e84cb43f9a206dd6ff97b5b1a041dd075aff",
        "transactionIndex": "0x2",
        "value": "0x0",
        "type": "0x2",
        "accessList": [],
        "chainId": "0x14a33",
        "v": "0x1",
        "r": "0x297216c700587958f7e8d50dce129a5834d340e2bb4128499ef1a7169a46cf9",
        "s": "0x2e4c38a47b72c7cd7f01756f948d1fe396ade4be8bc6114e45e94861b4fa6908"
      }
    ],
    "transactionsRoot": "0xfc3572129ebc7ab17b8742059f16b99a244c474ebd6e1ee1ecf6dafa5caabb40",
    "uncles": []
  }
}`
	respStruct := node.BlockResponse{}
	err := json.Unmarshal([]byte(resp), &respStruct)
	if err != nil {
		panic(err)
	}
	protoResp := &protos.Block{}
	err = protojson.Unmarshal(respStruct.Result, protoResp)
	if err != nil {
		panic(err)
	}
	fmt.Println(protoResp)
}
