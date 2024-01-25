package kafka

import (
	"crypto/sha256"
	"crypto/sha512"

	"github.com/xdg-go/scram"
)

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type SCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func NewSCRAMClient(hashFn scram.HashGeneratorFcn) *SCRAMClient {
	return &SCRAMClient{
		HashGeneratorFcn: hashFn,
	}
}

func (x *SCRAMClient) Begin(userName, password, authzID string) error {
	var err error
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *SCRAMClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

func (x *SCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
