package xscram

import (
	"crypto/sha256"
	"crypto/sha512"

	"github.com/xdg-go/scram"
)

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type Client struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func NewClient(hashFn scram.HashGeneratorFcn) *Client {
	return &Client{
		HashGeneratorFcn: hashFn,
	}
}

func (x *Client) Begin(userName, password, authzID string) error {
	var err error
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *Client) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

func (x *Client) Done() bool {
	return x.ClientConversation.Done()
}
