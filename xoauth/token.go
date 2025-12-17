package xoauth

import "time"

type Token struct {
	AccessToken string
	TokenType   string
	Expiry      time.Time
	Scope       string
}
