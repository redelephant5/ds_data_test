package utils

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
)

func CalculateHash(reader io.Reader) string{
	h := sha256.New()
	io.Copy(h, reader)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
