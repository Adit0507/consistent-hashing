package utils

import (
	"crypto/sha1"
	"strconv"
)

func Hash(key string) int {
	h := sha1.New()
	h.Write([]byte(key))
	hashBytes := h.Sum(nil)

	// convertin first 4 bytes to int
	hash := int(hashBytes[0])<< 24+ int(hashBytes[1])<<16 + int(hashBytes[2])<<8 + int(hashBytes[3])

	if hash < 0 {
		hash -= hash
	}
	return hash
}

func GenerateVirtualNodeKey(nodeName string, index int) string {
	return nodeName + "#"+ strconv.Itoa(index)
}