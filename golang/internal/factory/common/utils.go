package common

import (
	"fmt"
	"math/rand/v2"
)

func GenerateNewConsumerTag(name string) string {
	return fmt.Sprintf("consumer-tag-%s-%d", name, rand.Uint64())
}
