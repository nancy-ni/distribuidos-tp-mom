package factory

import (
	"fmt"
	"math/rand/v2"
)

func generateNewConsumerTag(name string) string {
	return fmt.Sprintf("consumer-tag-%s-%d", name, rand.Uint64())
}
