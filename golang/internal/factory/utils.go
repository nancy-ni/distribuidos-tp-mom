package factory

import (
	"fmt"
	"math/rand/v2"
)

const MAX_CONSUMERTAG_NUMBER = 999999999

func generateNewConsumerTag(name string) string {
	return fmt.Sprintf("consumer-tag-%s-%d", name, rand.IntN(MAX_CONSUMERTAG_NUMBER))
}
