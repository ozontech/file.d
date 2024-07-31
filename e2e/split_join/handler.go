package split_join

import (
	"github.com/Shopify/sarama"
)

type handlerFunc func(message *sarama.ConsumerMessage)

func (h handlerFunc) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (h handlerFunc) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h handlerFunc) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h(msg)
		session.MarkMessage(msg, "")
	}
	return nil
}
