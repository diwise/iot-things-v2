package application

import (
	"context"

	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
	"github.com/diwise/messaging-golang/pkg/messaging"
)

// TopicPublisher är produktions-EventPublisher över messaging-golang:
// ThingValuesEventV1 implementerar messaging.TopicMessage (topic
// things.v1.values), så PublishOnTopic routar, bekräftar och felklassar
// enligt bibliotekets kontrakt (ingen blind omsändning vid unknown).
type TopicPublisher struct {
	MsgCtx messaging.MsgContext
}

// PublishThingValues publicerar ett konvergerat batch-event.
func (p *TopicPublisher) PublishThingValues(ctx context.Context, ev pkgtypes.ThingValuesEventV1) error {
	return p.MsgCtx.PublishOnTopic(ctx, ev)
}
