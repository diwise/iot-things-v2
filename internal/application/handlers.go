package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	diwisepkg "github.com/diwise/senml/diwise"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var tracer = otel.Tracer("iot-things-v2")

var errMissingTenant = errors.New("message contains no tenant")

// NewMeasurementsHandler mirrors iot-things internal/application/handlers.go:
// same envelope ({pack, timestamp}), same ack contract (structural/identity
// errors are Permanent, handling errors propagate for retry), same reference
// time (reception time) for relative SenML times.
func NewMeasurementsHandler(c context.Context, app ThingsApp) messaging.TopicMessageHandler {
	log := logging.GetFromContext(c)

	totalCounter, err := otel.Meter("iot-things-v2/measurements").Int64Counter(
		"diwise.things.measurements.total",
		metric.WithUnit("1"),
		metric.WithDescription("Total number of received measurements"),
	)
	if err != nil {
		log.Error("failed to create otel total measurements counter", "err", err.Error())
	}

	return func(ctx context.Context, topicMessage messaging.IncomingTopicMessage, logger *slog.Logger) error {
		var err error

		logger = logger.With("topic_name", topicMessage.TopicName())

		ctx, span := runtimeTracer.Start(ctx, "receive-measurements")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, log := o11y.AddTraceIDToLoggerAndStoreInContext(span, logger, ctx)

		msg := struct {
			Pack      senml.Pack `json:"pack"`
			Timestamp time.Time  `json:"timestamp"`
		}{}

		err = json.Unmarshal(topicMessage.Body(), &msg)
		if err != nil {
			log.Error("could not unmarshal message", "err", err.Error())
			return messaging.Permanent(err)
		}

		receivedAt := msg.Timestamp
		if receivedAt.IsZero() {
			receivedAt = time.Now().UTC()
		}

		parsed, err := diwisepkg.Parse(msg.Pack, receivedAt)
		if err != nil {
			log.Error("message contains an invalid package", "err", err.Error())
			return messaging.Permanent(err)
		}

		deviceID := parsed.DeviceID()
		tenant := parsed.Tenant()

		if tenant == "" {
			// Core berikar och validerar tenant; tomt värde är ett
			// kontraktsbrott som aldrig läker vid retry.
			err = errMissingTenant
			log.Error("message contains no tenant")
			return messaging.Permanent(err)
		}
		if topicMessage.MessageID() == "" {
			// Dedup/outbox kräver stabilt messageID (L6/L10); tomt ID läker
			// aldrig vid retry.
			log.Error("message contains no message ID")
			return messaging.Permanent(fmt.Errorf("message contains no message ID"))
		}

		logger = logger.With("device_id", deviceID, "tenant", tenant)

		observations := convPack(parsed, receivedAt, topicMessage.MessageID())

		if len(observations) == 0 {
			log.Warn("no measurements found in pack")
			return nil
		}

		totalCounter.Add(ctx, 1)

		ctx = logging.NewContextWithLogger(ctx, logger)

		// Tilldela yttre err (inte skugga med :=) så att deferred
		// span-avslut ser felet och sätter Error-status (TV-07).
		if err = app.HandleMeasurements(ctx, tenant, topicMessage.MessageID(), observations); err != nil {
			log.Error("could not handle measurements", "err", err.Error())
			return err
		}

		return nil
	}
}

// convPack mirrors iot-things convPack: one observation per resource with the
// observation's own URN. ID semantics are device/[channel]/object/resource;
// the resource lookup stays within its observation so identical resource
// numbers from different objects never mix. v2 maps onto things.Signal +
// things.Observation instead of things.Measurement.
func convPack(parsed *diwisepkg.Pack, receivedAt time.Time, messageID string) []things.Observation {
	observations := make([]things.Observation, 0)

	for _, o := range parsed.Objects() {
		urn := o.URN()
		meta := o.Metadata()

		for _, r := range o.Resources() {
			name := r.Name[strings.LastIndex(r.Name, "/")+1:]
			n, err := strconv.Atoi(name)
			if err != nil || n == 0 {
				continue // Header (n:0) och icke-numeriska postnamn är aldrig mätvärden.
			}

			if r.Value == nil && r.BoolValue == nil {
				continue
			}

			ts, _ := r.GetTime()

			sig := signalOf(r.Name, urn)

			obs := things.Observation{
				Signal:     sig,
				Value:      r.Value,
				BoolValue:  r.BoolValue,
				Unit:       r.Unit,
				ObservedAt: ts.UTC(),
				ReceivedAt: receivedAt,
				Quality:    things.QualityOK,
				Source:     meta.Source,
				MessageID:  messageID,
			}

			observations = append(observations, obs)
		}
	}

	return observations
}

// signalOf splits device/[channel...]/object/resource; object semantics come
// from the observation URN (same split as iot-things SignalOf).
func signalOf(id, urn string) things.Signal {
	parts := strings.Split(id, "/")
	s := things.Signal{DeviceID: parts[0], Object: urn}
	if len(parts) >= 2 {
		s.Resource = parts[len(parts)-1]
	}
	if len(parts) >= 3 {
		s.Channel = strings.Join(parts[1:len(parts)-2], "/")
	}
	return s
}
