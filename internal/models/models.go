package models

import (
	"encoding/json"
	"errors"
	"time"
)

const (
	EventTypeCustomerCreated = "CUSTOMER_CREATED"
	EventTypeOrderCreated    = "ORDER_CREATED"
)

var (
	ErrMissingField = errors.New("missing required field")
	ErrInvalidTime  = errors.New("invalid event_time")
)

type CustomerEvent struct {
	EventID    string
	EventTime  time.Time
	EventType  string
	CustomerID string
}

type OrderEvent struct {
	EventID     string
	EventTime   time.Time
	EventType   string
	OrderID     string
	CustomerID  string
	TotalAmount float64
	Currency    string
}

type eventTypeEnvelope struct {
	EventType string `json:"event_type"`
}

type customerEventJSON struct {
	EventID    string `json:"event_id"`
	EventTime  string `json:"event_time"`
	EventType  string `json:"event_type"`
	CustomerID string `json:"customer_id"`
}

type orderEventJSON struct {
	EventID     string  `json:"event_id"`
	EventTime   string  `json:"event_time"`
	EventType   string  `json:"event_type"`
	OrderID     string  `json:"order_id"`
	CustomerID  string  `json:"customer_id"`
	TotalAmount float64 `json:"total_amount"`
	Currency    string  `json:"currency"`
}

func DetectEventType(payload []byte) (string, error) {
	var env eventTypeEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return "", err
	}
	if env.EventType == "" {
		return "", ErrMissingField
	}
	return env.EventType, nil
}

func ParseCustomerEvent(payload []byte) (CustomerEvent, error) {
	var raw customerEventJSON
	if err := json.Unmarshal(payload, &raw); err != nil {
		return CustomerEvent{}, err
	}
	eventTime, err := parseEventTime(raw.EventTime)
	if err != nil {
		return CustomerEvent{}, err
	}
	if raw.EventID == "" || raw.EventType == "" || raw.CustomerID == "" {
		return CustomerEvent{}, ErrMissingField
	}
	if raw.EventType != EventTypeCustomerCreated {
		return CustomerEvent{}, errors.New("unexpected event_type")
	}
	return CustomerEvent{
		EventID:    raw.EventID,
		EventTime:  eventTime,
		EventType:  raw.EventType,
		CustomerID: raw.CustomerID,
	}, nil
}

func ParseOrderEvent(payload []byte) (OrderEvent, error) {
	var raw orderEventJSON
	if err := json.Unmarshal(payload, &raw); err != nil {
		return OrderEvent{}, err
	}
	eventTime, err := parseEventTime(raw.EventTime)
	if err != nil {
		return OrderEvent{}, err
	}
	if raw.EventID == "" || raw.EventType == "" || raw.OrderID == "" || raw.CustomerID == "" || raw.Currency == "" {
		return OrderEvent{}, ErrMissingField
	}
	if raw.EventType != EventTypeOrderCreated {
		return OrderEvent{}, errors.New("unexpected event_type")
	}
	return OrderEvent{
		EventID:     raw.EventID,
		EventTime:   eventTime,
		EventType:   raw.EventType,
		OrderID:     raw.OrderID,
		CustomerID:  raw.CustomerID,
		TotalAmount: raw.TotalAmount,
		Currency:    raw.Currency,
	}, nil
}

func parseEventTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, ErrMissingField
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}
	return time.Time{}, ErrInvalidTime
}
