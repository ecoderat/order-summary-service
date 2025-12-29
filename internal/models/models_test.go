package models

import "testing"

func TestDetectEventType(t *testing.T) {
	payload := []byte(`{"event_type":"CUSTOMER_CREATED"}`)
	typeName, err := DetectEventType(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if typeName != EventTypeCustomerCreated {
		t.Fatalf("unexpected type %q", typeName)
	}
}

func TestDetectEventTypeMissing(t *testing.T) {
	payload := []byte(`{"event_id":"1"}`)
	_, err := DetectEventType(payload)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestParseCustomerEvent(t *testing.T) {
	payload := []byte(`{"event_id":"e1","event_time":"2025-12-01T10:00:00Z","event_type":"CUSTOMER_CREATED","customer_id":"C123"}`)
	evt, err := ParseCustomerEvent(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if evt.CustomerID != "C123" || evt.EventID != "e1" {
		t.Fatalf("unexpected event: %+v", evt)
	}
}

func TestParseCustomerEventMissing(t *testing.T) {
	payload := []byte(`{"event_id":"e1","event_time":"2025-12-01T10:00:00Z","event_type":"CUSTOMER_CREATED"}`)
	_, err := ParseCustomerEvent(payload)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestParseCustomerEventInvalidTime(t *testing.T) {
	payload := []byte(`{"event_id":"e1","event_time":"not-a-time","event_type":"CUSTOMER_CREATED","customer_id":"C123"}`)
	_, err := ParseCustomerEvent(payload)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestParseOrderEvent(t *testing.T) {
	payload := []byte(`{"event_id":"e2","event_time":"2025-12-10T14:30:00Z","event_type":"ORDER_CREATED","order_id":"O456","customer_id":"C123","total_amount":1250.75,"currency":"TRY"}`)
	evt, err := ParseOrderEvent(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if evt.OrderID != "O456" || evt.CustomerID != "C123" {
		t.Fatalf("unexpected event: %+v", evt)
	}
}

func TestParseOrderEventMissing(t *testing.T) {
	payload := []byte(`{"event_id":"e2","event_time":"2025-12-10T14:30:00Z","event_type":"ORDER_CREATED","order_id":"O456","customer_id":"C123"}`)
	_, err := ParseOrderEvent(payload)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestParseOrderEventInvalidTime(t *testing.T) {
	payload := []byte(`{"event_id":"e2","event_time":"bad","event_type":"ORDER_CREATED","order_id":"O456","customer_id":"C123","total_amount":1,"currency":"TRY"}`)
	_, err := ParseOrderEvent(payload)
	if err == nil {
		t.Fatalf("expected error")
	}
}
