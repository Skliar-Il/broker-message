package envelope

import (
	"testing"

	"github.com/google/uuid"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	idem := uuid.Must(uuid.NewV7())
	env := NewPublish(idem, []byte("hello"))
	env = env.WithServerMsgID(uuid.Must(uuid.NewV7()))

	raw := env.Encode()
	got, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.IdempotencyID != env.IdempotencyID {
		t.Fatalf("idempotency: %v != %v", got.IdempotencyID, env.IdempotencyID)
	}
	if got.ServerMsgID != env.ServerMsgID {
		t.Fatalf("server_msg: %v != %v", got.ServerMsgID, env.ServerMsgID)
	}
	if string(got.Payload) != "hello" {
		t.Fatalf("payload: %q", got.Payload)
	}
}

func TestTryDecodeRejectsPlainPayload(t *testing.T) {
	_, ok := TryDecode([]byte("not an envelope"))
	if ok {
		t.Fatal("expected false for plain payload")
	}
}
