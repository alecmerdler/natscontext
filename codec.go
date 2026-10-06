package natscontext

import "encoding/json"

// Codec encodes and decodes values stored in the NATS KV bucket.
//
// The default JSONCodec decodes into generic Go types (map[string]any,
// []any, float64, etc.) since the concrete type information is not
// preserved across the network. Provide a custom Codec (e.g. gob with
// registered types) if typed round-tripping is required.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

// JSONCodec is the default Codec, backed by encoding/json.
type JSONCodec struct{}

func (JSONCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
