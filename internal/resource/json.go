package resource

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

// Duration is a JSON string accepted by time.ParseDuration, such as "60s".
type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

var (
	idPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,127}$`)
	actionPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
	digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

func checkID(field, v string) error {
	if !idPattern.MatchString(v) {
		return fmt.Errorf("resource: %s %q is not a valid id", field, v)
	}
	return nil
}

// Encode serializes v as JSON. Types in this package reject unknown fields
// when read back with Decode.
func Encode(v any) ([]byte, error) {
	if v == nil {
		return nil, errors.New("resource: encode nil")
	}
	buf, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, errors.New("resource: empty JSON")
	}
	return buf, nil
}

// Decode reads one JSON value into dst and rejects unknown fields and
// trailing values. It does not validate the value; call Validate.
func Decode(b []byte, dst any) error {
	if dst == nil {
		return errors.New("resource: decode nil")
	}
	return decodeStrict(b, dst)
}

// decodeStrict is the inner decoder. UnmarshalJSON methods call this on an
// alias type so they do not recurse through Decode.
func decodeStrict(b []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("resource: trailing JSON value")
		}
		return fmt.Errorf("resource: trailing JSON: %w", err)
	}
	return nil
}
