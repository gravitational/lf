package lf

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/gravitational/trace"
)

type Offset struct {
	SchemaVersion int
	RecordID      uint64
}

// String returns string version of the offset
func (o Offset) String() string {
	values := make(url.Values)
	values.Set("v", fmt.Sprintf("%v", o.SchemaVersion))
	values.Set("rid", fmt.Sprintf("%v", o.RecordID))
	u := url.URL{
		Scheme:   componentLogFormat,
		RawQuery: values.Encode(),
	}
	return u.String()
}

// ParseOffset parses offset from URL string
func ParseOffset(offset string) (*Offset, error) {
	u, err := url.Parse(offset)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	values := u.Query()

	schemaString := values.Get("v")
	schema, err := strconv.ParseInt(schemaString, 10, 32)
	if err != nil {
		return nil, trace.BadParameter("failed to parse schema: %q", schemaString)
	}
	if schema != V1 {
		return nil, trace.BadParameter("schema version %q is not supported", schema)
	}

	recordIDString := values.Get("rid")
	recordID, err := strconv.ParseUint(recordIDString, 10, 64)
	if err != nil {
		return nil, trace.BadParameter("failed to parse process id: %q", recordIDString)
	}

	return &Offset{
		SchemaVersion: int(schema),
		RecordID:      recordID,
	}, nil
}
