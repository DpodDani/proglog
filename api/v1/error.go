package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	// build new error status (and code)
	st := status.New(
		404,
		fmt.Sprintf("offset out of range: %d", e.Offset),
	)

	msg := fmt.Sprintf(
		"The requested offset is outside the log's range: %d",
		e.Offset,
	)

	// errdetails package provides some useful protobufs
	// eg. messages to use to handle bad requests, debug info,
	// localised messages etc.
	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}

	// attach error details to the status
	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}

	return std
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
