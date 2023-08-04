package common

import (
	"errors"
	"fmt"
)

var (
	ErrFieldNotFound = errors.New("datasource table field not found")
	ErrNotAvailable  = errors.New("the method is not available")
)

func WrapErrorWithMessage(message string, err error) error {
	return fmt.Errorf("%s: %w", message, err)
}
