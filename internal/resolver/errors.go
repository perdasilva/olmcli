package resolver

import "fmt"

type RetryableError string

func (v RetryableError) Error() string {
	return string(v)
}

type FatalError string

func (v FatalError) Error() string {
	return string(v)
}

type ConflictError FatalError

func (v ConflictError) Error() string {
	return v.Error()
}

type NotFoundError RetryableError

func (v NotFoundError) Error() string {
	return fmt.Sprintf("variable with id %s not found", string(v))
}

type PreconditionError RetryableError

func (v PreconditionError) Error() string {
	return fmt.Sprintf("precondition failed: %s", string(v))
}

func IsConflictError(err error) bool {
	_, ok := err.(ConflictError)
	return ok
}

func IsPreconditionError(err error) bool {
	_, ok := err.(PreconditionError)
	return ok
}

func IsNotFoundError(err error) bool {
	_, ok := err.(NotFoundError)
	return ok
}

func IgnoreNotFound(err error) error {
	if IsNotFoundError(err) {
		return nil
	}
	return err
}

func IsRetryableError(err error) bool {
	_, ok := err.(RetryableError)
	return ok
}

func IsFatalError(err error) bool {
	_, ok := err.(FatalError)
	return ok
}
