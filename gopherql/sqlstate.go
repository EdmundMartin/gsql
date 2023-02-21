package gopherql

type SQLStateError struct {
	Code string
	Msg  string
}

func (s SQLStateError) Error() string {
	return s.Msg
}
