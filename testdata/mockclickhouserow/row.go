package mockclickhouserow

type Row struct {
	ScanFn       func(dest ...any) error
	ErrFn        func() error
	ScanStructFn func(dest any) error
}

func (r Row) Scan(dest ...any) error {
	return r.ScanFn(dest...)
}

func (r Row) Err() error {
	if r.ErrFn == nil {
		return nil
	}
	return r.ErrFn()
}

func (r Row) ScanStruct(dest any) error {
	if r.ScanStructFn == nil {
		return nil
	}
	return r.ScanStructFn(dest)
}
