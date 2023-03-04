package gopherql

import (
	"os"
	"testing"
)

func TestHeader_Bytes(t *testing.T) {

	h := NewHeader()

	result := h.Bytes()

	otherH := HeaderFromBytes(result)

	if h.TransactionID != otherH.TransactionID {
		t.Error("expected matching transaction ids")
	}

	if h.PageSize != otherH.PageSize {
		t.Error("expected matching page sizes")
	}
}

func TestNewDatabaseFile(t *testing.T) {

	fileName := "test_db"
	defer deleteFile(fileName)
	err := NewDatabaseFile(fileName)
	if err != nil {
		t.Error("unexpected error writing to disk")
	}
}

func TestReadHeader(t *testing.T) {
	fileName := "other_test_db"
	defer deleteFile(fileName)

	err := NewDatabaseFile(fileName)
	if err != nil {
		t.Error("unexpected error during setup")
	}

	f, err := os.Open(fileName)
	if err != nil {
		t.Error("unexpected error during setup")
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Error("unexpected error during read")
	}

	if h.TransactionID != 2 {
		t.Error("unexpected transaction error")
	}

	if h.PageSize != defaultPgSize {
		t.Error("unexpected page size")
	}
}

func deleteFile(filePath string) {
	os.Remove(filePath)
}
