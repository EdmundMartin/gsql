package gopherql

import (
	"bytes"
	"testing"
)

func TestNewPageObject(t *testing.T) {

	key := []byte("EdmundMartin")
	val := []byte("MartinEdmund")

	obj := NewPageObject(key, val, 2, 0)

	contents := obj.Bytes()

	_, objFromBytes := PageObjectFromBytes(contents)

	if bytes.Compare(key, objFromBytes.Key) != 0 {
		t.Errorf("unexpected key value: %s", objFromBytes.Key)
	}

	if bytes.Compare(val, objFromBytes.Value) != 0 {
		t.Errorf("unexpected value: %s", objFromBytes.Value)
	}

	if objFromBytes.TransactionID != 2 {
		t.Errorf("unexpected transaction id, expected 2, got: %d", objFromBytes.TransactionID)
	}
}

func TestNewPage(t *testing.T) {

	page := NewPage(0, defaultPgSize)

	key := []byte("EdmundMartin")
	val := []byte("MartinEdmund")
	newVal := []byte("MartinEJ")

	obj := NewPageObject(key, val, 2, 0)

	err := page.Add(obj)
	if err != nil {
		t.Errorf("got unexpected error")
	}

	retObj := page.Get(key, 2)
	if retObj == nil || bytes.Compare(retObj.Value, val) != 0 {
		t.Errorf("did not get expected val: %s", val)
	}

	err = page.Replace(key, 2, newVal)
	if err != nil {
		t.Errorf("got unexpected err: %s", err)
	}

	retObj = page.Get(key, 2)

	if retObj == nil || bytes.Compare(retObj.Value, newVal) != 0 {
		t.Errorf("di not get expected val: %s", val)
	}
}
