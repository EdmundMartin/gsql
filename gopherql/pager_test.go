package gopherql

import (
	"fmt"
	"os"
	"testing"
)

func TestFilePager(t *testing.T) {
	dbFile := "filePagerTst.db"
	defer deleteFile(dbFile)

	err := NewDatabaseFile(dbFile)
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	fp, err := NewFilePager(file, 0, defaultPgSize)
	if err != nil {
		t.Fatal(err)
	}

	emptyPage := NewPage(0, defaultPgSize)

	next, err := fp.AppendPage(emptyPage)
	if err != nil {
		fmt.Println(err)
		t.Error("unexpected error storing page")
	}
	if next != 1 {
		t.Error("unexpected next")
	}

	otherPage := NewPage(0, defaultPgSize)

	otherPage.Add(NewBlobPageObject(
		[]byte("edmund"),
		[]byte("martin"),
		10,
		10,
		0,
	))
	fmt.Println(otherPage.Data)
	fmt.Println(len(otherPage.Data))

	next, err = fp.AppendPage(otherPage)
	if err != nil {
		fmt.Println(err)
		t.Error("unexpected error storing page")
	}
	if next != 2 {
		t.Error("unexpected next")
	}

	p, err := fp.FetchPage(2)
	if err != nil {
		t.Error("unexpected error fetching page")
	}
	if p.Data[3] != 33 {
		t.Error("unexpected first byte value")
	}
}
