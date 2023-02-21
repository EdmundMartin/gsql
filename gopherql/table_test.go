package gopherql

import (
	"testing"
)

func TestColumn_ByteTransformation(t *testing.T) {

	c := &Column{
		Name:    "PrimaryKey",
		Type:    StringColumn,
		NotNull: false,
	}

	result := c.Bytes()

	col, err := ColumnFromBytes(result)

	if err != nil {
		t.Error("unexpected error")
	}

	if col.Name != c.Name {
		t.Error("names do not match")
	}

	if col.Type != c.Type {
		t.Error("types do not match")
	}

	if col.NotNull != c.NotNull {
		t.Error("not null does not match")
	}
}

func TestColumns_ByteTransformation(t *testing.T) {

	c := []*Column{
		{
			"PrimaryKey",
			Int64Column,
			true,
		},
		{
			"ValueField",
			StringColumn,
			false,
		},
	}

	result := Columns(c).Bytes()

	cols, err := ColumnsFromBytes(result)

	if err != nil {
		t.Error("unexpected error")
	}

	if cols[0].Name != c[0].Name {
		t.Error("unexpected name")
	}
	if cols[1].Name != c[1].Name {
		t.Error("unexpected name")
	}

	if cols[0].Type != c[0].Type {
		t.Error("unexpected type")
	}

	if cols[1].Type != c[1].Type {
		t.Error("unexpected type")
	}

	if cols[0].NotNull != c[0].NotNull {
		t.Error("unexpected null value")
	}

	if cols[1].NotNull != c[1].NotNull {
		t.Error("unexpected null value")
	}
}

func TestPrimaryKeys_BytesTransformation(t *testing.T) {

	p := []string{
		"Hello",
		"Goodbye",
		"Why",
	}

	result := PrimaryKeys(p).Bytes()
	pks := PrimaryKeysFromBytes(result)

	if len(p) != len(pks) {
		t.Error("problem marshalling/unmarshalling")
	}

	for idx := 0; idx < len(pks); idx++ {
		if p[idx] != pks[idx] {
			t.Errorf("unexpected value at pos: %d, got: %s, expected: %s", idx, pks[idx], p[idx])
		}
	}
}

func TestTable_Bytes(t *testing.T) {

	c := []*Column{
		{
			Name:    "PK",
			Type:    StringColumn,
			NotNull: true,
		},
		{
			Name:    "Value",
			Type:    Int64Column,
			NotNull: true,
		},
	}

	table := &Table{
		Name:        "ExampleTable",
		Columns:     c,
		PrimaryKeys: []string{"PK"},
		Virtual:     false,
	}

	result := table.Bytes()

	loadedTable, err := TableFromBytes(result)
	if err != nil {
		t.Error("unexpected error")
	}

	if loadedTable.Name != table.Name {
		t.Error("unexpected table name")
	}

	if len(table.Columns) != len(loadedTable.Columns) {
		t.Error("unexpected column count")
	}
}
