package gopherql

import "bytes"

type Btree struct {
	PageSize int
	Pager    Pager
}

func NewBTree(pager Pager) *Btree {
	return &Btree{
		PageSize: defaultPgSize,
		Pager:    pager,
	}
}

func (bt Btree) SearchPage(key []byte) ([]int, []int, error) {

	if bt.Pager.TotalPages() == 0 {
		return []int{}, []int{}, nil
	}

	path := []int{}
	depthIterator := []int{}
	currentPage := bt.Pager.GetRootPage()

	for {
		path = append(path, currentPage)
		page, err := bt.Pager.FetchPage(currentPage)
		if err != nil {
			return nil, nil, err
		}

		if page.Kind == kindLeaf {
			break
		}

		objects := page.Objects()
		depthIterator = append(depthIterator, len(objects)-1)

		found := false
		for depthIterator[len(depthIterator)-1] >= 0 {
			if bytes.Compare(key, objects[depthIterator[len(depthIterator)-1]].Key) >= 0 {
				buf := NewByteReader(objects[depthIterator[len(depthIterator)-1]].Value)
				currentPage = buf.ReadUint32()
				found = true
				break
			}
			depthIterator[len(depthIterator)-1]--
		}

		if !found {
			depthIterator[len(depthIterator)-1] = 0
			buf := NewByteReader(objects[0].Value)
			currentPage = buf.ReadUint32()
		}
	}

	return path, depthIterator, nil
}

func (bt Btree) Update(old, new *PageObject, transID int) []int {
	return []int{}
}
