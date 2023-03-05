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

	if bt.Pager.TotalPages() == 0 {
		return []int{}
	}

	if bytes.Compare(old.Key, new.Key) == 0 {
		// TODO - Update single page
		return []int{}
	}
	return []int{}
}

func (bt Btree) Remove(key []byte, transID int, handleBlob bool) error {

	path, _, err := bt.SearchPage(key)
	if err != nil {
		return err
	}
	pageNumber := path[len(path)-1]
	emptyPages := []int{}

	page, err := bt.Pager.FetchPage(pageNumber)
	if err != nil {
		return err
	}

	objToDelete := page.Get(key, transID)

	if handleBlob && objToDelete.IsBlobRef {

		blobPieces, hasFrag := objToDelete.BlobInfo()

		for part := 0; part < blobPieces; part++ {
			if err := bt.Remove(blobObjectKey(key, uint32(part)), transID, false); err != nil {
				return err
			}
		}

		if hasFrag {
			// TODO - is this correct?
			if err := bt.Remove(newBlobFragmentKey(key), transID, false); err != nil {
				return err
			}
		}

		return bt.Remove(key, transID, false)
	}

	page.Delete(key, transID)
	if err := bt.Pager.StorePage(pageNumber, page); err != nil {
		return err
	}

	if page.IsEmpty() {
		if pageNumber == bt.Pager.GetRootPage() {
			return bt.Pager.TruncateAll()
		}

		emptyPages = append(emptyPages, pageNumber)
	}

	if len(path) > 1 {
		for pathIdx := len(path) - 2; pathIdx >= 0; pathIdx-- {
			t, err := bt.Pager.FetchPage(path[pathIdx])
			if err != nil {
				return err
			}
			didDelete := t.Delete(key, 0)

			lowerPage, err := bt.Pager.FetchPage(path[pathIdx+1])
			if err != nil {
				return err
			}
			if !lowerPage.IsEmpty() && didDelete {
				lowerBound := lowerPage.Head().Key

				buf := NewByteWriter()
				buf.WriteUint32(path[pathIdx+1])
				buf.WriteBytes(make([]byte, defaultPgSize-uint32Size))

				obj := NewPageObject(lowerBound[:], buf.Bytes(), 0, 0)

				if err = t.Add(obj); err != nil {
					return err
				}
			}

			err = bt.Pager.StorePage(path[pathIdx], t)
			if err != nil {
				return err
			}

			if t.IsEmpty() {
				if path[pathIdx] == bt.Pager.GetRootPage() {
					return bt.Pager.TruncateAll()
				}

				emptyPages = append(emptyPages, path[pathIdx])
			}

		}
	}

	//TODO - Fill empty pages

	return nil
}

func (bt Btree) Expire(key []byte, transID, delID int) (int, error) {

	path, _, err := bt.SearchPage(key)
	if err != nil {
		return -1, err
	}
	pageNumber := path[len(path)-1]

	page, err := bt.Pager.FetchPage(pageNumber)
	if err != nil {
		return -1, err
	}

	if page.Expire(key, transID, delID) {
		err = bt.Pager.StorePage(pageNumber, page)
		if err != nil {
			return pageNumber, err
		}
	}
	return -1, nil
}
