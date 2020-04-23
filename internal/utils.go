package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Exists tell path exists
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// GetDatafiles get *.data files from path
func GetDatafiles(path string) ([]string, error) {
	fns, err := filepath.Glob(fmt.Sprintf("%s/*.data", path))
	if err != nil {
		return nil, err
	}
	sort.Strings(fns)
	return fns, nil
}

// ParseIds return int filenames
func ParseIds(fns []string) ([]int, error) {
	ids := make([]int, len(fns))
	for _, fn := range fns {
		base := filepath.Base(fn)
		ext := filepath.Ext(fn)
		id, err := strconv.ParseInt(strings.TrimSuffix(base, ext), 10, 64)
		if err != nil {
			return nil, err
		}
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	return ids, nil
}
