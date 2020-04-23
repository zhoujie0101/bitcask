package internal

type Item struct {
	FileID int   `json: fileID`
	Offset int64 `json: offset`
	Size   int64 `json: size`
}
