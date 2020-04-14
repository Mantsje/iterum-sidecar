package data

// LocalFileDesc is a description of an iterum data file downloaded and stored somewhere on the local volume
type LocalFileDesc struct {
	Name      string `json:"name"` // Original name as stored in the idv repository
	LocalPath string `json:"path"` // Local path to file
}
