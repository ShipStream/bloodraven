package sidecar

func coverageBoundToEntry(entry ManifestEntry) bool {
	id := entry.ID
	if id == "" {
		id = entry.ContentID
	}
	return id != "" && entry.GTIDCoverage != nil && entry.GTIDCoverage.ContentID == id
}
