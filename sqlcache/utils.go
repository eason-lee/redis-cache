package sqlcache

import (
	"log"
	"strings"
)

// 数据库信息的脱敏
func desensitize(datasource string) string {
	// remove account
	pos := strings.LastIndex(datasource, "@")
	if 0 <= pos && pos+1 < len(datasource) {
		datasource = datasource[pos+1:]
	}

	return datasource
}

func logInstanceError(datasource string, err error) {
	datasource = desensitize(datasource)
	log.Printf("Error on getting sql instance of %s: %v", datasource, err)
}
