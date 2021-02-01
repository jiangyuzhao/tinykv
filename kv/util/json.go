package util

import (
	"encoding/json"
	"fmt"
)

func JsonExpr(data interface{}) string {
	if data == nil {
		return ""
	}
	expr, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return fmt.Sprintf("%+v", data)
	}
	return string(expr)
}
