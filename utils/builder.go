package utils

import (
	"fmt"
	"reflect"
)


// FieldNames get struct field name with tag
func FieldNames(in interface{}, tag string) []string {
	out := make([]string, 0)
	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		panic(fmt.Errorf("ToMap only accepts structs; got %T", v))
	}

	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		fi := typ.Field(i)
		if tagv := fi.Tag.Get(tag); tagv != "" {
			out = append(out, fmt.Sprintf("`%s`", tagv))
		} else {
			out = append(out, fmt.Sprintf(`"%s"`, fi.Name))
		}
	}

	return out
}


// RemoveStrs ...
func RemoveStrs(strings []string, strs ...string) []string {
	out := append([]string(nil), strings...)

	for _, str := range strs {
		var n int
		for _, v := range out {
			if v != str {
				out[n] = v
				n++
			}
		}
		out = out[:n]
	}

	return out
}