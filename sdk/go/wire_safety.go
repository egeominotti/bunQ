package bunqueue

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type jsSafeVisit struct {
	kind reflect.Kind
	typ  reflect.Type
	ptr  uintptr
}

var timeType = reflect.TypeOf(time.Time{})

// jsSafe is the BigInt-killer guard. It also rejects cyclic graphs and maps
// with non-string keys, neither of which the JavaScript wire object can
// represent safely.
func jsSafe(value any) (any, error) {
	return jsSafeValue(reflect.ValueOf(value), map[jsSafeVisit]bool{})
}

func jsSafeValue(value reflect.Value, visiting map[jsSafeVisit]bool) (any, error) {
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil, nil
		}
		return jsSafeValue(value.Elem(), visiting)
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		release, err := enterJsSafe(value, visiting)
		if err != nil {
			return nil, err
		}
		defer release()
		return jsSafeValue(value.Elem(), visiting)
	}
	if value.Type() == timeType {
		if !value.CanInterface() {
			return nil, fmt.Errorf("cannot encode inaccessible time.Time")
		}
		return float64(value.Interface().(time.Time).UnixMilli()), nil
	}
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return jsSafeInt64(value.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return jsSafeUint64(value.Uint()), nil
	case reflect.Map:
		return jsSafeMap(value, visiting)
	case reflect.Slice:
		if value.IsNil() {
			return nil, nil
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			if !value.CanInterface() {
				return nil, fmt.Errorf("cannot encode inaccessible byte slice")
			}
			return value.Interface(), nil
		}
		release, err := enterJsSafe(value, visiting)
		if err != nil {
			return nil, err
		}
		defer release()
		return jsSafeSequence(value, visiting)
	case reflect.Array:
		return jsSafeSequence(value, visiting)
	case reflect.Struct:
		return jsSafeStruct(value, visiting)
	default:
		if !value.CanInterface() {
			return nil, fmt.Errorf("cannot encode inaccessible %s", value.Type())
		}
		return value.Interface(), nil
	}
}

func jsSafeMap(value reflect.Value, visiting map[jsSafeVisit]bool) (any, error) {
	if value.IsNil() {
		return nil, nil
	}
	if value.Type().Key().Kind() != reflect.String {
		return nil, fmt.Errorf("map key type %s is unsupported; wire maps require strings", value.Type().Key())
	}
	release, err := enterJsSafe(value, visiting)
	if err != nil {
		return nil, err
	}
	defer release()
	out := make(map[string]any, value.Len())
	iter := value.MapRange()
	for iter.Next() {
		safe, err := jsSafeValue(iter.Value(), visiting)
		if err != nil {
			return nil, err
		}
		out[iter.Key().String()] = safe
	}
	return out, nil
}

func jsSafeSequence(value reflect.Value, visiting map[jsSafeVisit]bool) ([]any, error) {
	out := make([]any, value.Len())
	for i := range value.Len() {
		safe, err := jsSafeValue(value.Index(i), visiting)
		if err != nil {
			return nil, err
		}
		out[i] = safe
	}
	return out, nil
}

func jsSafeStruct(value reflect.Value, visiting map[jsSafeVisit]bool) (map[string]any, error) {
	out := make(map[string]any, value.NumField())
	typ := value.Type()
	for i := range value.NumField() {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := strings.Split(field.Tag.Get("msgpack"), ",")
		if tag[0] == "-" {
			continue
		}
		name := field.Name
		if tag[0] != "" {
			name = tag[0]
		}
		if slicesContain(tag[1:], "omitempty") && value.Field(i).IsZero() {
			continue
		}
		safe, err := jsSafeValue(value.Field(i), visiting)
		if err != nil {
			return nil, err
		}
		if slicesContain(tag[1:], "inline") {
			if inline, ok := safe.(map[string]any); ok {
				for key, item := range inline {
					out[key] = item
				}
				continue
			}
		}
		out[name] = safe
	}
	return out, nil
}

func enterJsSafe(value reflect.Value, visiting map[jsSafeVisit]bool) (func(), error) {
	visit := jsSafeVisit{kind: value.Kind(), typ: value.Type(), ptr: value.Pointer()}
	if visiting[visit] {
		return nil, fmt.Errorf("cyclic value of type %s is unsupported", value.Type())
	}
	visiting[visit] = true
	return func() { delete(visiting, visit) }, nil
}

func slicesContain(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func jsSafeInt64(value int64) any {
	if value < int32Min || value > int32Max {
		return float64(value)
	}
	return value
}

func jsSafeUint64(value uint64) any {
	if value > int32Max {
		return float64(value)
	}
	return value
}
