package internal

import (
	"fmt"
	"strconv"
)

func ToString(in any) string {
	return fmt.Sprintf("%v", in)
}

func ParseInt64(s string) (int64, error) {
	if i, err := strconv.ParseInt(s, 10, 64); err != nil {
		return 0, fmt.Errorf("unable to parse int: %v", err)
	} else {
		return i, nil
	}
}

func GetBoolean(s string) bool {
	if parseBool, err := strconv.ParseBool(s); err != nil {
		return false
	} else {
		return parseBool
	}
}

func GetSliceWithout(exception int64, inputData []int64) []int64 {
	ret := []int64{}
	for _, v := range inputData {
		if v != exception {
			ret = append(ret, v)
		}
	}
	return ret
}
