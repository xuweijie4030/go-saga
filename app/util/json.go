package util

import "encoding/json"

func JsonMarshalToStr(param interface{}) (result string, err error) {
	b, err := json.Marshal(param)
	if err != nil {
		return result, err
	}
	result = string(b)

	return result, nil
}
