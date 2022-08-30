package utils

import (
	"bytes"
	"encoding/binary"

	"github.com/vmihailenco/msgpack/v5"
)

// func EncodeKey(key interface{}) (res []byte, err error) {
// 	switch key.(type) {
// 	case []byte:
// 		return key.([]byte), nil
// 	case bool, float32, float64, complex64, complex128, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
// 		buf := new(bytes.Buffer)
// 		err = binary.Write(buf, binary.LittleEndian, key)
// 		return buf.Bytes(), nil
// 		//

// 	case int:
// 		val := uint64(key.(int))
// 		buf := new(bytes.Buffer)
// 		binary.Write(buf, binary.LittleEndian, val)
// 		return buf.Bytes(), nil
// 	case string:
// 		return []byte(key.(string)), nil
// 	default:
// 		res, err = msgpack.Marshal(key)
// 		return
// 	}
// }

// func EncodeValue(value interface{}) (res []byte, err error) {
// 	var str string
// 	switch value.(type) {
// 	case int:
// 		temp := value.(int)
// 		str = strconv.FormatInt(int64(temp), 10)
// 	case int8:
// 		temp := value.(int8)
// 		str = strconv.FormatInt(int64(temp), 10)
// 	case int16:
// 		temp := value.(int16)
// 		str = strconv.FormatInt(int64(temp), 10)
// 	case int32:
// 		temp := value.(int32)
// 		str = strconv.FormatInt(int64(temp), 10)
// 	case int64:
// 		temp := value.(int64)
// 		str = strconv.FormatInt(temp, 10)
// 	case uint:
// 		temp := value.(uint)
// 		str = strconv.FormatUint(uint64(temp), 10)
// 	case uint8:
// 		temp := value.(uint8)
// 		str = strconv.FormatUint(uint64(temp), 10)
// 	case uint16:
// 		temp := value.(uint16)
// 		str = strconv.FormatUint(uint64(temp), 10)
// 	case uint32:
// 		temp := value.(uint32)
// 		str = strconv.FormatUint(uint64(temp), 10)
// 	case uint64:
// 		temp := value.(uint64)
// 		str = strconv.FormatUint(uint64(temp), 10)
// 	case bool:
// 		temp := value.(bool)
// 		str = strconv.FormatBool(temp)
// 	case float32:
// 		temp := value.(float32)
// 		str = strconv.FormatFloat(float64(temp), 'f', -1, 64)
// 	case float64:
// 		temp := value.(float64)
// 		str = strconv.FormatFloat(temp, 'f', -1, 64)
// 	case string:
// 		str = value.(string)
// 	case []byte:
// 		return value.([]byte), nil
// 	default:
// 		res, err = msgpack.Marshal(value)
// 		return
// 	}

// 	return []byte(str), nil
// 	// switch value.(type) {
// 	// case []byte:
// 	// 	return value.([]byte), nil
// 	// case string:
// 	// 	return []byte(value.(string)), nil
// 	// default:
// 	// 	res, err = msgpack.Marshal(value)
// 	// 	return
// 	// }
// }

// func DecodeValue(value []byte, dest interface{}) (err error) {
// 	switch dest.(type) {
// 	case *[]byte:
// 		*dest.(*[]byte) = value

// 	case *string:
// 		*dest.(*string) = string(value)
// 	default:
// 		err := msgpack.Unmarshal(value, dest)
// 		return err
// 	}
// 	return
// }

// EncodeKey returns key in bytes.
func EncodeKey(key interface{}) (res []byte, err error) {
	switch key.(type) {
	case []byte:
		return key.([]byte), nil
	case bool, float32, float64, complex64, complex128, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		buf := new(bytes.Buffer)
		err = binary.Write(buf, binary.BigEndian, key)
		return buf.Bytes(), err
	case int:
		val := uint64(key.(int))
		p := make([]byte, 8)
		p[0] = byte(val >> 56)
		p[1] = byte(val >> 48)
		p[2] = byte(val >> 40)
		p[3] = byte(val >> 32)
		p[4] = byte(val >> 24)
		p[5] = byte(val >> 16)
		p[6] = byte(val >> 8)
		p[7] = byte(val)
		return p, err
	case string:
		return []byte(key.(string)), nil
	default:
		res, err = msgpack.Marshal(key)
		return
	}
}

// EncodeValue returns value in bytes.
func EncodeValue(value interface{}) (res []byte, err error) {
	switch value.(type) {
	case []byte:
		return value.([]byte), nil
	case string:
		return []byte(value.(string)), err
	default:
		res, err = msgpack.Marshal(value)
		return
	}
}

// DecodeValue decode value to dest.
func DecodeValue(value []byte, dest interface{}) (err error) {
	switch dest.(type) {
	case *[]byte:
		*dest.(*[]byte) = value
	case *string:
		*dest.(*string) = string(value)
	default:
		err = msgpack.Unmarshal(value, dest)
		return
	}
	return
}
