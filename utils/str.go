package utils

import "strconv"

// f is format type; is is 如果prec 为-1，则代表使用最少数量的、但又必需的数字来表示f。 
func Float64ToStr(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

func StrToFloat64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}
