package util

import (
	"errors"
	"fmt"
	"go_code/project13/rosedb"
	"strconv"
)

var (
	ErrSyntax         = errors.New("(error) ERR Syntax error")
	ErrWrongArgsOfNum = errors.New("(error) wrong number of arguments")
	ErrExpire         = errors.New("(error) ERR value is not an integer or out of range")
	ErrCmd            = errors.New("(error) I'm sorry, I don't recognize that command. Please type HELP for one of these commands")
)

func CheckArgs(cmd string, args []string) (ok bool, err error) {
	switch cmd {
	case "SET":
		if len(args) != 2 {
			return false, ErrWrongArgsOfNum
		}
		return true, nil
	case "SETEX":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}

		if _, err := strconv.ParseFloat(args[2], 64); err != nil {
			return false, err
		}
		return true, nil
	case "SETNX":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "GET":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "GETSET":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "MSET":
		if (len(args)%2 - 1) != 0 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + " for 'mset' command")
		}
		return true, nil
	case "MGET":
		if len(args) == 0 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + " for 'mget' command")
		}
		return true, nil
	case "APPEND":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "STREXISTS":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "PREFIXSCAN":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		if _, err := strconv.ParseInt(args[2], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "RANGESCAN":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "EXPIRE":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "PERSIST":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "TTL":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "HSET":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		return true, nil
	case "HSETNX":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		return true, nil
	case "HGET":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "HGETALL":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "HMSET":
		if (len(args)%2 - 1) != 0 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + " for 'hmset' command")
		}

		return true, nil
	case "HMGET":
		if len(args) <= 0 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + " for 'hmget' command")
		}
		return true, nil
	case "HDEL":
		if len(args) == 0 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + " for 'hdel' command")
		}
		return true, nil
	case "HKEYEXISTS":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "HEXISTS":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "HLEN":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}

		return true, nil
	case "HVALS":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "HKEYS":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "HCLEAR":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "HEXPIRE":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}

		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}

		return true, nil
	case "HTTL":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}

		return true, nil
	case "LPUSH":
		if len(args) < 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + "for lpush command")
		}

		return true, nil
	case "RPUSH":
		if len(args) < 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + "for rpush command")
		}

		return true, nil
	case "LPOP":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}

		return true, nil
	case "RPOP":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}

		return true, nil
	case "LINDEX":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "LREM":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseInt(args[2], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "LINSERT":
		if len(args) != 4 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 4)", len(args))
		}
		if args[3] != "after" && args[3] != "before" {
			return false, fmt.Errorf("insert option err for linsert command")
		}
		return true, nil
	case "LSET":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseInt(args[2], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "LTRIM":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}

		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		if _, err := strconv.ParseInt(args[2], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "LRANGE":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}

		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		if _, err := strconv.ParseInt(args[2], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "LLEN":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "LKEYEXISTS":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "LVALEXISTS":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "LCLEAR":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "LEXPIRE":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "LTTL":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "SADD":
		if len(args) < 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + "for sadd command")
		}
		return true, nil
	case "SPOP":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "SISMEMBER":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}

		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "SRANDMEMBER":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "SREM":
		if len(args) < 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + "for srem command")
		}
		return true, nil
	case "SMOVE":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}

		return true, nil
	case "SCARD":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}

		return true, nil
	case "SMEMBERS":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}

		return true, nil
	case "SDIFF":
		if len(args) == 0 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error() + "for sdiff command")
		}
		return true, nil
	case "SKEYEXISTS":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "SCLEAR":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "SEXPIRE":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "STTL":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "ZADD":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseFloat(args[2], 64); err != nil {
			return false, err
		}
		return true, nil
	case "ZSCORE":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "ZRANK":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "ZREVRANK":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "ZCARD":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "ZINCRBY":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseFloat(args[2], 32); err != nil {
			return false, err
		}

		return true, nil
	case "ZRANGE":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		if _, err := strconv.ParseInt(args[2], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "ZREVRANGE":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		if _, err := strconv.ParseInt(args[2], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "ZRANGEWITHSCORES":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		if _, err := strconv.ParseInt(args[2], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "ZREVRANGEWITHSCORES":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		if _, err := strconv.ParseInt(args[2], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "ZGETBYRANK":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}

		return true, nil
	case "ZREVGETBYRANK":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}

		return true, nil
	case "ZSCORERANGE":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseFloat(args[2], 32); err != nil {
			return false, err
		}
		if _, err := strconv.ParseFloat(args[1], 32); err != nil {
			return false, err
		}

		return true, nil
	case "ZREVSCORERANGE":
		if len(args) != 3 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 3)", len(args))
		}
		if _, err := strconv.ParseFloat(args[2], 32); err != nil {
			return false, err
		}
		if _, err := strconv.ParseFloat(args[1], 32); err != nil {
			return false, err
		}
		return true, nil
	case "ZREM":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		return true, nil
	case "ZKEYEXISTS":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "ZCLEAR":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}
		return true, nil
	case "ZEXPIRE":
		if len(args) != 2 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 2)", len(args))
		}
		if _, err := strconv.ParseInt(args[1], 10, 64); err != nil {
			return false, err
		}
		return true, nil
	case "ZTTL":
		if len(args) != 1 {
			return false, fmt.Errorf(rosedb.ErrWrongNumberArgs.Error()+"(given %d,expected 1)", len(args))
		}

		return true, nil
	}
	return false, ErrCmd

}
