package main

import (
	"context"
	"fmt"
	"go_code/project13/rosedb"
	"go_code/project13/rosedb/cmd/cli/service"
	"go_code/project13/rosedb/cmd/cli/util"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/peterh/liner"
	"google.golang.org/grpc"
)

var commandList = [][]string{
	{"SET", "key value", "STRING"},
	{"GET", "key", "STRING"},
	{"SETNX", "key seconds value", "STRING"},
	{"SETEX", "key value", "STRING"},
	{"GETSET", "key value", "STRING"},
	{"MSET", "[key value...]", "STRING"},
	{"MGET", "[key...]", "STRING"},
	{"APPEND", "key value", "STRING"},
	{"STREXISTS", "key", "STRING"},
	{"REMOVE", "key", "STRING"},
	{"PREFIXSCAN", "prefix limit offset", "STRING"},
	{"RANGESCAN", "start end", "STRING"},
	{"EXPIRE", "key seconds", "STRING"},
	{"PERSIST", "key", "STRING"},
	{"TTL", "key", "STRING"},

	{"LPUSH", "key value [value...]", "LIST"},
	{"RPUSH", "key value [value...]", "LIST"},
	{"LPOP", "key", "LIST"},
	{"RPOP", "key", "LIST"},
	{"LINDEX", "key index", "LIST"},
	{"LREM", "key value count", "LIST"},
	{"LINSERT", "key element pivot BEFORE|AFTER ", "LIST"},
	{"LSET", "key index value", "LIST"},
	{"LTRIM", "key start end", "LIST"},
	{"LRANGE", "key start end", "LIST"},
	{"LLEN", "key", "LIST"},
	{"LKEYEXISTS", "key", "LIST"},
	{"LVALEXISTS", "key value", "LIST"},
	{"LClear", "key", "LIST"},
	{"LExpire", "key seconds", "LIST"},
	{"LTTL", "key", "LIST"},

	{"HSET", "key field value", "HASH"},
	{"HSETNX", "key field value", "HASH"},
	{"HGET", "key field", "HASH"},
	{"HMSET", "[key field...]", "HASH"},
	{"HMGET", "[key...]", "HASH"},
	{"HGETALL", "key", "HASH"},
	{"HDEL", "key field [field...]", "HASH"},
	{"HKEYEXISTS", "key", "HASH"},
	{"HEXISTS", "key field", "HASH"},
	{"HLEN", "key", "HASH"},
	{"HKEYS", "key", "HASH"},
	{"HVALS", "key", "HASH"},
	{"HCLEAR", "key", "HASH"},
	{"HEXPIRE", "key seconds", "HASH"},
	{"HTTL", "key", "HASH"},

	{"SADD", "key members [members...]", "SET"},
	{"SPOP", "key count", "SET"},
	{"SISMEMBER", "key member", "SET"},
	{"SRANDMEMBER", "key count", "SET"},
	{"SREM", "key members [members...]", "SET"},
	{"SMOVE", "src dst member", "SET"},
	{"SCARD", "key", "key", "SET"},
	{"SMEMBERS", "key", "SET"},
	{"SUNION", "key [key...]", "SET"},
	{"SDIFF", "key [key...]", "SET"},
	{"SKEYEXISTS", "key", "SET"},
	{"SCLEAR", "key", "SET"},
	{"SEXPIRE", "key seconds", "SET"},
	{"STTL", "key", "SET"},

	{"ZADD", "key score member", "ZSET"},
	{"ZSCORE", "key member", "ZSET"},
	{"ZCARD", "key", "ZSET"},
	{"ZRANK", "key member", "ZSET"},
	{"ZREVRANK", "key member", "ZSET"},
	{"ZINCRBY", "key increment member", "ZSET"},
	{"ZRANGE", "key start stop", "ZSET"},
	{"ZREVRANGE", "key start stop", "ZSET"},
	{"ZREM", "key member", "ZSET"},
	{"ZGETBYRANK", "key rank", "ZSET"},
	{"ZREVGETBYRANK", "key rank", "ZSET"},
	{"ZSCORERANGE", "key min max", "ZSET"},
	{"ZREVSCORERANGE", "key max min", "ZSET"},
	{"ZKEYEXISTS", "key", "ZSET"},
	{"ZCLEAR", "key", "ZSET"},
	{"ZEXPIRE", "key", "ZSET"},
	{"ZTTL", "key", "ZSET"},
}

var (
	history_fn = "/tmp/rosedb-cli"
)
var cli service.RosedbClient

func initClie(cnf *rosedb.Config) {
	//   cnf.GrpcAddr
	// test docker  -- >
	conn, err := grpc.Dial("192.168.0.4:11000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf(" grpc.Dial err:%v", err)
	}
	cli = service.NewRosedbClient(conn)
}

func main() {
	cnf := rosedb.DefaultConfig()
	addr := fmt.Sprintf("%s", cnf.GrpcAddr)
	initClie(cnf)

	line := liner.NewLiner()
	defer line.Close()
	// 设置 CTRL +c 是否管用
	line.SetCtrlCAborts(true)

	line.SetCompleter(func(li string) (res []string) {
		for _, c := range commandList {
			if strings.HasPrefix(c[0], strings.ToUpper(li)) {
				res = append(res, strings.ToLower(c[0]))
			}
		}
		return
	})
	if f, err := os.Open(history_fn); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	go func() {
		if f, err := os.Create(history_fn); err != nil {
			log.Print("Error writing history file: ", err)
		} else {
			line.WriteHistory(f)
			f.Close()
		}
	}()

	promt := addr + ">:"
	for {
		cmd, err := line.Prompt(promt)
		if err != nil {
			fmt.Printf(" line.Prompt err %v\n", err)
		}
		// parse  cmd
		cmd = strings.TrimSpace(cmd)
		if len(cmd) == 0 {
			continue
		}
		reqCmd, args := parseCommandLine(cmd)

		if reqCmd == "" && args == nil {
			continue
		}
		if reqCmd == "quit" || reqCmd == "exits" {
			break
		}
		line.AppendHistory(cmd)
		// create request struct & call
		reqAndCall(reqCmd, args)

	}
}

func parseCommandLine(cmd string) (string, []string) {
	cmdData := strings.Split(cmd, " ")
	if len(cmdData) == 0 {
		return "", nil
	}

	var args []string = make([]string, len(cmdData))

	for i := 0; i < len(cmdData); i++ {
		args[i] = cmdData[i]
	}

	return args[0], args[1:]
}

func reqAndCall(reqCmd string, args []string) {

	cmd := strings.ToUpper(reqCmd)
	ctx := context.Background()
	switch cmd {
	case "SET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		req := &service.SetReq{
			Key:   []byte(args[0]),
			Value: []byte(args[1]),
		}
		rsp, _ := cli.Set(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			break
		}
		fmt.Println("Ok")
	case "SETEX":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		duration, _ := strconv.ParseInt(args[2], 10, 64)
		req := &service.SetExReq{
			Key:      []byte(args[0]),
			Value:    []byte(args[1]),
			Duration: duration,
		}
		rsp, _ := cli.SetEx(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			break
		}
		fmt.Println("Ok")
	case "SETNX":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		req := &service.SetNxReq{
			Key:   []byte(args[0]),
			Value: []byte(args[1]),
		}
		rsp, _ := cli.SetNx(ctx, req)
		if rsp.ErrorMsg == "" {
			if rsp.Ok {
				fmt.Println("1")
			} else {
				fmt.Println("0")
			}
		} else {
			fmt.Println(rsp.ErrorMsg)
		}

	case "GET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.GetReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.Get(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(string(rsp.Dest))
	case "GETSET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.GetSetReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.GetSet(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(string(rsp.Dest))
	case "MSET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		keys := make([][]byte, 0)
		vals := make([][]byte, 0)
		for i := 0; i < len(args)-1; i += 2 {
			keys = append(keys, []byte(args[i]))
			vals = append(vals, []byte(args[i+1]))
		}
		req := &service.MSetReq{
			Key:    keys,
			Values: vals,
		}

		rsp, _ := cli.MSet(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "MGET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		keys := make([][]byte, 0)
		for _, arg := range args {
			keys = append(keys, []byte(arg))
		}

		req := &service.MGetReq{
			Keys: keys,
		}

		rsp, _ := cli.MGet(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		for _, v := range rsp.Values {
			if len(v) == 0 {
				fmt.Println("(nil)")
			} else {
				fmt.Println(string(v))
			}
		}
	case "APPEND":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.AppendReq{
			Key:   []byte(args[0]),
			Value: []byte(args[1]),
		}

		rsp, _ := cli.Append(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "STREXISTS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.StrExistsReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.StrExists(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Ok)
	case "PREFIXSCAN":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		limit, _ := strconv.Atoi(args[1])
		offset, _ := strconv.Atoi(args[2])
		req := &service.PrefixScanReq{
			Prefix: []byte(args[0]),
			Limit:  int64(limit),
			Offset: int64(offset),
		}

		rsp, _ := cli.PrefixScan(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		for _, v := range rsp.Values {
			fmt.Println(string(v))
		}
	case "RANGESCAN":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.RangeScanReq{
			Start: []byte(args[0]),
			End:   []byte(args[0]),
		}
		rsp, _ := cli.RangeScan(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		for _, v := range rsp.Values {
			fmt.Println(string(v))
		}
	case "REMOVE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.RemoveReq{
			Key: []byte(args[0]),
		}
		rsp, _ := cli.Remove(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "EXPIRE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		duration, _ := strconv.Atoi(args[1])
		req := &service.ExpireReq{
			Key:      []byte(args[0]),
			Duration: int64(duration),
		}
		rsp, _ := cli.Expire(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "PERSIST":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		req := &service.PersistReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.Persist(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "TTL":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		req := &service.TTLReq{
			Key: []byte(args[0]),
		}
		rsp, _ := cli.TTl(ctx, req)
		fmt.Println(rsp.Ttl)

	case "HSET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HSetReq{
			Key:   []byte(args[0]),
			Field: []byte(args[1]),
			Value: []byte(args[2]),
		}

		rsp, _ := cli.HSet(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Res)
	case "HSETNX":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HSetNxReq{
			Key:   []byte(args[0]),
			Field: []byte(args[1]),
			Value: []byte(args[2]),
		}

		rsp, _ := cli.HSetNx(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Res)
	case "HGET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HGetReq{
			Key:   []byte(args[0]),
			Field: []byte(args[1]),
		}

		rsp, _ := cli.HGet(ctx, req)

		fmt.Println(string(rsp.Value))
	case "HGETALL":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HGetAllReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.HGetAll(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		for _, v := range rsp.Values {
			fmt.Println(string(v))
		}
	case "HMSET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		filedsAndVal := make([][]byte, len(args)-1)
		for i := 1; i < len(args); i++ {
			filedsAndVal[i-1] = []byte(args[i])
		}

		req := &service.HMSetReq{
			Key:    []byte(args[0]),
			Values: filedsAndVal,
		}

		rsp, _ := cli.HMSet(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "HMGET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		fileds := make([][]byte, len(args)-1)
		for i := 1; i < len(args); i++ {
			fileds[i-1] = []byte(args[i])
		}

		req := &service.HMGetReq{
			Key:    []byte(args[0]),
			Fileds: fileds,
		}

		rsp, _ := cli.HMGet(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		for _, v := range rsp.Values {
			if len(v) == 0 {
				fmt.Println("(nil)")
			} else {
				fmt.Println(string(v))
			}
		}
	case "HDEL":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		fileds := make([][]byte, len(args)-1)
		for i := 1; i < len(args); i++ {
			fileds[i-1] = []byte(args[i])
		}

		req := &service.HDelReq{
			Key:    []byte(args[0]),
			Fileds: fileds,
		}

		rsp, _ := cli.HDel(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		fmt.Println(rsp.Res)
	case "HKEYEXISTS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HKeyExistsReq{
			Key: []byte(args[0]),
		}
		rsp, _ := cli.HKeyExists(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		fmt.Println(rsp.Ok)
	case "HEXISTS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HExistsReq{
			Key:   []byte(args[0]),
			Filed: []byte(args[1]),
		}
		rsp, _ := cli.HExists(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		fmt.Println(rsp.Ok)
	case "HLEN":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HLenReq{
			Key: []byte(args[0]),
		}
		rsp, _ := cli.HLen(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		fmt.Println(rsp.Length)
	case "HVALS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HValsReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.HVals(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println(("nil"))
		}

	case "HKEYS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HKeysReq{
			Key: []byte(args[0]),
		}
		rsp, _ := cli.HKeys(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}

	case "HCLEAR":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HClearReq{
			Key: []byte(args[0]),
		}
		rsp, _ := cli.HClear(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "HEXPIRE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		duration, _ := strconv.ParseInt(args[1], 10, 64)

		req := &service.HExpireReq{
			Key:      []byte(args[0]),
			Duration: duration,
		}

		rsp, _ := cli.HExpire(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "HTTL":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.HTTLReq{
			Key: []byte(args[0]),
		}
		rsp, _ := cli.HTTL(ctx, req)
		fmt.Println(rsp.Ttl)
	case "LPUSH":
		fmt.Println(args)
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		vals := make([][]byte, len(args)-1)
		for i := 1; i < len(args); i++ {
			vals[i-1] = []byte(args[i])
		}
		req := &service.LPushReq{
			Key:    []byte(args[0]),
			Values: vals,
		}

		rsp, _ := cli.LPush(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Res)
	case "RPUSH":

		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		vals := make([][]byte, len(args)-1)
		for i := 1; i < len(args); i++ {
			vals[i-1] = []byte(args[i])
		}
		req := &service.RPushReq{
			Key:    []byte(args[0]),
			Values: vals,
		}

		rsp, _ := cli.RPush(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Res)
	case "LPOP":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.LPopReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.LPop(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(string(rsp.Value))
	case "RPOP":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.RPopReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.RPop(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(string(rsp.Value))
	case "LINDEX":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		idx, _ := strconv.Atoi(args[1])
		req := &service.LIndexReq{
			Key: []byte(args[0]),
			Idx: int64(idx),
		}

		rsp, _ := cli.LIndex(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(string(rsp.Value))
	case "LREM":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		count, _ := strconv.Atoi(args[2])
		req := &service.LRemReq{
			Key:   []byte(args[0]),
			Value: []byte(args[1]),
			Count: int64(count),
		}

		rsp, _ := cli.LRem(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Res)
	case "LINSERT":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		option := service.InsertOption_Before
		if args[3] == "after" {
			option = service.InsertOption_After
		}
		req := &service.LInsertReq{
			Key:    []byte(args[0]),
			Value:  []byte(args[1]),
			Pivot:  []byte(args[2]),
			Option: option,
		}

		rsp, _ := cli.LInsert(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Count)
	case "LSET":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		idx, _ := strconv.Atoi(args[2])
		req := &service.LSetReq{
			Key:   []byte(args[0]),
			Value: []byte(args[1]),
			Idx:   int64(idx),
		}

		rsp, _ := cli.LSet(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "LTRIM":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		start, _ := strconv.Atoi(args[1])
		end, _ := strconv.Atoi(args[2])
		req := &service.LTrimReq{
			Key:   []byte(args[0]),
			Start: int64(start),
			End:   int64(end),
		}

		rsp, _ := cli.LTrim(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "LRANGE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		start, _ := strconv.Atoi(args[1])
		end, _ := strconv.Atoi(args[2])
		req := &service.LRangeReq{
			Key:   []byte(args[0]),
			Start: int64(start),
			End:   int64(end),
		}

		rsp, _ := cli.LRange(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		for _, v := range rsp.Values {
			fmt.Println(string(v))
		}

	case "LLEN":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.LLenReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.LLen(ctx, req)
		fmt.Println((rsp.Length))
	case "LKEYEXISTS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.LKeyExistsReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.LKeyExists(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Ok)
	case "LVALEXISTS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.LValExistsReq{
			Key:   []byte(args[0]),
			Value: []byte(args[1]),
		}

		rsp, _ := cli.LValExists(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Ok)
	case "LCLEAR":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.LClearReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.LClear(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "LEXPIRE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		duration, _ := strconv.ParseInt(args[1], 10, 64)
		req := &service.LExpireReq{
			Key:      []byte(args[0]),
			Duration: duration,
		}

		rsp, _ := cli.LExpire(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		fmt.Println("Ok")
	case "LTTL":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.LTTLReq{
			Key: []byte(args[0]),
		}
		rsp, _ := cli.LTTL(ctx, req)
		fmt.Println(rsp.Ttl)

	case "SADD":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		members := make([][]byte, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = []byte(args[i])
		}
		req := &service.SAddReq{
			Key:     []byte(args[0]),
			Members: members,
		}
		rsp, _ := cli.SAdd(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Res)
	case "SPOP":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		count, _ := strconv.Atoi(args[1])
		req := &service.SPopReq{
			Key:   []byte(args[0]),
			Count: int64(count),
		}
		rsp, _ := cli.SPop(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}

	case "SISMEMBER":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.SIsMemberReq{
			Key:    []byte(args[0]),
			Member: []byte(args[1]),
		}

		rsp, _ := cli.SIsMember(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		fmt.Println(rsp.IsMember)
	case "SRANDMEMBER":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		count, _ := strconv.Atoi(args[1])
		req := &service.SRandMemberReq{
			Key:   []byte(args[0]),
			Count: int64(count),
		}
		rsp, _ := cli.SRandMember(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "SREM":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		members := make([][]byte, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = []byte(args[i])
		}

		req := &service.SRemReq{
			Key:     []byte(args[0]),
			Members: members,
		}

		rsp, _ := cli.SRem(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Res)
	case "SMOVE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.SMoveReq{
			Src:    []byte(args[0]),
			Dst:    []byte(args[1]),
			Member: []byte(args[2]),
		}

		rsp, _ := cli.SMove(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "SCARD":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.SCardReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.SCard(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Res)
	case "SMEMBERS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.SMembersReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.SMembers(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "SDIFF":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		keys := make([][]byte, len(args))
		for i := 1; i < len(args); i++ {
			keys[i-1] = []byte(args[i])
		}
		req := &service.SDiffReq{
			Keys: keys,
		}

		rsp, _ := cli.SDiff(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "SKEYEXISTS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.SKeyExistsReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.SKeyExists(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Ok)
	case "SCLEAR":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.SClearReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.SClear(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "SEXPIRE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		duration, _ := strconv.ParseInt(args[2], 10, 64)

		req := &service.SExpireReq{
			Key:      []byte(args[0]),
			Duration: duration,
		}

		rsp, _ := cli.SExpire(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}

		fmt.Println("Ok")
	case "STTL":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		req := &service.STTLReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.STTL(ctx, req)
		fmt.Println(rsp.Ttl)

	case "ZADD":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		score, _ := strconv.ParseFloat(args[2], 64)
		req := &service.ZAddReq{
			Key:    []byte(args[0]),
			Member: []byte(args[1]),
			Score:  score,
		}

		rsp, _ := cli.ZAdd(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("(nil)")
	case "ZSCORE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		req := &service.ZScoreReq{
			Key:    []byte(args[0]),
			Member: []byte(args[1]),
		}

		rsp, _ := cli.ZScore(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Ok {
			fmt.Println(rsp.Score)
		} else {
			fmt.Println("nil")
		}
	case "ZRANK":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.ZRankReq{
			Key:    []byte(args[0]),
			Member: []byte(args[1]),
		}

		rsp, _ := cli.ZRank(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Rank)

	case "ZREVRANK":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.ZRevRankReq{
			Key:    []byte(args[0]),
			Member: []byte(args[1]),
		}

		rsp, _ := cli.ZRevRank(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Rank)
	case "ZCARD":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.ZCardReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.ZCard(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Size)
	case "ZINCRBY":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		increment, _ := strconv.ParseFloat(args[2], 32)
		req := &service.ZIncrByReq{
			Key:       []byte(args[0]),
			Member:    []byte(args[1]),
			Increment: float32(increment),
		}

		rsp, _ := cli.ZIncrBy(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Score)
	case "ZRANGE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		start, _ := strconv.Atoi(args[1])
		end, _ := strconv.Atoi(args[2])
		req := &service.ZRangeReq{
			Key:   []byte(args[0]),
			Start: int64(start),
			End:   int64(end),
		}

		rsp, _ := cli.ZRange(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "ZREVRANGE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		start, _ := strconv.Atoi(args[1])
		end, _ := strconv.Atoi(args[2])
		req := &service.ZRevRangeReq{
			Key:   []byte(args[0]),
			Start: int64(start),
			End:   int64(end),
		}

		rsp, _ := cli.ZRevRange(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "ZRANGEWITHSCORES":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		start, _ := strconv.Atoi(args[1])
		end, _ := strconv.Atoi(args[2])
		req := &service.ZRangeWithScoresReq{
			Key:   []byte(args[0]),
			Start: int64(start),
			End:   int64(end),
		}

		rsp, _ := cli.ZRangeWithScores(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "ZREVRANGEWITHSCORES":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		start, _ := strconv.Atoi(args[1])
		end, _ := strconv.Atoi(args[2])
		req := &service.ZRevRangeWithScoresReq{
			Key:   []byte(args[0]),
			Start: int64(start),
			End:   int64(end),
		}

		rsp, _ := cli.ZRevRangeWithScores(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "ZGETBYRANK":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		rank, _ := strconv.Atoi(args[1])
		req := &service.ZGetByRankReq{
			Key:  []byte(args[0]),
			Rank: int64(rank),
		}

		rsp, _ := cli.ZGetByRank(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "ZREVGETBYRANK":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		rank, _ := strconv.Atoi(args[1])
		req := &service.ZRevGetByRankReq{
			Key:  []byte(args[0]),
			Rank: int64(rank),
		}

		rsp, _ := cli.ZRevGetByRank(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "ZSCORERANGE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		min, _ := strconv.ParseFloat(args[1], 32)
		max, _ := strconv.ParseFloat(args[2], 32)

		req := &service.ZScoreRangeReq{
			Key: []byte(args[0]),
			Min: min,
			Max: max,
		}

		rsp, _ := cli.ScoreRange(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "ZREVSCORERANGE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		min, _ := strconv.ParseFloat(args[1], 32)
		max, _ := strconv.ParseFloat(args[2], 32)

		req := &service.ZRevScoreRangeReq{
			Key: []byte(args[0]),
			Min: min,
			Max: max,
		}

		rsp, _ := cli.ZRevScoreRange(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Values != nil {
			for _, v := range rsp.Values {
				fmt.Println(string(v))
			}
		} else {
			fmt.Println("(nil)")
		}
	case "ZREM":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.ZRemReq{
			Key:    []byte(args[0]),
			Member: []byte(args[1]),
		}

		rsp, _ := cli.ZRem(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		if rsp.Ok {
			fmt.Println("Ok")
		} else {
			fmt.Println("false")
		}
	case "ZKEYEXISTS":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.ZKeyExistsReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.ZKeyExists(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Ok)
	case "ZCLEAR":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}

		req := &service.ZClearReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.ZClear(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Ok)
	case "ZEXPIRE":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		duration, _ := strconv.ParseInt(args[1], 10, 64)
		req := &service.ZExpireReq{
			Key:      []byte(args[0]),
			Duration: duration,
		}

		rsp, _ := cli.ZExpire(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println("Ok")
	case "ZTTL":
		if ok, err := util.CheckArgs(cmd, args); !ok {
			fmt.Println(err.Error())
			return
		}
		req := &service.ZTTLReq{
			Key: []byte(args[0]),
		}

		rsp, _ := cli.ZTTL(ctx, req)
		if rsp.ErrorMsg != "" {
			fmt.Println(rsp.ErrorMsg)
			return
		}
		fmt.Println(rsp.Ttl)
	default:
		fmt.Println(util.ErrCmd.Error())
	}

}
