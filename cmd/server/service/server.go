package service

import (
	context "context"
	"errors"
	"go_code/project13/rosedb"
	"go_code/project13/rosedb/ds/list"
	"go_code/project13/rosedb/utils"
	"log"
	"strconv"
	"strings"
	"sync"
)

type ExecCmdFunc func(*rosedb.RoseDB, []string) (interface{}, error)

var ExecCmd = make(map[string]ExecCmdFunc)

var (
	nestedMultiErr  = errors.New("ERR MULTI calls can not be nested")
	withoutMultiErr = errors.New("ERR EXEC without MULTI")
	execAbortErr    = errors.New("EXECABORT Transaction discarded because of previous errors")
	keyNotData      = errors.New("key not exists , key was expired , key is nil or not element in key")
)

func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

type GrpcServer struct {
	db     *rosedb.RoseDB
	closed bool
	mu     sync.Mutex
}

func NewGrpcServer(db *rosedb.RoseDB) *GrpcServer {
	return &GrpcServer{
		db:     db,
		closed: false,
	}
}

func (g *GrpcServer) GrpcClose() {
	if g.closed {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.db.Close(); err != nil {
		log.Fatalf("  g.db.Close err :%v", err)
	}
	g.closed = true
}

// func (g *GrpcServer) Listen(addr string) {
// 	s := grpc.NewServer()
// 	RegisterRosedbServer(s, g)

// }

func (g *GrpcServer) SAdd(_ context.Context, req *SAddReq) (*SAddRsp, error) {
	res := &SAddRsp{}

	resInt, err := g.db.SAdd(req.Key, req.Members...)
	res.Res = int64(resInt)
	if err != nil {
		res.ErrorMsg = err.Error()
	}
	return res, nil
}
func (g *GrpcServer) SPop(_ context.Context, req *SPopReq) (*SPopRsp, error) {
	rsp := &SPopRsp{}
	if vals, err := g.db.SPop(req.Key, int(req.Count)); err != nil {
		rsp.ErrorMsg = err.Error()
	} else {
		rsp.Values = vals
	}
	return rsp, nil

}

func (g *GrpcServer) SIsMember(_ context.Context, req *SIsMemberReq) (*SIsMemberRsp, error) {
	rsp := &SIsMemberRsp{}
	rsp.IsMember = g.db.SIsMember(req.Key, req.Member)
	return rsp, nil
}

func (g *GrpcServer) SRandMember(_ context.Context, req *SRandMemberReq) (*SRandMemberRsp, error) {
	rsp := &SRandMemberRsp{}
	if vals := g.db.SRandMember(req.Key, int(req.Count)); vals != nil {
		rsp.Values = vals
	} else {
		rsp.ErrorMsg = keyNotData.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) SRem(_ context.Context, req *SRemReq) (*SRemRsp, error) {
	rsp := &SRemRsp{}
	res, err := g.db.SRem(req.Key, req.Members...)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(res)
	return rsp, nil
}
func (g *GrpcServer) SMove(_ context.Context, req *SMoveReq) (*SMoveRsp, error) {
	rsp := &SMoveRsp{}
	err := g.db.SMove(req.Src, req.Dst, req.Member)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}
func (g *GrpcServer) SCard(_ context.Context, req *SCardReq) (*SCardRsp, error) {
	rsp := &SCardRsp{}
	rsp.Res = int64(g.db.SCard(req.Key))
	return rsp, nil
}
func (g *GrpcServer) SMembers(_ context.Context, req *SMembersReq) (*SMembersRsp, error) {
	rsp := &SMembersRsp{}
	if vals := g.db.SMembers(req.Key); vals != nil {
		rsp.Values = vals
	} else {
		rsp.ErrorMsg = keyNotData.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) SDiff(_ context.Context, req *SDiffReq) (*SDiffRsp, error) {
	rsp := &SDiffRsp{}
	if vals := g.db.SDiff(req.Keys...); vals != nil {
		rsp.Values = vals
	} else {
		rsp.ErrorMsg = keyNotData.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) SKeyExists(_ context.Context, req *SKeyExistsReq) (*SKeyExistsRsp, error) {
	rsp := &SKeyExistsRsp{}
	rsp.Ok = g.db.SKeyExists(req.Key)
	return rsp, nil
}

func (g *GrpcServer) SClear(_ context.Context, req *SClearReq) (*SClearRsp, error) {
	rsp := &SClearRsp{}
	err := g.db.SClear(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	} else {
		rsp.Ok = true
	}
	return rsp, nil
}

func (g *GrpcServer) SExpire(_ context.Context, req *SExpireReq) (*SExpireRsp, error) {
	rsp := &SExpireRsp{}
	if err := g.db.SExpire(req.Key, req.Duration); err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) STTL(_ context.Context, req *STTLReq) (*STTLRsp, error) {
	rsp := &STTLRsp{}
	rsp.Ttl = g.db.STTL(req.Key)
	return rsp, nil
}

//  * cmd_hash *
// Return num of elements in hash of the specified filed of key.
func (g *GrpcServer) HSet(_ context.Context, req *HSetReq) (*HSetRsp, error) {
	rsp := &HSetRsp{}
	res, err := g.db.HSet(req.Key, req.Field, req.Value)
	rsp.Res = int64(res)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}
func (g *GrpcServer) HSetNx(_ context.Context, req *HSetNxReq) (*HSetNxRsp, error) {
	rsp := &HSetNxRsp{}
	res, err := g.db.HSetNx(req.Key, req.Field, req.Value)
	rsp.Res = int64(res)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}
func (g *GrpcServer) HGet(_ context.Context, req *HGetReq) (*HGetRsp, error) {
	rsp := &HGetRsp{}
	res := g.db.HGet(req.Key, req.Field)
	rsp.Value = res
	return rsp, nil
}

func (g *GrpcServer) HGetAll(_ context.Context, req *HGetAllReq) (*HGetAllRsp, error) {
	rsp := &HGetAllRsp{}
	res := g.db.HGetAll(req.Key)
	rsp.Values = res
	return rsp, nil
}

func (g *GrpcServer) HMSet(_ context.Context, req *HMSetReq) (*HMSetRsp, error) {
	rsp := &HMSetRsp{}
	if err := g.db.HMSet(req.Key, req.Values...); err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}
func (g *GrpcServer) HMGet(_ context.Context, req *HMGetReq) (*HMGetRsp, error) {
	rsp := &HMGetRsp{}
	rsp.Values = g.db.HMGet(req.Key, req.Fileds...)

	return rsp, nil
}
func (g *GrpcServer) HDel(_ context.Context, req *HDelReq) (*HDelRsp, error) {
	rsp := &HDelRsp{}
	res, err := g.db.HDel(req.Key, req.Fileds...)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(res)
	return rsp, nil
}
func (g *GrpcServer) HKeyExists(_ context.Context, req *HKeyExistsReq) (*HKeyExistsRsp, error) {
	rsp := &HKeyExistsRsp{}
	rsp.Ok = g.db.HKeyExist(req.Key)

	return rsp, nil
}
func (g *GrpcServer) HExists(_ context.Context, req *HExistsReq) (*HExistsRsp, error) {
	rsp := &HExistsRsp{}
	rsp.Ok = g.db.HExists(req.Key, req.Filed)
	return rsp, nil
}

func (g *GrpcServer) HLen(_ context.Context, req *HLenReq) (*HLenRsp, error) {
	rsp := &HLenRsp{}
	rsp.Length = int64(g.db.HLen(req.Key))
	return rsp, nil
}

func (g *GrpcServer) HVals(_ context.Context, req *HValsReq) (*HValsRsp, error) {
	rsp := &HValsRsp{}
	rsp.Values = g.db.HVals(req.Key)
	return rsp, nil
}

func (g *GrpcServer) HKeys(_ context.Context, req *HKeysReq) (*HKeysRsp, error) {
	rsp := &HKeysRsp{}
	vals := g.db.HKeys(req.Key)
	if len(vals) != 0 {
		rsp.Values = make([][]byte, len(vals))
		for i, v := range vals {
			rsp.Values[i] = []byte(v)
		}
	}
	return rsp, nil
}

func (g *GrpcServer) HClear(_ context.Context, req *HClearReq) (*HClearRsp, error) {
	rsp := &HClearRsp{}
	if err := g.db.HClear(req.Key); err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) HExpire(_ context.Context, req *HExpireReq) (*HExpireRsp, error) {
	rsp := &HExpireRsp{}
	if err := g.db.HExpire(req.Key, req.Duration); err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) HTTL(_ context.Context, req *HTTLReq) (*HTTLRsp, error) {
	rsp := &HTTLRsp{}
	rsp.Ttl = g.db.HTTL(req.Key)
	return rsp, nil
}

//  * cmd_list  *
func (g *GrpcServer) LPush(_ context.Context, req *LPushReq) (*LPushRsp, error) {
	rsp := &LPushRsp{}
	var values []interface{}

	for _, v := range req.Values {
		values = append(values, v)
	}

	resInt, err := g.db.LPush(req.Key, values...)

	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(resInt)
	return rsp, nil
}

func (g *GrpcServer) RPush(_ context.Context, req *RPushReq) (*RPushRsp, error) {
	rsp := &RPushRsp{}
	var values []interface{}

	for _, v := range req.Values {
		values = append(values, v)
	}

	resInt, err := g.db.RPush(req.Key, values...)

	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(resInt)
	return rsp, nil
}

func (g *GrpcServer) LPop(_ context.Context, req *LPopReq) (*LPopRsp, error) {
	rsp := &LPopRsp{}
	res, err := g.db.LPop(req.Key)

	if err != nil {
		rsp.ErrorMsg = err.Error()
	}

	rsp.Value = res
	return rsp, nil
}

func (g *GrpcServer) RPop(_ context.Context, req *RPopReq) (*RPopRsp, error) {
	rsp := &RPopRsp{}
	res, err := g.db.RPop(req.Key)

	if err != nil {
		rsp.ErrorMsg = err.Error()
	}

	rsp.Value = res
	return rsp, nil
}

func (g *GrpcServer) LIndex(_ context.Context, req *LIndexReq) (*LIndexRsp, error) {
	rsp := &LIndexRsp{}
	if res := g.db.LIndex(req.Key, int(req.Idx)); res != nil {
		rsp.Value = res
	}

	return rsp, nil
}

func (g *GrpcServer) LRem(_ context.Context, req *LRemReq) (*LRemRsp, error) {
	rsp := &LRemRsp{}

	if res, err := g.db.LRem(req.Key, req.Value, int(req.Count)); err != nil {
		rsp.ErrorMsg = err.Error()
	} else {
		rsp.Res = int64(res)
	}

	return rsp, nil
}

func (g *GrpcServer) LInsert(_ context.Context, req *LInsertReq) (*LInsertRsp, error) {
	rsp := &LInsertRsp{}

	if res, err := g.db.LInsert(string(req.Key), list.InsertOption(req.Option), req.Pivot, req.Value); err != nil {
		rsp.ErrorMsg = err.Error()
	} else {
		rsp.Count = int64(res)
	}

	return rsp, nil
}

func (g *GrpcServer) LSet(_ context.Context, req *LSetReq) (*LSetRsp, error) {
	rsp := &LSetRsp{}

	if ok, err := g.db.LSet(req.Key, int(req.Idx), req.Value); err != nil {
		rsp.ErrorMsg = err.Error()
	} else {
		rsp.Ok = ok
	}

	return rsp, nil
}

func (g *GrpcServer) LTrim(_ context.Context, req *LTrimReq) (*LTrimRsp, error) {
	rsp := &LTrimRsp{}

	if err := g.db.LTrim(req.Key, int(req.Start), int(req.End)); err != nil {
		rsp.ErrorMsg = err.Error()
	}

	return rsp, nil
}

func (g *GrpcServer) LRange(_ context.Context, req *LRangeReq) (*LRangeRsp, error) {
	rsp := &LRangeRsp{}

	if res, err := g.db.LRange(req.Key, int(req.Start), int(req.End)); err != nil {
		rsp.ErrorMsg = err.Error()
	} else {
		rsp.Values = res
	}

	return rsp, nil
}

func (g *GrpcServer) LLen(_ context.Context, req *LLenReq) (*LLenRsp, error) {
	rsp := &LLenRsp{}

	rsp.Length = int64(g.db.LLen(req.Key))

	return rsp, nil
}

func (g *GrpcServer) LKeyExists(_ context.Context, req *LKeyExistsReq) (*LKeyExistsRsp, error) {
	rsp := &LKeyExistsRsp{}

	rsp.Ok = g.db.LKeyExists(req.Key)

	return rsp, nil
}

func (g *GrpcServer) LValExists(_ context.Context, req *LValExistsReq) (*LValExistsRsp, error) {
	rsp := &LValExistsRsp{}

	rsp.Ok = g.db.LValExists(req.Key, req.Value)

	return rsp, nil
}

func (g *GrpcServer) LClear(_ context.Context, req *LClearReq) (*LClearRsp, error) {
	rsp := &LClearRsp{}

	err := g.db.LClear(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) LExpire(_ context.Context, req *LExpireReq) (*LExpireRsp, error) {
	rsp := &LExpireRsp{}

	err := g.db.LExpire(req.Key, req.Duration)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) LTTL(_ context.Context, req *LTTLReq) (*LTTLRsp, error) {
	rsp := &LTTLRsp{}

	rsp.Ttl = g.db.LTTL(req.Key)

	return rsp, nil
}

// * cmd_str *
func (g *GrpcServer) Set(_ context.Context, req *SetReq) (*SetRsp, error) {
	rsp := &SetRsp{}
	if err := g.db.Set(req.Key, req.Value); err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) SetNx(_ context.Context, req *SetNxReq) (*SetNxRsp, error) {
	rsp := &SetNxRsp{}
	if ok, err := g.db.SetNx(req.Key, req.Value); err != nil {
		rsp.ErrorMsg = err.Error()
	} else {
		rsp.Ok = ok
	}
	return rsp, nil
}

func (g *GrpcServer) SetEx(_ context.Context, req *SetExReq) (*SetExRsp, error) {
	rsp := &SetExRsp{}
	if err := g.db.SetEx(req.Key, req.Value, req.Duration); err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) Get(_ context.Context, req *GetReq) (*GetRsp, error) {
	rsp := &GetRsp{}
	if err := g.db.Get(req.Key, &rsp.Dest); err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) GetSet(_ context.Context, req *GetSetReq) (*GetSetRsp, error) {
	rsp := &GetSetRsp{}
	if err := g.db.GetSet(req.Key, req.Value, &rsp.Dest); err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) MSet(_ context.Context, req *MSetReq) (*MSetRsp, error) {
	rsp := &MSetRsp{}
	if len(req.Key) != len(req.Values) {
		rsp.ErrorMsg = "len(req.Key) != len(req.Values)"
		return rsp, nil
	}

	var multiData []interface{}
	for i := 0; i < len(req.Key); i++ {
		multiData = append(multiData, req.Key[i])
		multiData = append(multiData, req.Values[i])
	}

	err := g.db.MSet(multiData...)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) MGet(_ context.Context, req *MGetReq) (*MGetRsp, error) {
	rsp := &MGetRsp{}
	var multiData []interface{}
	for i := 0; i < len(req.Keys); i++ {
		multiData = append(multiData, req.Keys[i])
	}
	vals, err := g.db.MGet(multiData...)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Values = vals
	return rsp, nil
}

func (g *GrpcServer) Append(_ context.Context, req *AppendReq) (*AppendRsp, error) {
	rsp := &AppendRsp{}
	err := g.db.Append(req.Key, string(req.Value))
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}

	return rsp, nil
}

func (g *GrpcServer) StrExists(_ context.Context, req *StrExistsReq) (*StrExistsRsp, error) {
	rsp := &StrExistsRsp{}
	rsp.Ok = g.db.StrExists(req.Key)

	return rsp, nil
}

func (g *GrpcServer) Remove(_ context.Context, req *RemoveReq) (*RemoveRsp, error) {
	rsp := &RemoveRsp{}
	err := g.db.Remove(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) PrefixScan(_ context.Context, req *PrefixScanReq) (*PrefixScanRsp, error) {
	rsp := &PrefixScanRsp{}
	vals, err := g.db.PrefixScan(string(req.Prefix), int(req.Limit), int(req.Offset))
	if err != nil {
		rsp.ErrorMsg = err.Error()
		return rsp, nil
	}

	rsp.Values = make([][]byte, 0)
	for _, val := range vals {
		vEncode, err := utils.EncodeValue(val)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		}
		rsp.Values = append(rsp.Values, vEncode)
	}
	return rsp, nil
}

func (g *GrpcServer) RangeScan(_ context.Context, req *RangeScanReq) (*RangeScanRsp, error) {
	rsp := &RangeScanRsp{}
	vals, err := g.db.RangeScan(req.Start, req.End)
	if err != nil {
		rsp.ErrorMsg = err.Error()
		return rsp, nil
	}
	rsp.Values = make([][]byte, 0)
	for _, val := range vals {
		vEncode, err := utils.EncodeValue(val)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		}
		rsp.Values = append(rsp.Values, vEncode)
	}
	return rsp, nil
}

func (g *GrpcServer) Expire(_ context.Context, req *ExpireReq) (*ExpireRsp, error) {
	rsp := &ExpireRsp{}
	err := g.db.Expire(req.Key, req.Duration)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) Persist(_ context.Context, req *PersistReq) (*PersistRsp, error) {
	rsp := &PersistRsp{}
	err := g.db.Persist(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) TTl(_ context.Context, req *TTLReq) (*TTLRsp, error) {
	rsp := &TTLRsp{}
	res := g.db.TTL(req.Key)
	rsp.Ttl = res
	return rsp, nil
}

//** cmd_zse
func (g *GrpcServer) ZAdd(_ context.Context, req *ZAddReq) (*ZAddRsp, error) {
	res := &ZAddRsp{}

	err := g.db.ZAdd(req.Key, req.Score, req.Member)

	if err != nil {
		res.ErrorMsg = err.Error()
	}
	return res, nil
}

func (g *GrpcServer) ZScore(_ context.Context, req *ZScoreReq) (*ZScoreRsp, error) {
	rsp := &ZScoreRsp{}

	rsp.Ok, rsp.Score = g.db.ZScore(req.Key, req.Member)
	return rsp, nil
}

func (g *GrpcServer) ZRank(_ context.Context, req *ZRankReq) (*ZRankRsp, error) {
	rsp := &ZRankRsp{}

	rsp.Rank = g.db.ZRank(req.Key, req.Member)
	return rsp, nil
}

func (g *GrpcServer) ZCard(_ context.Context, req *ZCardReq) (*ZCardRsp, error) {
	rsp := &ZCardRsp{}

	rsp.Size = int64(g.db.ZCard(req.Key))
	return rsp, nil
}

func (g *GrpcServer) ZRevRank(_ context.Context, req *ZRevRankReq) (*ZRevRankRsp, error) {
	rsp := &ZRevRankRsp{}

	rsp.Rank = g.db.ZRevRank(req.Key, req.Member)
	return rsp, nil
}

func (g *GrpcServer) ZIncrBy(_ context.Context, req *ZIncrByReq) (*ZIncrByRsp, error) {
	rsp := &ZIncrByRsp{}

	score, err := g.db.ZIncrBy(req.Key, float64(req.Increment), req.Member)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Score = score
	return rsp, nil
}

func (g *GrpcServer) ZRange(_ context.Context, req *ZRangeReq) (*ZRangeRsp, error) {
	rsp := &ZRangeRsp{}

	if vals := g.db.ZRange(req.Key, int(req.Start), int(req.End)); vals != nil {
		rsp.Values = make([][]byte, 0)
		for _, val := range vals {
			vEncode, err := utils.EncodeValue(val)
			if err != nil {
				rsp.ErrorMsg = err.Error()
				return rsp, nil
			}
			rsp.Values = append(rsp.Values, vEncode)
		}
	}

	return rsp, nil
}

func (g *GrpcServer) ZRangeWithScores(_ context.Context, req *ZRangeWithScoresReq) (*ZRangeWithScoresRsp, error) {
	rsp := &ZRangeWithScoresRsp{}

	if vals := g.db.ZRangeWithScores(req.Key, int(req.Start), int(req.End)); vals != nil {
		rsp.Values = make([][]byte, 0)

		for i := 0; i <= len(vals)-2; i += 2 {
			vEncode, err := utils.EncodeValue(vals[i])
			if err != nil {
				rsp.ErrorMsg = err.Error()
				return rsp, nil
			}
			score := vals[i+1].(float64)
			s := strconv.FormatFloat(score, 'f', -1, 64)

			rsp.Values = append(rsp.Values, vEncode, []byte(s))
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZRem(_ context.Context, req *ZRemReq) (*ZRemRsp, error) {
	rsp := &ZRemRsp{}

	ok, err := g.db.ZRem(req.Key, req.Member)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Ok = ok
	return rsp, nil
}

func (g *GrpcServer) ZRevRange(_ context.Context, req *ZRevRangeReq) (*ZRevRangeRsp, error) {
	rsp := &ZRevRangeRsp{}
	if vals := g.db.ZRevRange(req.Key, int(req.Start), int(req.End)); vals != nil {
		rsp.Values = make([][]byte, 0)
		for _, val := range vals {
			vEncode, err := utils.EncodeValue(val)
			if err != nil {
				rsp.ErrorMsg = err.Error()
				return rsp, nil
			}
			rsp.Values = append(rsp.Values, vEncode)
		}
	}

	return rsp, nil
}

func (g *GrpcServer) ZRevRangeWithScores(_ context.Context, req *ZRevRangeWithScoresReq) (*ZRevRangeWithScoresRsp, error) {
	rsp := &ZRevRangeWithScoresRsp{}

	if vals := g.db.ZRevRangeWithScores(req.Key, int(req.Start), int(req.End)); vals != nil {
		rsp.Values = make([][]byte, 0)
		for i := 0; i <= len(vals)-2; i += 2 {
			vEncode, err := utils.EncodeValue(vals[i])
			if err != nil {
				rsp.ErrorMsg = err.Error()
				return rsp, nil
			}
			score := vals[i+1].(float64)
			s := strconv.FormatFloat(score, 'f', -1, 64)

			rsp.Values = append(rsp.Values, vEncode, []byte(s))
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZGetByRank(_ context.Context, req *ZGetByRankReq) (*ZGetByRankRsp, error) {
	rsp := &ZGetByRankRsp{}

	if vals := g.db.ZGetByRank(req.Key, int(req.Rank)); vals != nil {
		rsp.Values = make([][]byte, 0)
		for i := 0; i <= len(vals)-2; i += 2 {
			vEncode, err := utils.EncodeValue(vals[i])
			if err != nil {
				rsp.ErrorMsg = err.Error()
				return rsp, nil
			}
			score := vals[i+1].(float64)
			s := strconv.FormatFloat(score, 'f', -1, 64)
			rsp.Values = append(rsp.Values, vEncode, []byte(s))
		}
	}

	return rsp, nil
}

func (g *GrpcServer) ZRevGetByRank(_ context.Context, req *ZRevGetByRankReq) (*ZRevGetByRankRsp, error) {
	rsp := &ZRevGetByRankRsp{}

	if vals := g.db.ZRevGetByRank(req.Key, int(req.Rank)); vals != nil {
		rsp.Values = make([][]byte, 0)
		for i := 0; i <= len(vals)-2; i += 2 {
			vEncode, err := utils.EncodeValue(vals[i])
			if err != nil {
				rsp.ErrorMsg = err.Error()
				return rsp, nil
			}
			score := vals[i+1].(float64)
			s := strconv.FormatFloat(score, 'f', -1, 64)
			rsp.Values = append(rsp.Values, vEncode, []byte(s))
		}
	}

	return rsp, nil
}

func (g *GrpcServer) ScoreRange(_ context.Context, req *ZScoreRangeReq) (*ZScoreRangeRsp, error) {
	rsp := &ZScoreRangeRsp{}

	if vals := g.db.ZScoreRange(req.Key, req.Min, req.Max); vals != nil {
		rsp.Values = make([][]byte, 0)
		for i := 0; i <= len(vals)-2; i += 2 {
			vEncode, err := utils.EncodeValue(vals[i])
			if err != nil {
				rsp.ErrorMsg = err.Error()
				return rsp, nil
			}
			score := vals[i+1].(float64)
			s := strconv.FormatFloat(score, 'f', -1, 64)
			rsp.Values = append(rsp.Values, vEncode, []byte(s))
		}
	}

	return rsp, nil
}

func (g *GrpcServer) ZRevScoreRange(_ context.Context, req *ZRevScoreRangeReq) (*ZRevScoreRangeRsp, error) {
	rsp := &ZRevScoreRangeRsp{}

	if vals := g.db.ZRevScoreRange(req.Key, req.Min, req.Max); vals != nil {
		rsp.Values = make([][]byte, 0)
		for i := 0; i <= len(vals)-2; i += 2 {
			vEncode, err := utils.EncodeValue(vals[i])
			if err != nil {
				rsp.ErrorMsg = err.Error()
				return rsp, nil
			}
			score := vals[i+1].(float64)
			s := strconv.FormatFloat(score, 'f', -1, 64)
			rsp.Values = append(rsp.Values, vEncode, []byte(s))
		}
	}

	return rsp, nil
}

func (g *GrpcServer) ZKeyExists(_ context.Context, req *ZKeyExistsReq) (*ZKeyExistsRsp, error) {
	rsp := &ZKeyExistsRsp{}

	rsp.Ok = g.db.ZKeyExists(req.Key)

	return rsp, nil
}

func (g *GrpcServer) ZClear(_ context.Context, req *ZClearReq) (*ZClearRsp, error) {
	rsp := &ZClearRsp{}

	err := g.db.ZClear(req.Key)
	if err != nil {
		rsp.Ok = false
		rsp.ErrorMsg = err.Error()
	} else {
		rsp.Ok = true
	}

	return rsp, nil
}

func (g *GrpcServer) ZExpire(_ context.Context, req *ZExpireReq) (*ZExpireRsp, error) {
	rsp := &ZExpireRsp{}

	err := g.db.ZExpire(req.Key, req.Duration)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) ZTTL(_ context.Context, req *ZTTLReq) (*ZTTLRsp, error) {
	rsp := &ZTTLRsp{}
	rsp.Ttl = g.db.ZTTL(req.Key)
	return rsp, nil
}

func (g *GrpcServer) mustEmbedUnimplementedRosedbServer() {}
