package service

// problem
/*
	1  in testing, when update data,new data maybe not write disk
*/
import (
	"context"
	"go_code/project13/rosedb"
	"go_code/project13/rosedb/storage"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var dbPath = "/tmp/rosedb_server"

func InitServer() *GrpcServer {

	config := rosedb.DefaultConfig()
	config.DirPath = dbPath
	config.IdxMode = rosedb.KeyOnlyMemMode
	config.RwMethod = storage.FileIO

	db, err := rosedb.Open(*config)
	if err != nil {
		log.Fatal(err)
	}

	return &GrpcServer{
		db: db,
		mu: sync.Mutex{},
	}
}

func TestSet_SAdd(t *testing.T) {
	server := InitServer()
	key := []byte("h1")
	t.Run("SAdd", func(t *testing.T) {
		context := context.Background()
		req := &SAddReq{
			Key: key,
		}
		req.Members = append(req.Members, []byte("t1"))
		req.Members = append(req.Members, []byte("t2"))
		req.Members = append(req.Members, []byte("t3"))
		req.Members = append(req.Members, []byte(strconv.Itoa(45)))

		rsp, _ := server.SAdd(context, req)
		log.Printf("%#v", rsp)
		rsp, _ = server.SAdd(context, &SAddReq{
			Key: nil,
		})
		log.Printf("%#v", rsp)
	})

}

// func TestSet_SPop(t *testing.T) {
// 	server := InitServer()
// 	key := []byte("h1")

// 	t.Run("SPop", func(t *testing.T) {
// 		context := context.Background()

// 		req := &SPopReq{
// 			Key:   key,
// 			Count: 2,
// 		}
// 		scard := SCardReq{
// 			Key: key,
// 		}
// 		rp, _ := server.SCard(context, &scard)
// 		t.Log(rp.Res)
// 		rsp, _ := server.SPop(context, req)
// 		for _, v := range rsp.Values {
// 			t.Log(string(v))
// 		}

// 		rp, _ = server.SCard(context, &scard)
// 		t.Log(rp.Res)
// 	})

// }

// func TestSet_SIsMember(t *testing.T) {
// 	server := InitServer()
// 	key := []byte("h1")

// 	t.Run("SIsMember", func(t *testing.T) {
// 		context := context.Background()

// 		req := &SIsMemberReq{
// 			Key:    key,
// 			Member: []byte("t3"),
// 		}
// 		rsp, _ := server.SIsMember(context, req)
// 		t.Logf("%#v", rsp)

// 	})

// }

// func TestSet_SRandMember(t *testing.T) {
// 	server := InitServer()
// 	key := []byte("h1")

// 	t.Run("SRandMember", func(t *testing.T) {
// 		context := context.Background()

// 		req := &SRandMemberReq{
// 			Key:   key,
// 			Count: 2,
// 		}
// 		c1, _ := server.SRandMember(context, &SRandMemberReq{
// 			Key: []byte("c1"),
// 		})
// 		t.Log(c1.ErrorMsg)

// 		rsp, _ := server.SRandMember(context, req)
// 		if rsp.ErrorMsg == "" {
// 			for _, v := range rsp.Values {
// 				t.Log(string(v))
// 			}
// 		} else {
// 			t.Log(rsp.ErrorMsg)
// 		}

// 	})
// }

func TestSet_SRem(t *testing.T) {
	server := InitServer()
	key := []byte("h1")

	t.Run("SRem", func(t *testing.T) {
		context := context.Background()

		req := &SRemReq{
			Key: key,
		}
		req.Members = append(req.Members, []byte("t1"))
		req.Members = append(req.Members, []byte("t3"))

		rsp, _ := server.SRem(context, req)
		t.Log(rsp.Res)

	})

}

func TestSet_SMove(t *testing.T) {
	server := InitServer()
	key := []byte("h1")

	t.Run("SMove", func(t *testing.T) {
		context := context.Background()

		req := &SMoveReq{
			Src:    key,
			Dst:    []byte("h2"),
			Member: []byte("t2"),
		}

		rsp, _ := server.SMove(context, req)
		if rsp.ErrorMsg == "" {
			ll, _ := server.SCard(context, &SCardReq{
				Key: []byte("h2"),
			})
			t.Log(ll.Res)
		} else {
			t.Log(rsp.ErrorMsg)
		}

	})

}

func TestSet_SCard(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("SCard", func(t *testing.T) {
		context := context.Background()

		// req := &SCardReq{
		// 	Key: key,
		// }
		rsp, _ := server.SCard(context, &SCardReq{
			Key: []byte("h2"),
		})
		if rsp.ErrorMsg == "" {
			t.Log(rsp.Res)
		} else {
			t.Log(rsp.ErrorMsg)
		}

	})

}

func TestSet_SMembers(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("SMembers", func(t *testing.T) {
		context := context.Background()

		// req := &SCardReq{
		// 	Key: key,
		// }
		rsp, _ := server.SMembers(context, &SMembersReq{
			Key: []byte("h2"),
		})
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			for _, v := range rsp.Values {
				t.Log(string(v))
			}
		}

	})

}

func TestSet_SDiff(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("SDiff", func(t *testing.T) {
		context := context.Background()

		req := &SDiffReq{}
		// req.Keys = append(req.Keys, []byte("h1"))
		// req.Keys = append(req.Keys, []byte(""))
		rsp, _ := server.SDiff(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			for _, v := range rsp.Values {
				t.Log(string(v))
			}
		}

	})

}

func TestSet_SKeyExists(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("SKeyExists", func(t *testing.T) {
		context := context.Background()

		req := &SKeyExistsReq{Key: []byte("")}
		req = &SKeyExistsReq{}
		// req = &SKeyExistsReq{}
		// req.Keys = append(req.Keys, []byte("h1"))
		// req.Keys = append(req.Keys, []byte(""))
		rsp, _ := server.SKeyExists(context, req)
		t.Logf("%#v", rsp)

	})

}

func TestSet_SClear(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("SClear", func(t *testing.T) {
		context := context.Background()

		req := &SClearReq{
			Key: []byte("asd"),
		}
		// req = &SKeyExistsReq{}
		// req = &SKeyExistsReq{}
		// req.Keys = append(req.Keys, []byte("h1"))
		// req.Keys = append(req.Keys, []byte(""))
		rsp, _ := server.SClear(context, req)
		t.Logf("%#v", rsp)

	})

}

func TestSet_SExpire(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("SExpire", func(t *testing.T) {
		context := context.Background()

		req := &SExpireReq{
			Key:      []byte("h1"),
			Duration: 200,
		}
		// req = &SKeyExistsReq{}
		// req = &SKeyExistsReq{}
		// req.Keys = append(req.Keys, []byte("h1"))
		// req.Keys = append(req.Keys, []byte(""))
		rsp, _ := server.SExpire(context, req)
		t.Logf("%#v", rsp)

	})

}

func TestSet_STTL(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("sttl", func(t *testing.T) {
		context := context.Background()

		req := &STTLReq{
			Key: []byte("h1"),
		}
		// req = &SKeyExistsReq{}
		// req = &SKeyExistsReq{}
		// req.Keys = append(req.Keys, []byte("h1"))
		// req.Keys = append(req.Keys, []byte(""))
		for i := 0; i < 10; i++ {
			rsp, _ := server.STTL(context, req)
			t.Logf("%#v", rsp)
			time.Sleep(2 * time.Second)
		}

	})
}

func TestSet_HSet(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("HSet", func(t *testing.T) {
		context := context.Background()

		req := &HSetReq{
			Key:   []byte("h1"),
			Field: []byte("k000"),
			Value: []byte("v000"),
		}

		for i := 0; i < 10; i++ {
			req.Field = append(req.Field, []byte(strconv.Itoa(i))...)
			req.Value = append(req.Value, []byte(strconv.Itoa(i))...)
			rsp, _ := server.HSet(context, req)
			t.Logf("%#v", rsp)
		}

	})

}

func TestSet_HGetAll(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("HGetAll", func(t *testing.T) {
		context := context.Background()

		req := &HGetAllReq{
			Key: []byte("h1"),
		}

		rsp, _ := server.HGetAll(context, req)
		if rsp.ErrorMsg == "" {
			for _, v := range rsp.Values {
				t.Log(string(v))
			}
		} else {
			t.Log(rsp.ErrorMsg)
		}

	})

}

func TestSet_HSetNx(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("HSetNx", func(t *testing.T) {
		context := context.Background()

		req := &HSetNxReq{
			Key:   []byte("h1"),
			Field: []byte("k000c"),
			Value: []byte("v000c"),
		}

		rsp, _ := server.HSetNx(context, req)
		if rsp.ErrorMsg == "" {
			t.Log(rsp.Res)
		} else {
			t.Log(rsp.ErrorMsg)
		}

	})

}

func TestSet_HGet(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("HGet", func(t *testing.T) {
		context := context.Background()

		req := &HGetReq{
			Key:   []byte("h1"),
			Field: []byte("k000c"),
		}
		rsp, _ := server.HGet(context, req)
		t.Log(string(rsp.Value))
	})

}

func TestSet_HMSet(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("HGet", func(t *testing.T) {
		context := context.Background()

		req := &HMSetReq{
			Key: []byte("h1"),
		}
		req.Values = append(req.Values, []byte("1"), []byte("11"))
		req.Values = append(req.Values, []byte("2"), []byte("22"))
		req.Values = append(req.Values, []byte("33"))
		rsp, _ := server.HMSet(context, req)
		t.Log(rsp.ErrorMsg)
	})

}

func TestSet_HMGet(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("HMGet", func(t *testing.T) {
		context := context.Background()

		req := &HMGetReq{
			Key: []byte("h1"),
		}
		req.Fileds = append(req.Fileds, []byte("1"))
		req.Fileds = append(req.Fileds, []byte("2"))
		req.Fileds = append(req.Fileds, []byte("3"))
		req.Fileds = append(req.Fileds, []byte("44"))
		rsp, _ := server.HMGet(context, req)
		if rsp.ErrorMsg == "" {
			for _, v := range rsp.Values {
				t.Log(string(v))
			}
		} else {
			t.Log(rsp.ErrorMsg)
		}
	})

}

func TestSet_HDel(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("HDel", func(t *testing.T) {
		context := context.Background()

		req := &HDelReq{
			Key: []byte("h1"),
		}
		req.Fileds = append(req.Fileds, []byte("123asd"))
		rsp, _ := server.HDel(context, req)
		if rsp.ErrorMsg == "" {

			t.Log(rsp.Res)
		} else {
			t.Log(rsp.ErrorMsg)
		}
	})

}

func TestSet_HKeyExists(t *testing.T) {
	server := InitServer()
	// key := []byte("h1")

	t.Run("HKeyExists", func(t *testing.T) {
		context := context.Background()

		req := &HKeyExistsReq{
			Key: []byte("asdqwe"),
		}
		rsp, _ := server.HKeyExists(context, req)
		if rsp.ErrorMsg == "" {
			t.Log(rsp.Ok)
		} else {
			t.Log(rsp.ErrorMsg)
		}
	})

}

func TestSet_HLen(t *testing.T) {
	server := InitServer()
	t.Run("HLen", func(t *testing.T) {
		context := context.Background()

		req := &HLenReq{
			Key: []byte("h21"),
		}
		rsp, _ := server.HLen(context, req)
		if rsp.ErrorMsg == "" {
			t.Log(rsp.Length)
		} else {
			t.Log(rsp.ErrorMsg)
		}
	})

}

func TestSet_HVals(t *testing.T) {
	server := InitServer()
	t.Run("HVals", func(t *testing.T) {
		context := context.Background()

		req := &HValsReq{
			Key: []byte("h112"),
		}
		rsp, _ := server.HVals(context, req)
		if rsp.ErrorMsg == "" {
			for _, v := range rsp.Values {
				t.Log(string(v))
			}
		} else {
			t.Log(rsp.ErrorMsg)
		}
	})

}

func TestSet_HClear(t *testing.T) {
	server := InitServer()
	t.Run("HClear", func(t *testing.T) {
		context := context.Background()

		req := &HClearReq{
			Key: []byte("h1"),
		}
		rsp, _ := server.HClear(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		}
	})

}

func TestSet_HExpire(t *testing.T) {
	server := InitServer()
	t.Run("HClear", func(t *testing.T) {
		context := context.Background()

		req := &HExpireReq{
			Key:      []byte("h1"),
			Duration: 120,
		}

		rsp, _ := server.HExpire(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		}

		for i := 0; i < 5; i++ {
			cc, _ := server.HTTL(context, &HTTLReq{
				Key: []byte("h1"),
			})
			t.Log(cc.Ttl)
			time.Sleep(3 * time.Second)
		}
	})

}

func TestSet_LPush(t *testing.T) {
	server := InitServer()
	t.Run("LPush", func(t *testing.T) {
		context := context.Background()

		req := &LPushReq{
			Key: []byte("ls1"),
		}
		req.Values = append(req.Values, []byte("faz"))
		req.Values = append(req.Values, []byte("fsa"))
		req.Values = append(req.Values, []byte("fl4"))
		req.Values = append(req.Values, []byte("f96"))
		rsp, _ := server.LPush(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			t.Log(rsp.Res)
		}

	})

}

func TestSet_LPop(t *testing.T) {
	server := InitServer()
	t.Run("LPop", func(t *testing.T) {
		context := context.Background()

		req := &LPopReq{
			Key: []byte("ls1"),
		}

		rsp, _ := server.LPop(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			t.Log(string(rsp.Value))
		}
	})

}

func TestSet_RPush(t *testing.T) {
	server := InitServer()
	t.Run("RPush", func(t *testing.T) {
		context := context.Background()

		req := &RPushReq{
			Key: []byte("rc1"),
		}
		req.Values = append(req.Values, []byte("rr5"))
		req.Values = append(req.Values, []byte("rr6"))
		req.Values = append(req.Values, []byte("rr7"))
		rsp, _ := server.RPush(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			t.Log(rsp.Res)
		}

	})

}

func TestSet_LIndex(t *testing.T) {
	server := InitServer()
	t.Run("LIndex", func(t *testing.T) {
		context := context.Background()

		req := &LIndexReq{
			Key: []byte("ll1"),
			Idx: 1,
		}

		rsp, _ := server.LIndex(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			t.Log(string(rsp.Value))
		}
	})
}

func TestSet_RPop(t *testing.T) {
	server := InitServer()
	t.Run("RPop", func(t *testing.T) {
		context := context.Background()

		req := &RPopReq{
			Key: []byte("asdasd"),
		}

		rsp, _ := server.RPop(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			t.Log(string(rsp.Value))
		}
	})

}

func TestSet_LRem(t *testing.T) {
	server := InitServer()
	t.Run("LRem", func(t *testing.T) {
		context := context.Background()

		req := &LRemReq{
			Key:   []byte("ll1"),
			Value: []byte("sa"),
			Count: 2,
		}

		rsp, _ := server.LRem(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			t.Log(rsp.Res)
		}
	})

}

func TestSet_LInsert(t *testing.T) {
	server := InitServer()
	t.Run("LInsert", func(t *testing.T) {
		context := context.Background()

		req := &LInsertReq{
			Key:    []byte("ll1"),
			Value:  []byte("sas"),
			Pivot:  []byte("l4"),
			Option: InsertOption_Before,
		}

		rsp, _ := server.LInsert(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			t.Log(rsp.Count)
		}
	})

}

func TestSet_LSet(t *testing.T) {
	server := InitServer()
	t.Run("LSet", func(t *testing.T) {
		context := context.Background()

		req := &LSetReq{
			Key:   []byte("ll1"),
			Value: []byte("sassss"),
			Idx:   20,
		}

		rsp, _ := server.LSet(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			t.Log(rsp.Ok)
		}
	})

}

func TestSet_LTrim(t *testing.T) {
	server := InitServer()
	t.Run("LTrim", func(t *testing.T) {
		context := context.Background()

		req := &LTrimReq{
			Key:   []byte("rc1"),
			Start: 1,
			End:   4,
		}

		rsp, _ := server.LTrim(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		}
	})

}

func TestSet_LRange(t *testing.T) {
	server := InitServer()
	t.Run("LRange", func(t *testing.T) {
		context := context.Background()

		req := &LRangeReq{
			Key:   []byte("rc1"),
			Start: 0,
			End:   -1,
		}

		rsp, _ := server.LRange(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			for _, v := range rsp.Values {
				t.Log(string(v))
			}
		}
	})

}

func TestSet_LLen(t *testing.T) {
	server := InitServer()
	t.Run("LLen", func(t *testing.T) {
		context := context.Background()

		req := &LLenReq{
			Key: []byte("ls1"),
		}

		rsp, _ := server.LLen(context, req)
		t.Log(rsp.Length)
	})

}

func TestSet_LKeyExists(t *testing.T) {
	server := InitServer()
	t.Run("LKeyExists", func(t *testing.T) {
		context := context.Background()

		req := &LKeyExistsReq{
			Key: []byte("ll1asdz"),
		}

		rsp, _ := server.LKeyExists(context, req)
		t.Log(rsp.Ok)
	})

}

func TestSet_LValExists(t *testing.T) {
	server := InitServer()
	t.Run("LValExists", func(t *testing.T) {
		context := context.Background()

		req := &LValExistsReq{
			Key:   []byte("ll1"),
			Value: []byte("l4"),
		}

		rsp, _ := server.LValExists(context, req)
		t.Log(rsp.Ok)
	})

}

func TestSet_LClear(t *testing.T) {
	server := InitServer()
	t.Run("LClear", func(t *testing.T) {
		context := context.Background()

		req := &LClearReq{
			Key: []byte("lzxc1"),
		}

		rsp, _ := server.LClear(context, req)
		t.Log(rsp.ErrorMsg)
	})

}

func TestSet_LExpire(t *testing.T) {
	server := InitServer()
	t.Run("LExpire", func(t *testing.T) {
		context := context.Background()

		req := &LExpireReq{
			Key:      []byte("h1"),
			Duration: 120,
		}

		rsp, _ := server.LExpire(context, req)
		t.Log(rsp.ErrorMsg)

		rp := &LTTLReq{
			Key: []byte("h1"),
		}

		for i := 0; i < 5; i++ {

			rsp, _ := server.LTTL(context, rp)
			t.Log(rsp.Ttl)
			time.Sleep(2 * time.Second)
		}
	})

}

func TestSet_Set(t *testing.T) {
	server := InitServer()
	t.Run("Set", func(t *testing.T) {
		context := context.Background()

		req := &SetReq{
			Key:   []byte("s3"),
			Value: []byte("ss`3"),
		}

		rsp, _ := server.Set(context, req)
		t.Log(rsp.ErrorMsg)

	})

}

func TestSet_SetNx(t *testing.T) {
	server := InitServer()
	t.Run("SetNx", func(t *testing.T) {
		context := context.Background()

		req := &SetNxReq{
			Key:   []byte("s4"),
			Value: []byte("ss`4a"),
		}

		rsp, _ := server.SetNx(context, req)
		t.Log(rsp.Ok)

	})

}

func TestSet_SetEx(t *testing.T) {
	server := InitServer()
	t.Run("SetEx", func(t *testing.T) {
		context := context.Background()

		req := &SetExReq{
			Key:      []byte("mm1"),
			Value:    []byte("vv1"),
			Duration: 600,
		}

		rsp, _ := server.SetEx(context, req)
		t.Log(rsp.ErrorMsg)

	})

}

func TestSet_Get(t *testing.T) {
	server := InitServer()
	t.Run("Get", func(t *testing.T) {
		context := context.Background()

		req := &GetReq{
			Key: []byte("s1asd"),
		}

		rsp, _ := server.Get(context, req)
		t.Log(string(rsp.Dest))

	})

}

func TestSet_GetSet(t *testing.T) {
	server := InitServer()
	t.Run("GetSet", func(t *testing.T) {
		context := context.Background()

		req := &GetSetReq{
			Key:   []byte("s1"),
			Value: []byte("s1s"),
		}

		rsp, _ := server.GetSet(context, req)
		t.Log(string(rsp.Dest))

	})

}

func TestSet_MSet(t *testing.T) {
	server := InitServer()
	t.Run("MSet", func(t *testing.T) {
		context := context.Background()

		req := &MSetReq{}
		req.Key = append(req.Key, []byte("m1"), []byte("m2"), []byte("m3"))
		req.Values = append(req.Values, []byte("v1"), []byte("v2"), []byte("v3"))
		rsp, _ := server.MSet(context, req)
		t.Logf("%#v", rsp)

	})

}

func TestSet_MGet(t *testing.T) {
	server := InitServer()
	t.Run("MGet", func(t *testing.T) {
		context := context.Background()

		req := &MGetReq{}
		req.Keys = append(req.Keys, []byte("m1"), []byte("m2"), []byte("m3"))

		rsp, _ := server.MGet(context, req)

		for _, v := range rsp.Values {
			t.Log(string(v))
		}

	})
}

func TestSet_Append(t *testing.T) {
	server := InitServer()
	t.Run("Append", func(t *testing.T) {
		context := context.Background()

		req := &AppendReq{
			Key:   []byte("m1"),
			Value: []byte("ccc"),
		}

		rsp, _ := server.Append(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		}

		rp := &GetReq{
			Key: []byte("m1"),
		}

		rsp2, _ := server.Get(context, rp)
		t.Log(string(rsp2.Dest))
	})

}

func TestSet_StrExists(t *testing.T) {
	server := InitServer()
	t.Run("StrExists", func(t *testing.T) {
		context := context.Background()

		req := &StrExistsReq{
			Key: []byte("mmm"),
		}

		rsp, _ := server.StrExists(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		}
		t.Log(rsp.Ok)
	})

}

func TestSet_Remove(t *testing.T) {
	server := InitServer()
	t.Run("Remove", func(t *testing.T) {
		context := context.Background()

		req := &RemoveReq{
			Key: []byte("m1"),
		}

		rsp, _ := server.Remove(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		}

	})

}

func TestSet_PrefixScan(t *testing.T) {
	server := InitServer()
	t.Run("PrefixScan", func(t *testing.T) {
		context := context.Background()

		req := &PrefixScanReq{
			Prefix: []byte("m"),
			Limit:  2,
		}

		rsp, _ := server.PrefixScan(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			for _, v := range rsp.Values {
				t.Log(string(v))
			}
		}

	})

}

func TestSet_RangeScan(t *testing.T) {
	server := InitServer()
	t.Run("RangeScan", func(t *testing.T) {
		context := context.Background()

		req := &RangeScanReq{
			Start: []byte("m2"),
			End:   []byte("z"),
		}

		rsp, _ := server.RangeScan(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		} else {
			for _, v := range rsp.Values {
				t.Log(string(v))
			}
		}
	})
}

func TestSet_Expire(t *testing.T) {
	server := InitServer()
	t.Run("Expire", func(t *testing.T) {
		context := context.Background()

		req := &ExpireReq{
			Key:      []byte("m2"),
			Duration: 110,
		}

		rsp, _ := server.Expire(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		}
		for i := 0; i < 5; i++ {
			rp, _ := server.TTl(context, &TTLReq{
				Key: []byte("m2"),
			})
			t.Log(rp.Ttl)
			time.Sleep(2 * time.Second)
		}

	})
}

func TestSet_Persist(t *testing.T) {
	server := InitServer()
	t.Run("Expire", func(t *testing.T) {
		context := context.Background()

		req := &PersistReq{
			Key: []byte("m2"),
		}

		rsp, _ := server.Persist(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		}
		for i := 0; i < 5; i++ {
			rp, _ := server.TTl(context, &TTLReq{
				Key: []byte("m2"),
			})
			t.Log(rp.Ttl)
		}
	})
}

func TestSet_ZAdd(t *testing.T) {
	server := InitServer()
	t.Run("ZAdd", func(t *testing.T) {
		context := context.Background()

		req := &ZAddReq{
			Key:    []byte("a1-->a1"),
			Member: []byte("testData1..》"),
		}
		for i := 0; i < 10; i++ {
			req.Member = append(req.Member, []byte(strconv.Itoa(i))...)
			req.Score = float64(rand.Float64() * float64(rand.Intn(1000)))
			rsp, _ := server.ZAdd(context, req)
			t.Logf("%#v", rsp)
		}

	})
}

func TestSet_ZRank(t *testing.T) {
	server := InitServer()
	t.Run("ZRank", func(t *testing.T) {
		context := context.Background()

		req := &ZRankReq{
			Key:    []byte("a1-->01"),
			Member: []byte("testData1..》01"),
		}

		rsp, _ := server.ZRank(context, req)
		t.Logf("%#v", rsp)
	})
}

func TestSet_ZRange(t *testing.T) {
	server := InitServer()
	t.Run("ZRange", func(t *testing.T) {
		context := context.Background()

		req := &ZRangeReq{
			Key:   []byte("a1-->a1"),
			Start: 0,
			End:   10,
		}

		rsp, _ := server.ZRange(context, req)
		// t.Logf("%#v",rsp)
		for _, v := range rsp.Values {
			t.Log(string(v))
		}
		t.Logf("%#v",rsp)
	})
}

func TestSet_ZScore(t *testing.T) {
	server := InitServer()
	t.Run("ZScore", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")
		// t1 := &ZAddReq{Key: key, Member: []byte("k1asd"), Score: 12.5}
		// t2 := &ZAddReq{Key: key, Member: []byte("k2aasdd"), Score: 122.5}
		// t3 := &ZAddReq{Key: key, Member: []byte("k3avesd"), Score: 212.5}
		// t4 := &ZAddReq{Key: key, Member: []byte("k4qweasd"), Score: 112.5}
		// t5 := &ZAddReq{Key: key, Member: []byte("k5vczasd"), Score: 32.5}
		// t6 := &ZAddReq{Key: key, Member: []byte("k6zrasd"), Score: 54.5}
		// ins := []*ZAddReq{}
		// ins = append(ins, t1, t2, t3, t4, t5, t6)
		// for _, v := range ins {
		// 	server.ZAdd(context, v)
		// }
		req := &ZScoreReq{
			Key:    key,
			Member: []byte("k1asd"),
		}
		rsp, _ := server.ZScore(context, req)
		assert.Equal(t, float64(12.5), rsp.Score)
		t.Log(rsp.Score)
	})
}

func TestSet_ZCard(t *testing.T) {
	server := InitServer()
	t.Run("ZCard", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZCardReq{
			Key: key,
		}
		rsp, _ := server.ZCard(context, req)

		t.Logf("%#v", rsp)
	})
}

func TestSet_ZRevRank(t *testing.T) {
	server := InitServer()
	t.Run("ZRevRank", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZRevRankReq{
			Key:    key,
			Member: []byte("k1asd"),
		}
		rsp, _ := server.ZRevRank(context, req)

		t.Logf("%#v", rsp)
	})
}

func TestSet_ZIncrBy(t *testing.T) {
	server := InitServer()
	t.Run("ZIncrBy", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZIncrByReq{
			Key:       key,
			Member:    []byte("k1asd"),
			Increment: 22,
		}
		rsp, _ := server.ZIncrBy(context, req)

		t.Logf("%#v", rsp)
	})
}

func TestSet_ZRangeWithScores(t *testing.T) {
	server := InitServer()
	t.Run("ZRangeWithScores", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZRangeWithScoresReq{
			Key:   key,
			Start: 0,
			End:   3,
		}
		rsp, _ := server.ZRangeWithScores(context, req)
		for _, v := range rsp.Values {
			t.Log(string(v))
		}
	})
}

func TestSet_ZRem(t *testing.T) {
	server := InitServer()
	t.Run("ZRem", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZRemReq{
			Key:    key,
			Member: []byte("k1asd"),
		}
		rsp, _ := server.ZRem(context, req)
		t.Logf("%#v", rsp)
	})
}

func TestSet_ZRevRange(t *testing.T) {
	server := InitServer()
	t.Run("ZRevRange", func(t *testing.T) {
		context := context.Background()

		req := &ZRevRangeReq{
			Key:   []byte("a1-->a1"),
			Start: 0,
			End:   10,
		}

		rsp, _ := server.ZRevRange(context, req)
		// t.Logf("%#v",rsp)
		for _, v := range rsp.Values {
			t.Log(string(v))
		}
	})
}

func TestSet_ZRevRangeWithScores(t *testing.T) {
	server := InitServer()
	t.Run("ZRevRangeWithScores", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZRevRangeWithScoresReq{
			Key:   key,
			Start: 0,
			End:   3,
		}
		rsp, _ := server.ZRevRangeWithScores(context, req)
		for _, v := range rsp.Values {
			t.Log(string(v))
		}
	})
}

func TestSet_ZGetByRank(t *testing.T) {
	server := InitServer()
	t.Run("ZGetByRank", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZGetByRankReq{
			Key:  key,
			Rank: 0,
		}
		rsp, _ := server.ZGetByRank(context, req)
		for _, v := range rsp.Values {
			t.Log(string(v))
		}
	})
}

func TestSet_ZRevGetByRank(t *testing.T) {
	server := InitServer()
	t.Run("ZRevGetByRank", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZRevGetByRankReq{
			Key:  key,
			Rank: 0,
		}
		rsp, _ := server.ZRevGetByRank(context, req)
		for _, v := range rsp.Values {
			t.Log(string(v))
		}
	})
}

func TestSet_ScoreRange(t *testing.T) {
	server := InitServer()
	t.Run("ScoreRange", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZScoreRangeReq{
			Key: key,
			Min: 0,
			Max: 330,
		}
		rsp, _ := server.ScoreRange(context, req)
		for _, v := range rsp.Values {
			t.Log(string(v))
		}
	})
}

func TestSet_ZRevScoreRange(t *testing.T) {
	server := InitServer()
	t.Run("ZRevScoreRange", func(t *testing.T) {
		context := context.Background()
		key := []byte("key1")

		req := &ZRevScoreRangeReq{
			Key: key,
			Min: 0,
			Max: 330,
		}
		rsp, _ := server.ZRevScoreRange(context, req)
		for _, v := range rsp.Values {
			t.Log(string(v))
		}
	})
}

func TestSet_ZKeyExists(t *testing.T) {
	server := InitServer()
	t.Run("ZKeyExists", func(t *testing.T) {
		context := context.Background()
		key := []byte("sdfcxv")

		req := &ZKeyExistsReq{
			Key: key,
		}
		rsp, _ := server.ZKeyExists(context, req)
		t.Logf("%#v", rsp)
	})
}

func TestSet_ZClear(t *testing.T) {
	server := InitServer()
	t.Run("ZClear", func(t *testing.T) {
		context := context.Background()
		key := []byte("a1-->a1")

		req := &ZClearReq{
			Key: key,
		}
		rsp, _ := server.ZClear(context, req)
		t.Logf("%#v", rsp)
	})
}



func TestSet_ZExpire(t *testing.T) {
	server := InitServer()
	t.Run("ZExpire", func(t *testing.T) {
		context := context.Background()

		req := &ZExpireReq{
			Key:      []byte("key1"),
			Duration: 110,
		}

		rsp, _ := server.ZExpire(context, req)
		if rsp.ErrorMsg != "" {
			t.Log(rsp.ErrorMsg)
		}

		for i := 0; i < 5; i++ {
			rp, _ := server.ZTTL(context, &ZTTLReq{
				Key: []byte("key1"),
			})
			t.Log(rp.Ttl)
			time.Sleep(2 * time.Second)
		}

	})
}