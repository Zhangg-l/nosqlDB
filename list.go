package rosedb

func (db *RoseDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	return nil
}
/**
func(db *RoseDB)listMeta(key []byte)(uint32,uint32,error){
	
}

func(db *RoseDB)pushInternal(key []byte,val []byte,isLeft bool){
	headSeq ,tailSeq,err := db.listMeta(key)
}
**/