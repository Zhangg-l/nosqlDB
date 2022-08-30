package set

var existFlag = struct{}{}

type (
	Set struct {
		record Record
	}
	Record map[string]map[string]struct{}
)

func New() *Set {
	return &Set{make(Record)}
}

// SAdd Add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
func (s *Set) SAdd(key string, member []byte) int {
	if !s.exist(key) {
		s.record[key] = make(map[string]struct{})
	}

	s.record[key][string(member)] = existFlag
	return len(s.record[key])
}

// SPop Removes and returns one or more random members from the set value store at key.
func (s *Set) SPop(key string, count int) (val [][]byte) {
	if !s.exist(key) || count <= 0 {
		return
	}

	for k := range s.record[key] {
		delete(s.record[key], k)
		val = append(val, []byte(k))
		count--
		if count == 0 {
			break
		}

	}
	return
}

// SIsMember Returns if member is a member of the set stored at key.
func (s *Set) SIsMember(key string, member []byte) bool {
	return s.fieldExist(key, string(member))
}

// SRandMember When called with just the key argument, return a random element from the set value stored at key.
func (s *Set) SRandMember(key string, count int) (val [][]byte) {
	
	if !s.exist(key) || count == 0 {
		return
	}

	if count > 0 {
		for k := range s.record[key] {
			val = append(val, []byte(k))
			if len(val) == count {
				break
			}
		}
	} else {
		count = -count
		randomVal := func() []byte {
			for k := range s.record[key] {
				return []byte(k)
			}
			return nil
		}

		for count > 0 {
			val = append(val, randomVal())
			count--
		}
	}
	return
}

// SRem Remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
func (s *Set) SRem(key string, member []byte) bool {
	if !s.exist(key) {
		return false
	}
	if _, ok := s.record[key][string(member)]; ok {
		delete(s.record[key], string(member))
		return true
	}
	return false
}

// SMove Move member from the set at source to the set at destination.
// If the source set does not exist or does not contain the specified element,no operation is performed and returns 0.
func (s *Set) SMove(src, dst string, member []byte) bool {
	if !s.fieldExist(src, string(member)) {
		return false
	}

	if !s.exist(dst) {
		s.record[dst] = make(map[string]struct{})
	}
	delete(s.record[src], string(member))
	s.record[dst][string(member)] = existFlag
	return true
}

// SCard Returns the set cardinality (number of elements) of the set stored at key.
func (s *Set) SCard(key string) int {
	if !s.exist(key) {
		return 0
	}
	return len(s.record[key])
}

// SMembers Returns all the members of the set value stored at key.
func (s *Set) SMembers(key string) (val [][]byte) {
	if !s.exist(key) {
		return
	}

	for k := range s.record[key] {
		val = append(val, []byte(k))
	}

	return
}

//  SUnion Returns the members of the set resulting from the union of all the given sets.
func (s *Set) SUnion(keys ...string) (val [][]byte) {

	for _, k := range keys {
		if s.exist(k) {
			for v := range s.record[k] {
				val = append(val, []byte(v))
			}
		}
	}
	return
}

// 找出k1 中存在，而在其他k中不存在元素
// SDiff Returns the members of the set resulting from the difference between the first set and all the successive sets.
func (s *Set) SDiff(keys ...string) (val [][]byte) {
	if len(keys) == 0 || !s.exist(keys[0]) {
		return
	}
	for v := range s.record[keys[0]] {
		flag := true

		for i := 1; i < len(keys); i++ {
			if s.SIsMember(keys[i], []byte(v)) {
				flag = false
				break
			}
		}
		if flag {
			val = append(val, []byte(v))
		}
	}
	return
}

func (s *Set) SKeyExists(key string) (ok bool) {
	return s.exist(key)
}

// SClear clear the specified key in set.
func (s *Set) SClear(key string) {
	if s.SKeyExists(key) {
		delete(s.record, key)
	}
}

func (s *Set) fieldExist(key string, member string) bool {
	if !s.exist(key) {
		return false
	}
	if _, ok := s.record[key][member]; ok {
		return true
	}
	return false
}

func (s *Set) exist(key string) bool {
	if _, ok := s.record[key]; ok {
		return ok
	}
	return false
}
