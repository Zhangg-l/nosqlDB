package art

import (
	"reflect"
	"testing"
)

type person struct {
	age  int
	name string
}

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()

	type args struct {
		key   []byte
		value interface{}
	}

	tests := []struct {
		name     string
		art      *AdaptiveRadixTree
		args     args
		wantOldV interface{}
		wantNewV bool
	}{
		{"nil", art, args{key: nil, value: nil}, nil, false},
		{"normal-1", art, args{key: []byte("1"), value: 11}, nil, false},
		{"normal-2", art, args{key: []byte("1"), value: 22}, 11, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOldV, gotUpdate := tt.art.Put(tt.args.key, tt.args.value)
			if !reflect.DeepEqual(gotOldV, tt.wantOldV) {
				t.Errorf("Put() gotOldVal = %v, want %v", gotOldV, tt.wantOldV)
			}
			if gotUpdate != tt.wantNewV {
				t.Errorf("Put() gotUpdated = %v, want %v", gotUpdate, tt.wantNewV)
			}

		})
	}

}
func TestAdaptiveRadixTree_Get(t *testing.T) {
	tree := NewART()
	tree.Put(nil, nil)
	tree.Put([]byte("0"), 0)
	tree.Put([]byte("11"), 11)
	tree.Put([]byte("11"), "rewrite-data")

	type args struct {
		key []byte
	}
	tests := []struct {
		name string
		tree *AdaptiveRadixTree
		args args
		want interface{}
	}{
		{

			"nil", tree, args{key: nil}, nil,
		},
		{
			"zero", tree, args{key: []byte("0")}, 0,
		},
		{
			"rewrite-data", tree, args{key: []byte("11")}, "rewrite-data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tree.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}
