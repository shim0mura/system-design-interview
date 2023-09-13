package main

type BloomFilter struct {
	mapSize  int
	hashSize int
	bitmap   []bool
	hashFunc []func(string) int
}

func hashFuncGenerator(prime int, size int) func(string) int {
	return func(str string) int {
		hash := 0
		for _, v := range str {
			hash = (hash*prime + int(v)) % size
		}
		return hash
	}
}

func NewBloomFilter(m int, h int) *BloomFilter {
	funcs := make([]func(string) int, h)
	for i := 0; i < h; i++ {
		funcs[i] = hashFuncGenerator(i+10, m)
	}
	return &BloomFilter{
		mapSize:  m,
		hashSize: h,
		bitmap:   make([]bool, m),
		hashFunc: funcs,
	}
}

func (b *BloomFilter) Add(v string) {
	for _, f := range b.hashFunc {
		index := f(v)
		b.bitmap[index] = true
	}
}

func (b *BloomFilter) Contains(v string) bool {
	for _, f := range b.hashFunc {
		index := f(v)
		result := b.bitmap[index]
		if !result {
			return false
		}
	}

	return true
}
