package cpu

// CPUConfig holds configuration for CPU tests
type CPUConfig struct {
	NumCores int
	Debug    bool
	CPUList  []int
	LoadLevel  string
}

// RandomGenerator 介面定義隨機數生成器的行為
type RandomGenerator interface {
	Intn(n int) int
}

// 簡單的線性同餘隨機數生成器（避免依賴外部模組）
type SimpleRNG struct {
	seed uint64
}

// BranchTestResult 用於驗證計算結果
type BranchTestResult struct {
	Sum           int64
	XorResult     int64
	MultiplyCount uint64
	DivideCount   uint64
	ModuloCount   uint64
	ShiftCount    uint64
}
