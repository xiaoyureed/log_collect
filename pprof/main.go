package main

import (
	"math/rand"
	"os"
	"runtime/pprof"
	"time"
)

const (
	row = 10000
	col = 10000
)

//填充矩阵
func fillMatrix(m *[row][col]int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < row; i++ {
		for j := 0; j < col; j++ {
			m[i][j] = r.Intn(100000)
		}
	}
}

//模拟耗时计算
func calculate(m *[row][col]int) {
	for i := 0; i < row; i++ {
		tmp := 0
		for j := 0; j < col; j++ {
			tmp += m[i][j]
		}
	}
}
func main() {
	f, _ := os.Create("cpu.prof")
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	i := [row][col]int{}
	fillMatrix(&i)
	calculate(&i)

	memProf, _ := os.Create("mem.prof")
	defer memProf.Close()
	// 先回收一个再 dump 内存快照
	//runtime.GC()
	pprof.WriteHeapProfile(memProf)

	goroutineProf, _ := os.Create("goroutine.prof")
	defer goroutineProf.Close()
	lookup := pprof.Lookup("goroutine") // lookup 可以记录不同 tag, "goroutine"是代表协程信息的 tag
	if lookup != nil {
		lookup.WriteTo(goroutineProf, 0)
	}

}
/*
# 进入交互窗口
go tool pprof cpu.prof
	#接着输入, 查看 cpu 占用状态
	top
	#输出依次是:
	函数本身执行时间  所占比例 [sum%存疑]  函数本身时间+调用的子函数执行时间(cumulation)   所占比例
      flat  flat%   sum%        cum   cum%
     1.91s 97.95% 97.95%      1.91s 97.95%  main.fillMatrix
     0.02s  1.03% 98.97%      0.03s  1.54%  main.calculate (inline)

	# 查看指定方法每一行的详细耗时信息 (不必指定全名, 会自动做最大程度的匹配)
	list fillMatrix

 */