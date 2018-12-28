package fitting
/*
#cgo LDFLAGS: -L. -lopencv_core
#include "cfitting.h"
*/
import "C"
import (
	"unsafe"
	"math"
)

func GetCurveFittingWeight(X []float64,Y []float64, W []float64) bool {
	_x := (*C.double)(unsafe.Pointer(&X[0]))
	_y := (*C.double)(unsafe.Pointer(&Y[0]))
	Len := C.int(len(X))
	Max := C.int(len(W))
	_w := (*C.double)(unsafe.Pointer(&W[0]))
	Out := C.GetCurveWeight(_x,_y,Len,Max,_w)
	if Out != 0 {
		//fmt.Println(W)
		return true
	}
	return false
}

func Rounding(val float64) float64 {
	x,y:= math.Modf(val)
	if y>0.4 {
		x++
	}
	return x
}
func CheckCurveFitting(X []float64,Y []float64,W []float64) (valErr float64) {
	var tmpY,_x float64
	for i,x := range X {
		tmpY = W[0] + W[1]*x
		_x = x
		for _,w := range W[2:]{
			_x *= x
			tmpY += _x * w
		}
		valErr += math.Pow((Y[i] - tmpY),2)
	}
	return valErr/float64(len(X))
}
