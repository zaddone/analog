package telecar

type dar struct {
	sum,psum,n,val float64
}
func (self *dar) update (diff float64) {
	self.sum += diff
	self.n ++
	self.psum += diff*diff
}
func (self *dar)getVal() float64 {
	self.val = (self.psum/self.n) - ((self.sum*self.sum) / (self.n*self.n))
	return self.val
}
