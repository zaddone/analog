#include "cfitting.h"
#include "math.h"

bool CurveData(CvMat* Mx,CvMat* My,CvMat* Mw){

	if(Mx==NULL || My == NULL || Mw==NULL ) return false;
	CvMat *Mxs = cvCreateMat(Mx->cols,Mx->rows,CV_64FC1);
	cvTranspose(Mx,Mxs);  
	CvMat *Mul = cvCreateMat(Mxs->rows,Mx->cols,CV_64FC1);  
	cvMatMul(Mxs,Mx,Mul);
	CvMat *Mxx = cvCreateMat(Mul->rows,Mul->rows,CV_64FC1);
//	cvInvert(Mul,Mxx); 
	cvInvert(Mul,Mxx,CV_SVD);
	CvMat *Mxy = cvCreateMat(Mxs->rows,My->cols,CV_64FC1);
	cvMatMul(Mxs,My,Mxy);  
	
	cvMatMul(Mxx,Mxy,Mw);
	
	cvReleaseMat(&Mxs);
	cvReleaseMat(&Mul);
	cvReleaseMat(&Mxx);
	cvReleaseMat(&Mxy);
	return true;

}
unsigned int GetCurveWeight(const double *ArrX,const double *Y,const int len,const int xCols,double * Weights){
	CvMat* My = cvCreateMat(len,1,CV_64FC1);
	CvMat* Mx = cvCreateMat(len,xCols,CV_64FC1);
	int i,j,be;
	double a,_a;
	for(i = 0;i<len;i++){
		My->data.db[i] = Y[i];
		a = _a = ArrX[i];
		be = i * xCols;
		Mx->data.db[be] = 1;
		Mx->data.db[be+1] = a;
		for (j = 2; j < xCols ; j++){
			a = a * _a;
			if (isnan(a)){
				cvReleaseMat(&Mx);
				cvReleaseMat(&My);
				return 0;
			}
			Mx->data.db[be+j] = a;
		}
	}
	CvMat *Mw = cvCreateMat( xCols,1,CV_64FC1);
	CurveData(Mx,My,Mw);
	memcpy( Weights,Mw->data.db, sizeof(double)*xCols);
	cvReleaseMat(&Mx);
	cvReleaseMat(&My);
	cvReleaseMat(&Mw);
	return 1;
	
}

