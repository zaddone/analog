#pragma once
#include <stdio.h>
#include <stdbool.h>
#include <opencv/cv.h>
#include <opencv/highgui.h> 

unsigned int GetCurveWeight(const double *ArrX,const double *Y,const int len,const int xCols,double * Weights);

