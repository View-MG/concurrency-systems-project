// io.c  peripheral for iot-rz
//   19 nov 2016

#include "s21.h"
#include <math.h>
#include <random.h>

#define SINAMP    	50			// sin amplitude
#define SINLEN		1000		// sin period
#define SINMUL		2			// 10x period multiplier
#define DIGAMP		5			// square amplitude
#define DIGLEN		1000		// square period
#define DIGMUL		1			// 2x peroid multiplier
#define TIMER0DEL	150			// default timer0range
#define TIMER1DEL	200			// default timer1range

extern int clock;
extern int intmask[], intrq[];

int sinbuf[SINLEN], sinport;
int digbuf[DIGLEN], digport;
int timer0, timer1, timer0range, timer1range;

void gensin(void){
	double z, x;
	int i, s;
	z = 0.0;
	for(i = 0; i < SINLEN; i++){
		x = sin(z);					// in radian
		z = z + 0.00628318;
		x = x * (double)SINAMP;
		sinbuf[i] = (int)x;
	}
}

void showsin(void){
	int i;
	for(i = 0; i < SINLEN; i++){
		printf("%d, ",sinbuf[i]);
		if(i%10 == 0) printf("\n");
	}
}

int readsinport(void){
	int idx;
	idx = (clock / SINMUL) % SINLEN;
	sinport = sinbuf[idx];
	return sinport;
}

void gendig(void){
	int i, m;
	m = DIGLEN / 2;
	for(i = 0; i < m; i++)
		digbuf[i] = DIGAMP;
	for(i = m; i < DIGLEN; i++)
		digbuf[i] = 0;
}

void showdig(void){
	int i;
	for(i = 0; i < DIGLEN; i++){
		if(i%10 == 0) printf("%d ",digbuf[i]);
	}
}

int readdigport(void){
	int idx;
	idx = (clock / DIGMUL) % DIGLEN;
	digport = digbuf[idx];
	return digport;
}

void init_io(void){
	gensin();
//	showsin();
	gendig();
	mt_init_genrand(0);
	timer0range = TIMER0DEL;
	timer1range = TIMER1DEL;
	timer0 = 0;
	timer1 = 0;
}

int randomseed = 0;

int myrandom(void){
	randomseed = genrand_int31();
	return randomseed;
}

void simdevices(void){
	timer0++;
	timer0 %= timer0range;
	if( timer0 == 0 ) intrq[0] = 1;
	timer1++;
	timer1 %= timer1range;
	if( timer1 == 0 ) intrq[1] = 1;
}


