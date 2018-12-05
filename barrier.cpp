#include "Barrier.h"

Barrier::Barrier(int numThreads)
 : mutex(PTHREAD_MUTEX_INITIALIZER)
 , cv(PTHREAD_COND_INITIALIZER)
 , count(0)
 , numThreads(numThreads)
{ }


Barrier::~Barrier()
{
	pthread_mutex_destroy(&mutex);
	pthread_cond_destroy(&cv);
}


void Barrier::barrier()
{
	pthread_mutex_lock(&mutex);
	if (++count < numThreads) {
		pthread_cond_wait(&cv, &mutex);
	} else {
		count = 0;
		pthread_cond_broadcast(&cv);
	}
	pthread_mutex_unlock(&mutex);
}
