//
// Created by daniel hazan on 6/1/2018.
//

#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <pthread.h>
#include <vector>
#include <map>
#include <iostream>
#include <algorithm>
#include "Barrier.h"
#include <atomic>
#include <semaphore.h>

//-------------------data structures-------------------------//
/**the global vector containing all sub-vectors of <k2,v2> pairs, which are belonging
to the data mapped by Map-Phase threads, with id @pthread_t and mutex @pthread_mutex_t*
 */
typedef std::vector<std::pair<pthread_t, std::pair<IntermediateVec*,pthread_mutex_t *>>>
        MapPhaseVec;
/**the global vector containing all sub-vectors of <k3,v3> pairs, which are belonging
to the data reduced by Reduce-Phase threads, with id @pthread_t and mutex @pthread_mutex_t*
 */
typedef std::vector<std::pair<pthread_t ,OutputVec*>> ReducePhaseVec;
/**the container to which the shuffling thread inserts and merges all the sorted-sub-vectors
 * produced by the Map-phase threads
 * */
typedef std::map<K2*,std::vector<V2*>,K2cmp> ShuffledMapOut;



//the context saves the main global data structures and mutexes
typedef struct globalContext
{
    std::vector<IntermediateVec> vVector;
    std::vector<IntermediateVec> ShuffledQueue;
    MapReduceClient& client;
    Barrier barrier;
    OutputVec outputVec;
    bool mappingFinished;
    bool reductionFinished;
    bool startShuffle;
    bool stopShuffle;
    pthread_mutex_t  shuffled_mutex;
    pthread_mutex_t  activeTrMutex;
    sem_t shuffleSem;

    InputVec inputvector;

    /*
    unsigned int index;
    InputVec inputvector ;
    ShuffledMapOut shuffledMap;
    ShuffledMapOut::iterator shufflediter;
    IntermediateVec *CurrThreadInterVec;
    MapPhaseVec mapPhaseVec;
    ReducePhaseVec reducePhaseVec;
    std::vector<pthread_mutex_t *> mutexVector;
    std::vector<pthread_mutex_t *>::iterator mutexIter;
    OutputVec resultedOutLists;
    bool mappingFinished;
    bool reductionFinished;
    MapReduceClient& client;
    Barrier barrier;//barrier to wait for shuffle phase
*/
}Gcontext;

typedef struct ThreadContext{
    std::vector<IntermediatePair> trMapPhaseVec;
    std::atomic<int>* atomic_counter;
    Gcontext* globalContext;
    pthread_mutex_t  shuffled_mutex;
    pthread_mutex_t  activeTrMutex;

    pthread_cond_t conditionalVar ;

}Tcontext;

//comparison operator for shuffling and sorting after the Map-phase

bool K2cmp()(const IntermediatePair a, const IntermediatePair b) const
{
    return *a.first < *b.first;
}


//----------------------mutexes-------------------
// /*todo*/
//--maybe add the mutex objects to the context struct???


//cv - use for signaling to barrier each time a map' thread finish its task - for the shuffle phase
// to start
//pthread_cond_t mutexCondition = PTHREAD_COND_INITIALIZER;

//mutex to protect the active thread
//pthread_mutex_t activeThreadMutex = PTHREAD_MUTEX_INITIALIZER;

//mutex to protect the index used for chunk data
//pthread_mutex_t indexMutex = PTHREAD_MUTEX_INITIALIZER;

void startFramework(int multiThreadLevel, ThreadContext* context, const InputVec& inputVec,
                    const MapReduceClient& client,OutputVec& out)
{
    std::atomic<int> atomic_counter(0);
    context->atomic_counter = &atomic_counter;
    context->globalContext->outputVec = out;
    context->globalContext->mappingFinished = false;
    context->globalContext->reductionFinished = false;
    context->globalContext->startShuffle = false;
    context->globalContext->stopShuffle = false;

    //context->mutexVector = std::vector<pthread_mutex_t *>();
    //context->mapPhaseVec = MapPhaseVec();
    //context->reducePhaseVec = ReducePhaseVec();
    //context->mapPhaseVec.reserve(multiThreadLevel);
    //context->reducePhaseVec.reserve(multiThreadLevel);
    //context->shuffledMap = ShuffledMapOut();

    context->globalContext->inputvector = inputVec;
    context->globalContext->client = client;
    context->globalContext->barrier = Barrier(multiThreadLevel);
    context->shuffled_mutex = PTHREAD_MUTEX_INITIALIZER;
    context->activeTrMutex = PTHREAD_MUTEX_INITIALIZER;
    context->conditionalVar =  PTHREAD_COND_INITIALIZER;


}

void* ExecReduce(void* context1)
{
    //same as in mapExecute - calling client.reduce on each pair in each vector from the
    // shuffledQueue
}


/**
 * this methods runs when thread is created with function points to here.
 * in this pahse the thread creted merges all the sub- vectors created by other threads
 * during Map phase to one vector
 * */
void* ExecShuffle(void* context1)
{
    auto  Shuffledcontext = (ThreadContext*) context1;
    std::vector<IntermediatePair> tempVec ;
    tempVec = NULL;

    while(!(Shuffledcontext->globalContext->vVector.empty()))
    {


        /*
        K2 maxKeyVal;
        for(std::vector<IntermediatePair> vec : Shuffledcontext->globalContext->vVector)
        {
            if(!(vec.empty()))
            {
                if(maxKeyVal< *vec.back().first)
                {
                    maxKeyVal = *vec.back().first;
                }
            }
        }*/
        if(!tempVec.empty())
        {
            tempVec.clear();
        }
        for(std::vector<IntermediatePair> vec : Shuffledcontext->globalContext->vVector)
        {
            if(!(vec.empty()))
            {
                /*
                if(maxKeyVal< *(vec.back().first) && *(vec.back().first) < maxKeyVal)
                {
                    tempVec.push_back(std::move(vec.back()));
                    vec.pop_back();
                }*/
                tempVec.push_back(std::move(vec.back()));
                vec.pop_back();
            }
        }
        pthread_mutex_lock(&(Shuffledcontext->shuffled_mutex));
        Shuffledcontext->globalContext->ShuffledQueue.push_back(tempVec);
        pthread_mutex_unlock(&(Shuffledcontext->shuffled_mutex));

        //start reducePhase at the time a new vector is inserted to the Shuffled vector
        sem_post(&(Shuffledcontext->globalContext->shuffleSem));

    }
    Shuffledcontext->globalContext->stopShuffle = true;

}

/**
 * this methods runs when thread is created with function points to here.
 * in this phase each thread takes a chunk of data and call "map" function implemented
 * by the client, and save it in the thread's own vectors( using context)
 * */
void* ExecMap(void* context1)
{

    auto  Mapcontext = (ThreadContext*) context1;

    //MapPhaseVec* lvltwoVec = new MapPhaseVec();

    pthread_mutex_lock(&(Mapcontext->activeTrMutex));
    while(!(Mapcontext->globalContext->mappingFinished))
    {
        int oldVal = Mapcontext->atomic_counter->load();
        if (oldVal < Mapcontext->globalContext->inputvector.size())
        {
            Mapcontext->globalContext->client.map(
                    (Mapcontext->globalContext->inputvector[oldVal]).first,
                    (Mapcontext->globalContext->inputvector[oldVal]).second, Mapcontext);
            (*(Mapcontext->atomic_counter))++;
        }
        else
        {
            Mapcontext->globalContext->mappingFinished = true;
            Mapcontext->globalContext->startShuffle = true;

        }

    }

    std::sort(Mapcontext->trMapPhaseVec.begin(),Mapcontext->trMapPhaseVec.end(),K2cmp);
    Mapcontext->globalContext->vVector.push_back(Mapcontext->trMapPhaseVec);
    pthread_cond_signal(&(Mapcontext->conditionalVar));//!!!send to the barrier in wait()

    pthread_mutex_unlock(&(Mapcontext->activeTrMutex));


}


void emit2 (K2* key, V2* value, void* context)
{
    auto Mapcontext = (ThreadContext*) context;
    Mapcontext->trMapPhaseVec.push_back(IntermediatePair{key,value});
}


void emit3 (K3* key, V3* value, void* context)
{
    auto Mapcontext = (Gcontext*) context;
    Mapcontext->outputVec.push_back(OutputPair{key,value});
}

void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel)
{
    ThreadContext* Mycontext;
    startFramework(multiThreadLevel,Mycontext, inputVec,client,outputVec);
    pthread_t shuffledThread;
    //create Array of threads that will execute the Map
    pthread_t *threadMapArray = new pthread_t[multiThreadLevel];
    pthread_t *threadReduceArray = new pthread_t[multiThreadLevel];
    /*
    for(int i =0; i<multiThreadLevel; ++i)
    {
        pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
        Mycontext->mutexVector.push_back(&mutex);
    }
    Mycontext->mutexIter = Mycontext->mutexVector.begin();
*/
    for(int i = 0;i<multiThreadLevel;++i)
    {
        if(pthread_create(&threadMapArray[i],NULL,ExecMap,(void*) &Mycontext))
        {
            //error
        }
    }
    //now wait for all threads to finish their Map tasks and start to sort/shuffle the
    // intermidiate vector (LevelTwoVector)
    for(int i = 0;i<multiThreadLevel;++i)
    {
        if(pthread_join(threadMapArray[i],NULL))
        {
            //error
        }
    }

    //now shuffle, and start reduce-->
    //at this moment all threads finished the map phase and each one sorted its own
    // LevelTwoVector so call the barrier to wait all thread finish mapping and broadcast

    Mycontext->globalContext->barrier.barrier();

    /*todo*/ //implement shuffle and reduce using semaphores!!

    if(Mycontext->globalContext->startShuffle)
    {
        pthread_create(&shuffledThread, NULL, ExecShuffle, (void *) &Mycontext);

        for(int i = 0; i<multiThreadLevel-1; i++)
        {
            if(!(Mycontext->globalContext->ShuffledQueue.empty()))
            {
                if((Mycontext->globalContext->stopShuffle) == true)
                {
                    pthread_create(&shuffledThread, NULL, ExecReduce, (void *) &Mycontext);

                }
                pthread_create(&threadReduceArray[i], NULL, ExecReduce, (void *) &Mycontext);
                sem_wait((&(Mycontext->globalContext->shuffleSem)));
            }
        }
    }

}
