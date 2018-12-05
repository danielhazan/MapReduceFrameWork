#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"

void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);


void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel);

#endif //MAPREDUCEFRAMEWORK_H
