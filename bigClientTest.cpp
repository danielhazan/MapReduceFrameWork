/**
 * @author: Hadas Jacobi, 2018.
 *
 * This client generates 500,000 random ints in the range 0-5000.
 * It then divides them into groups according to their modulo 1000, and sums each group separately.
 *
 */

#include "MapReduceFramework.h"
#include <cstdio>
#include <cstdlib>

#define MOD 1000
#define INPUTS 500000  // Can't make it any larger or I get a segfault

class Vint : public V1 {
public:
    Vint() {}
    explicit Vint(int content) : content(content) { }
    int content;
};

class Kint : public K2, public K3{
public:
    explicit Kint(int i) : key(i) { }
    virtual bool operator<(const K2 &other) const {
        return key < static_cast<const Kint&>(other).key;
    }
    virtual bool operator<(const K3 &other) const {
        return key < static_cast<const Kint&>(other).key;
    }
    int key;
};

class Vsum : public V2, public V3{
public:
    explicit Vsum(ulong sum) : sum(sum) { }
    ulong sum;
};


class modsumClient : public MapReduceClient {
public:
    // maps to modulo MOD
    void map(const K1* key, const V1* value, void* context) const {
        int c = static_cast<const Vint*>(value)->content;
        Kint* k2;
        k2 = new Kint(c % MOD);

        Vsum* v2 = new Vsum(c);

        emit2(k2, v2, context);
    }

    // sums
    virtual void reduce(const IntermediateVec* pairs, void* context) const {
        const int key = static_cast<const Kint*>(pairs->at(0).first)->key;
        ulong sum = 0;
        for(const IntermediatePair& pair: *pairs) {
            sum += static_cast<const Vsum*>(pair.second)->sum;
            delete pair.first;
            delete pair.second;
        }
        Kint* k3 = new Kint(key);
        Vsum* v3 = new Vsum(sum);
        emit3(k3, v3, context);
    }
};


int main(int argc, char** argv)
{
    modsumClient client;
    InputVec inputVec;
    OutputVec outputVec;

    Vint inputs[INPUTS];
    for (int i = 0; i < INPUTS; i++){
        inputs[i].content = rand() % (5*MOD);
        inputVec.emplace_back(InputPair({nullptr, inputs + i}));
    }

    runMapReduceFramework(client, inputVec, outputVec, 50);

    for (OutputPair& pair: outputVec) {
        int key = ((const Kint*)pair.first)->key;
        ulong sum = ((const Vsum*)pair.second)->sum;
        printf("The sum of numbers whose mod%d == %d is %lu\n", MOD, key, sum);
        delete pair.first;
        delete pair.second;
    }

    return 0;
}
