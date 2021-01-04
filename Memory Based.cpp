// Stage1 Memory Based.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include <stdint.h>
#include <vector> 
#include<algorithm>

const uint32_t N_MEMORY_BLOCKS = 15;
const uint32_t N_HASH_BUCKETS = N_MEMORY_BLOCKS - 1;
const uint32_t BLOCK_SIZE_IN_TUPPLE = 8;



struct sTuple {
    uint32_t B;
    int32_t C;
};


struct rTuple {
    int32_t A;
    uint32_t B;
};

struct jTuple {
    int32_t A;
    uint32_t B;
    int32_t C;
};



std::vector<sTuple> relationS;
std::vector<rTuple> relationR;

std::vector<sTuple> relationSHash[N_HASH_BUCKETS];
std::vector<rTuple> relationRHash[N_HASH_BUCKETS];



std::vector<sTuple> sMemory;
std::vector<rTuple> rMemory;

std::vector<sTuple> sHashMemory[N_HASH_BUCKETS];
std::vector<rTuple> rHashMemory[N_HASH_BUCKETS];

std::vector<jTuple> joinedResult;


uint32_t getARandomNumber(uint32_t min_, uint32_t max_) // max not included
{
    uint32_t range = max_ - min_;
    uint32_t r1 = rand();
    uint32_t r2 = rand();
    return ((r1 * r2) % range) + min_; // to randoms are used to extend the range of the random number. the rand has a max of 32767
}




uint32_t nIoReads;
uint32_t nIoWrites;





////////// S block //////////
void printRelationS(std::vector<sTuple>& relationS_, bool printIndex)
{
    if (printIndex)
    {
        printf("index:  ");
    }
    printf("   B  |    C   \n");

    for (uint32_t i = 0; i < relationS_.size(); i++)
    {
        if (printIndex)
        {
            printf("%5d:  ", i);
        }
        printf("%6d| %6d\n", relationS_[i].B, relationS_[i].C);
    }
}



void generateSRelation(std::vector<sTuple>& relationS_, uint32_t nTuples_, uint32_t min_, uint32_t max_)
{
    relationS_.erase(relationS_.begin(), relationS_.end());
    relationS_.reserve(nTuples_);
    for (uint32_t i = 0; i < nTuples_; i++)
    {
        // printf("this is i: %d\n", i);
        int32_t randomBValue = getARandomNumber(min_, max_);
        int32_t randomNumber = getARandomNumber(min_, max_);
        sTuple temp;
        temp.B = randomBValue;
        temp.C = -randomNumber;
        relationS_.push_back(temp);
    }

}
////////// END: S block //////////







////////// R block //////////
void printRelationR(std::vector<rTuple>& relationR_, bool printIndex)
{
    if (printIndex)
    {
        printf("index:  ");
    }
    printf("   B  |    C   \n");

    for (uint32_t i = 0; i < relationR_.size(); i++)
    {
        if (printIndex)
        {
            printf("%5d:  ", i);
        }
        printf("%6d| %6d\n", relationR_[i].A, relationR_[i].B);
    }
}



void generateRRelationType1(std::vector<rTuple> &relationR_, std::vector<sTuple> &relationS_, uint32_t nTuples_)
{
    relationR_.erase(relationR_.begin(), relationR_.end());
    relationR_.reserve(nTuples_);
    for (uint32_t i = 0; i < nTuples_; i++)
    {
        // printf("this is i: %d\n", i);
        int32_t randomNumber = getARandomNumber(0, 1000);
        uint32_t randomIndex = getARandomNumber(0, relationS_.size());
        uint32_t bValue = relationS_[randomIndex].B;
        rTuple temp;
        temp.A = -randomNumber;
        temp.B = bValue;
        relationR_.push_back(temp);
    }
}



void generateRRelationType2(std::vector<rTuple>& relationR_, uint32_t nTuples_)
{
    relationR_.erase(relationR_.begin(), relationR_.end());
    relationR_.reserve(nTuples_);

    uint32_t minB = 20000, maxB = 30000 + 1;
    for (uint32_t i = 0; i < nTuples_; i++)
    {
        // printf("this is i: %d\n", i);
        int32_t randomNumber = getARandomNumber(0, 1000);
        uint32_t randomBValue = getARandomNumber(minB, maxB);
        rTuple temp;
        temp.A = -randomNumber;
        temp.B = randomBValue;
        relationR_.push_back(temp);
    }
}
////////// END: R block //////////




////////// Disk and Memory Block Read/Write //////////
template <class tuppleType>
uint32_t pushToMemFromDisk(tuppleType& diskTupples, uint32_t diskBlockIndex, tuppleType& memTupples)
{
    uint32_t startOfDiskBlock = diskBlockIndex * BLOCK_SIZE_IN_TUPPLE;
    uint32_t nTupplesToRead = BLOCK_SIZE_IN_TUPPLE;

    if (diskTupples.size() >= startOfDiskBlock)
    {
        if (diskTupples.size() - startOfDiskBlock < BLOCK_SIZE_IN_TUPPLE)
        {
            nTupplesToRead = diskTupples.size() - startOfDiskBlock;
        }
    }
    else
    {
        printf("\n!!!!!!!!!Wanna read from places that do not exist, size: %d, startOfMemBlock: %d, nTupplesToWrite%d!!!!!!!!!\n\n", diskTupples.size(), startOfDiskBlock, nTupplesToRead);
        return 0;
    }

    if (nTupplesToRead > 0)
    {
        nIoReads++;
    }
    
    std::copy_n(diskTupples.begin() + startOfDiskBlock, nTupplesToRead, std::back_inserter(memTupples)); // memTupples.begin() + startOfMemBlock
    return nTupplesToRead; 
}



template <class tuppleType>
uint32_t pushToMemFromDisk(tuppleType& diskTupples, uint32_t diskBlockIndex, tuppleType& memTupples, uint32_t nBlocks)
{
    uint32_t nTupplesRead = 0;

    while (nBlocks > 0)
    {
        nTupplesRead += pushToMemFromDisk(diskTupples, diskBlockIndex, memTupples);
        nBlocks--;
        diskBlockIndex++;
    }

    return nTupplesRead;
}



template <class tuppleType>
uint32_t pushToDiskFromMem(tuppleType& memBlock, uint32_t memBlockIndex, tuppleType& diskBlock)
{
    uint32_t startOfMemBlock = memBlockIndex * BLOCK_SIZE_IN_TUPPLE;

    uint32_t nTupplesToWrite = BLOCK_SIZE_IN_TUPPLE;

    if (memBlock.size() >= startOfMemBlock)// >= for taking care of zero without generating error
    {
        if (memBlock.size() - startOfMemBlock < BLOCK_SIZE_IN_TUPPLE)
        {
            nTupplesToWrite = memBlock.size() - startOfMemBlock;
        }
    }
    else
    {
        printf("\n!!!!!!!!!Wanna write from places that do not exist, size: %d, startOfMemBlock: %d, nTupplesToWrite%d!!!!!!!!!\n\n", memBlock.size(), startOfMemBlock, nTupplesToWrite);
        return 0;
    }

    if (nTupplesToWrite > 0)
    {
        nIoWrites++;
    }

    std::copy_n(memBlock.begin() + startOfMemBlock, nTupplesToWrite, std::back_inserter(diskBlock)); // diskBlock.begin() + startOfMemBlock
    return nTupplesToWrite;
}



template <class tuppleType>
uint32_t pushToDiskFromMem(tuppleType& memBlock, uint32_t memBlockIndex, tuppleType& diskBlock, uint32_t nBlocks)
{
    uint32_t nTupplesWritten = 0;
    while (nBlocks > 0)
    {
        nTupplesWritten += pushToDiskFromMem(memBlock, memBlockIndex, diskBlock);
        nBlocks--;
        memBlockIndex++;
    }
    return nTupplesWritten;
}
////////// END: Disk and Memory Block Read/Write //////////




void computeHashParameters(uint32_t nBucket, uint32_t min, uint32_t max, uint32_t& offset, uint32_t& hashDevider) // max must not be included in range
{
    uint32_t delta = max - min;
    hashDevider = delta / nBucket;
    if (delta % nBucket != 0)
    {
        hashDevider++;
    }
    offset = min;
}


uint32_t getHashCode(uint32_t input, uint32_t offset, uint32_t hashDevider) // max must not be included in range
{
    if (input < offset)
    {
        // printf("\n!!!!!!!!!Your key is below given range!!!!!!!!!\n\n");
        return 0xFFFFFFFF;
    }
    return (input - offset) / hashDevider;
}





template <class tuppleType>
uint32_t hashRelation(tuppleType& relationOnDisk, tuppleType& relationOnMem, tuppleType hashOnDisk[], tuppleType hashOnMem[], uint32_t offset, uint32_t hashDevider)
{
    // setting the Blocks to maintain N_MEMORY_BLOCKS
    relationOnMem.clear();

    for (uint32_t i = 0; i < N_HASH_BUCKETS; i++)
    {
        hashOnDisk[i].clear();
        hashOnMem[i].clear();
    }

    // hashing started
    uint32_t nTupplesHashed = 0;
    uint32_t nBlocksRead = 0;
    bool hashingEnded = false;
    while (!hashingEnded)
    {
        uint32_t nTupplesRead = pushToMemFromDisk(relationOnDisk, nBlocksRead, relationOnMem);
        nBlocksRead++;
        nTupplesHashed += nTupplesRead;
        // printf("read %d tuples from disk.\n", nTupplesRead);

        for (uint32_t i = 0; i < nTupplesRead; i++)
        {
            uint32_t hashCode = getHashCode(relationOnMem[i].B, offset, hashDevider);

            // printf("Got Hash Code of %d for %d.\n", hashCode, relationOnMem[i].B);

            if (hashCode >= N_HASH_BUCKETS)
            {
                // printf("\n!!!!!!!!!Your key is above given range!!!!!!!!!\n\n");
                // hashCode = N_HASH_BUCKETS - 1;
                continue;
            }

            if (hashOnMem[hashCode].size() >= BLOCK_SIZE_IN_TUPPLE)
            {
                // printf("pushing bucket %d to disk.\n", hashCode);
                pushToDiskFromMem(hashOnMem[hashCode], 0, hashOnDisk[hashCode]);
                hashOnMem[hashCode].clear();
            }

            hashOnMem[hashCode].push_back(relationOnMem[i]);
        }

        relationOnMem.clear();

        if (nTupplesRead < BLOCK_SIZE_IN_TUPPLE)
        {
            hashingEnded = true;
        }
    } 
    // hashing has ended

    // Pushing the last hashed tuples in memory to disk
    for (uint32_t i = 0; i < N_HASH_BUCKETS; i++)
    {
        // printf("pushing bucket %d to disk. it has %d tupples\n", i, hashOnMem[i].size());
        pushToDiskFromMem(hashOnMem[i], 0, hashOnDisk[i]);
        hashOnMem[i].clear();
    }

    return nTupplesHashed;
}





// assumes r is smaller and fits in the memory
uint32_t naturalJoin(std::vector<sTuple> sHashOnDisk[], std::vector<sTuple>& sStorageOnMem, std::vector<rTuple> rHashOnDisk[], std::vector<rTuple>& rStorageOnMem, std::vector<jTuple>& result)
{
    result.clear();


    uint32_t nTotalMatchingTuple = 0;
    for (uint32_t i = 0; i < N_HASH_BUCKETS; i++)
    {
        sStorageOnMem.clear();
        rStorageOnMem.clear();


        printf("Hash bucket[%d]: ", i);


        // reading R bucket
        uint32_t nRTupplesInMem = 0;
        uint32_t nBlocksRead = 0;
        bool endOfLoadingR = false;
        while (!endOfLoadingR)
        {
            uint32_t nTupplesRead = pushToMemFromDisk(rHashOnDisk[i], nBlocksRead, rStorageOnMem);
            nBlocksRead++;
            nRTupplesInMem += nTupplesRead;
            if (nTupplesRead < BLOCK_SIZE_IN_TUPPLE)
            {
                endOfLoadingR = true;
            }
        }
        // end of reading R bucket

        printf("%d R tupples were moved. ", nRTupplesInMem);


        // processing S bucket
        uint32_t nSTupplesProcessed = 0;
        uint32_t nMatchingTupleInBucket = 0;
        nBlocksRead = 0;
        bool endOfProcessingSTupples = false;
        while (!endOfProcessingSTupples)
        {
            uint32_t nTupplesRead = pushToMemFromDisk(sHashOnDisk[i], nBlocksRead, sStorageOnMem);
            nBlocksRead++;
            nSTupplesProcessed += nTupplesRead;

            for (uint32_t j = 0; j < nTupplesRead; j++)
            {
                for (uint32_t k = 0; k < nRTupplesInMem; k++) // rStorageOnMem.size() = nRTupplesInMem
                {
                    if (sStorageOnMem[j].B == rStorageOnMem[k].B)
                    {
                        jTuple temp;
                        temp.A = rStorageOnMem[k].A;
                        temp.B = rStorageOnMem[k].B;
                        temp.C = sStorageOnMem[j].C;
                        result.push_back(temp);
                        nMatchingTupleInBucket++;
                    }
                }
            }

            sStorageOnMem.clear();

            if (nTupplesRead < BLOCK_SIZE_IN_TUPPLE)
            {
                endOfProcessingSTupples = true;
            }
        }
        nTotalMatchingTuple += nMatchingTupleInBucket;
        // End of processing S bucket

        printf("%d S tupples were processed, finding %d matching tuple. Total Matching: %d.\n", 
            nSTupplesProcessed, nMatchingTupleInBucket, nTotalMatchingTuple);
    }

    return nTotalMatchingTuple;
}



void check20RandomBValues(std::vector<sTuple>& sRelation, std::vector<jTuple>& naturalJoin)
{
    printf("\n\n");
    for (uint32_t i = 0; i < 20; i++)
    {
        uint32_t randomlySelectedRtuple = getARandomNumber(0, sRelation.size());
        uint32_t selectedBValue = sRelation[randomlySelectedRtuple].B;

        printf("\nTest %d: B value selected is %d.", i, selectedBValue);
        uint32_t nTuplesFound = 0;
        for (uint32_t j = 0; j < naturalJoin.size(); j++)
        {
            if (naturalJoin[j].B == selectedBValue)
            {
                if (nTuplesFound == 0)
                {
                    printf("\nindex:     A  |   B  |    C   \n");
                }
                printf("%5d:  %6d| %6d| %6d\n", j, naturalJoin[j].A, naturalJoin[j].B, naturalJoin[j].C);
                nTuplesFound++;
            }
        }

        if (nTuplesFound == 0)
        {
            printf("But no matching B value was found in natural join of R and S.\n");
        }
    }
}




void printJoinResult(std::vector<jTuple>& joinResult, bool printIndex)
{
    if (printIndex)
    {
        printf("index:  ");
    }
    printf("   A  |   B  |    C   \n");

    for (uint32_t i = 0; i < joinResult.size(); i++)
    {
        if (printIndex)
        {
            printf("%5d:  ", i);
        }
        printf("%6d| %6d| %6d\n", joinResult[i].A, joinResult[i].B, joinResult[i].C);
    }
}





int main()
{
    uint32_t minKey = 10000;
    uint32_t maxKey = 50000 + 1; // max is not included in any of the functions so + 1


    uint32_t hashOffset;
    uint32_t hashDevider;


    std::cout << "Experiment 5.1:\n";
    nIoReads = 0;
    nIoWrites = 0;
    generateSRelation(relationS, 5000, minKey, maxKey);
    std::cout << "Relation S generated with 500 tupples.\n";
    
    generateRRelationType1(relationR, relationS, 1000);
    std::cout << "Relation R is generated with 1000 tuples.\n";

    computeHashParameters(N_HASH_BUCKETS, minKey, maxKey, hashOffset, hashDevider);
    std::cout << "\nHash Offset: " << hashOffset <<", Hash Devider: "<< hashDevider <<".\n";


    uint32_t nSTupplesHashed = hashRelation(relationS, sMemory, relationSHash, sHashMemory, hashOffset, hashDevider);
    std::cout << "\nHashed S, " << nSTupplesHashed << " tuples were hashed.\n";
    for (uint32_t i = 0; i < N_HASH_BUCKETS; i++)
    {
        std::cout << "Bucket[" << i << "]: (" << (i * hashDevider) + hashOffset << ", " 
            << ((i + 1) * hashDevider) + hashOffset << "), with " << relationSHash[i].size() << " tuples\n";
    }


    uint32_t nRTupplesHashed = hashRelation(relationR, rMemory, relationRHash, rHashMemory, hashOffset, hashDevider);
    std::cout << "\nHashed R, " << nRTupplesHashed << " tuples were hashed.\n";
    for (uint32_t i = 0; i < N_HASH_BUCKETS; i++)
    {
        std::cout << "Bucket[" << i << "]: [" << (i * hashDevider) + hashOffset << ", " 
            << ((i + 1) * hashDevider) + hashOffset << "), with " << relationRHash[i].size() << " tuples\n";
        if (relationRHash[i].size() > BLOCK_SIZE_IN_TUPPLE * (N_MEMORY_BLOCKS - 1)) // one block is for reading tupples from S
        {
            std::cout << "\n\n!!!!!!!!! We have a problem, R's bucket has more tupples than we can fit in memory!!!!!!!!!\n\n";
        }
    }

    std::cout << "\nStarting the natural joint operation on R (Smaller one) and S.\n";
    uint32_t matchingTuples = naturalJoin(relationSHash, sMemory, relationRHash, rMemory, joinedResult);

    std::cout << "\n\n\n>>>>   " << matchingTuples << " tuples are in natural join results. Total reads: " << nIoReads 
        << ", Total writes: " << nIoWrites << ", Total Operations: " << nIoReads + nIoWrites  << "\n";

    check20RandomBValues(relationS, joinedResult);


    std::cout << "Experiment 5.2:\n";
    nIoReads = 0;
    nIoWrites = 0;

    uint32_t minHash = 20000, maxHash = 30000 + 1; // hashing based on R since it is smaller and would have matter more for having balanced buckets


    std::cout << "Using smae relation S as 5.1 with 5000 tuples.\n";

    generateRRelationType2(relationR, 1200);
    std::cout << "New relation R is generated with 1200 tuples.\n";

    computeHashParameters(N_HASH_BUCKETS, minHash, maxHash, hashOffset, hashDevider);
    std::cout << "\nHash Offset: " << hashOffset << ", Hash Devider: " << hashDevider << ".\n";


    nSTupplesHashed = hashRelation(relationS, sMemory, relationSHash, sHashMemory, hashOffset, hashDevider);
    std::cout << "\nHashed S, " << nSTupplesHashed << " tuples were hashed.\n";
    for (uint32_t i = 0; i < N_HASH_BUCKETS; i++)
    {
        std::cout << "Bucket[" << i << "]: (" << (i * hashDevider) + hashOffset << ", "
            << ((i + 1) * hashDevider) + hashOffset << "), with " << relationSHash[i].size() << " tuples\n";
    }


    nRTupplesHashed = hashRelation(relationR, rMemory, relationRHash, rHashMemory, hashOffset, hashDevider);
    std::cout << "\nHashed R, " << nRTupplesHashed << " tuples were hashed.\n";
    for (uint32_t i = 0; i < N_HASH_BUCKETS; i++)
    {
        std::cout << "Bucket[" << i << "]: [" << (i * hashDevider) + hashOffset << ", "
            << ((i + 1) * hashDevider) + hashOffset << "), with " << relationRHash[i].size() << " tuples\n";
        if (relationRHash[i].size() > BLOCK_SIZE_IN_TUPPLE * (N_MEMORY_BLOCKS - 1)) // one block is for reading tupples from S
        {
            std::cout << "\n\n!!!!!!!!! We have a problem, R's bucket has more tupples than we can fit in memory!!!!!!!!!\n\n";
        }
    }

    std::cout << "\nStarting the natural joint operation on R (Smaller one) and S.\n";
    matchingTuples = naturalJoin(relationSHash, sMemory, relationRHash, rMemory, joinedResult);

    std::cout << "\n\n\n>>>>   " << matchingTuples << " tuples are in natural join results. Total reads: " << nIoReads
        << ", Total writes: " << nIoWrites << ", Total Operations: " << nIoReads + nIoWrites << "\n";

    std::cout << "\n\n\n Natural Join Results:\n";
    printJoinResult(joinedResult, true);
}

// Run program: Ctrl + F5 or Debug > Start Without Debugging menu
// Debug program: F5 or Debug > Start Debugging menu

// Tips for Getting Started: 
//   1. Use the Solution Explorer window to add/manage files
//   2. Use the Team Explorer window to connect to source control
//   3. Use the Output window to see build output and other messages
//   4. Use the Error List window to view errors
//   5. Go to Project > Add New Item to create new code files, or Project > Add Existing Item to add existing code files to the project
//   6. In the future, to open this project again, go to File > Open > Project and select the .sln file
