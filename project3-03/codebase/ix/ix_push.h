#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

#define IX_TYPE_LEAF     0
#define IX_TYPE_INTERNAL 1

// Headers and data types

// Leaf nodes contain pointers to prev and next nodes in linked list of leafs
// Also contain number of keys within and pointer to free space
// 0 is always meta node, so a 0 value for next/prev is like NULL
typedef struct LeafHeader
{
	uint32_t next;
	uint32_t prev;
	uint16_t numberOfSlots;
	uint16_t freeSpaceOffset;
} LeafHeader;

typedef struct DataEntry
{
    // not using union in print
    union
    {
        int32_t integer;
        float real;
        int32_t varcharOffset;
    };
    RID rid;
} DataEntry;

// each entry has offset to key and link to child
typedef struct IndexEntry
{
    //union
    //{
        int32_t integer;
        float real;
        int32_t varcharOffset;
    //};
	uint32_t childPage;
} IndexEntry;

// Internal nodes contain number of keys and pointer to free space
typedef struct InternalHeader
{
	uint16_t numberOfSlots;
	uint16_t freeSpaceOffset;
    uint32_t leftChildPage;
} InternalHeader;

class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        friend class IX_ScanIterator;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;

        void printBtree_rec(IXFileHandle &ixfileHandle, string prefix, const int32_t currPage, const Attribute &attr) const;
        void printInternalNode(IXFileHandle &, void *pageData, const Attribute &attr, string prefix) const;
        void printInternalSlot(const Attribute &attr, const int32_t slotNum, const void *data) const;
        void printLeafNode(void *pageData, const Attribute &attr) const;

        DataEntry getDataEntry(const int slotNum, const void *pageData) const;

        int compareLeafSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const;
        // Returns -1, 0, or 1 if key is less than, equal to, or greater than value
        int compare(const int key, const int value) const;
        int compare(const float key, const float value) const;
        int compare(const char *key, const char *value) const;
};

class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;


    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC readPage(PageNum pageNum, void *data);
    RC appendPage(const void *data);

    friend class IndexManager;
	private:
        FileHandle fh;

	};

#endif
