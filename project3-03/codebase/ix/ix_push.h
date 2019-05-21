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

typedef struct ChildEntry
{
    void *key;
    uint32_t childPage;
} ChildEntry;

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
    
        RC insert(const Attribute &attribute, const void *key, const RID &rid, IXFileHandle &fileHandle, int32_t pageID, ChildEntry &childEntry);
        RC insertIntoInternal(const Attribute attribute, ChildEntry entry, void *pageData);
        RC insertIntoLeaf(const Attribute attribute, const void *key, const RID &rid, void *pageData);
        RC splitLeaf(IXFileHandle &fileHandle, const Attribute &attribute, const void *key, const RID rid, const int32_t pageID, void *originalLeaf, ChildEntry &childEntry);
        RC splitInternal(IXFileHandle &fileHandle, const Attribute &attribute, const int32_t pageID, void *original, ChildEntry &childEntry);

        void printBtree_rec(IXFileHandle &ixfileHandle, string prefix, const int32_t currPage, const Attribute &attr) const;
        void printInternalNode(IXFileHandle &, void *pageData, const Attribute &attr, string prefix) const;
        void printInternalSlot(const Attribute &attr, const int32_t slotNum, const void *data) const;
        void printLeafNode(void *pageData, const Attribute &attr) const;

        void setInternalHeader(const InternalHeader header, void *pageData);
        InternalHeader getInternalHeader(const void *pageData) const;
        void setLeafHeader(const LeafHeader header, void *pageData);
        LeafHeader getLeafHeader(const void *pageData) const;
        void setIndexEntry(const IndexEntry entry, const int slotNum, void *pageData);
        IndexEntry getIndexEntry(const int slotNum, const void *pageData) const;
        void setDataEntry(const DataEntry entry, const int slotNum, void *pageData);
        DataEntry getDataEntry(const int slotNum, const void *pageData) const;
    
        int getKeyLengthInternal(const Attribute attr, const void *key) const;
        int getKeyLengthLeaf(const Attribute attr, const void *key) const;
        int getFreeSpaceInternal(void *pageData) const;
        int getFreeSpaceLeaf(void *pageData) const;
    
        RC find(IXFileHandle &handle, const Attribute attr, const void *key, int32_t &resultPageNum);
        RC treeSearch(IXFileHandle &handle, const Attribute attr, const void *key, const int32_t currPageNum, int32_t &resultPageNum);
        int32_t getNextChildPage(const Attribute attr, const void *key, void *pageData);
    
        RC deleteEntryFromLeaf(const Attribute attr, const void *key, const RID &rid, void *pageData);
        RC deleteEntryFromInternal(const Attribute attr, const void *key, void *pageData);

        int compareLeafSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const;
        // Returns -1, 0, or 1 if key is less than, equal to, or greater than value
        int compare(const int key, const int value) const;
        int compare(const float key, const float value) const;
        int compare(const char *key, const char *value) const;
};

class IX_ScanIterator {
public:
    
    // Constructor
    IX_ScanIterator();
    
    // Destructor
    ~IX_ScanIterator();
    
    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);
    
    // Terminate index scan
    RC close();
    
    friend class IndexManager;
private:
    IXFileHandle *fileHandle;
    Attribute attr;
    const void *lowKey;
    const void *highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;
    void *page;
    int slotNum;
    
    RC initialize(IXFileHandle &ixfileHandle, Attribute attribute, const void* lowKey, const void* highKey, bool lowKeyInclusive, bool highKeyInclusive);
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
