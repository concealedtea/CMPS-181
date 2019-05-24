#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

# define IX_EOF (-1)

typedef struct LeafHeader
{
    uint32_t next;
    uint32_t prev;
    uint16_t numberOfSlots;
    uint16_t freeSpaceOffset;
} LeafHeader;

typedef struct NonLeafHeader
{
    uint16_t numberOfSlots;
    uint16_t freeSpaceOffset;
    uint32_t leftChildPage;
} NonLeafHeader;

typedef struct ChildEntry
{
    void *key = nullptr;
    uint32_t childPage = 0;
} ChildEntry;

class IX_ScanIterator;
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
    RC insertIntoNonLeaf(const Attribute attribute, ChildEntry entry, void *pageData);
    RC insertIntoLeaf(const Attribute attribute, const void *key, const RID &rid, void *pageData);

    RC splitLeaf(IXFileHandle &fileHandle, const Attribute &attribute, const void *key, const RID rid, const int32_t pageID, void *originalLeaf, ChildEntry &childEntry);
    RC splitNonLeaf(IXFileHandle &fileHandle, const Attribute &attribute, const int32_t pageID, void *original, ChildEntry &childEntry);

    void printBtree_rec(IXFileHandle &ixfileHandle, string prefix, const int32_t currPage, const Attribute &attr) const;
    void printNonLeafNode(IXFileHandle &, void *pageData, const Attribute &attr, string prefix) const;
    void printLeafNode(void *pageData, const Attribute &attr) const;

    LeafHeader getLeafHeader(const void *pageData) const;
    RC getRootPageNum(IXFileHandle &fileHandle, int32_t &result) const;
    RC find(IXFileHandle &handle, const Attribute attr, const void *key, int32_t &resultPageNum);
    RC treeSearch(IXFileHandle &handle, const Attribute attr, const void *key, const int32_t currPageNum, int32_t &resultPageNum);
    int32_t getNextChildPage(const Attribute attr, const void *key, void *pageData);
    int compareSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const;
    int compareLeafSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const;
    int compare(const int key, const int value) const;
    int compare(const float key, const float value) const;
    int compare(const char *key, const char *value) const;
    int getKeySize(const Attribute attr, const void *key, int size) const;
    int getFreeSpaceNonLeaf(void *pageData) const;
    int getFreeSpaceLeaf(void *pageData) const;
    RC deleteEntryFromLeaf(const Attribute attr, const void *key, const RID &rid, void *pageData);
    RC deleteEntryFromNonLeaf(const Attribute attr, const void *key, void *pageData);
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

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    unsigned getNumberOfPages();

    RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, const void *data);
    RC appendPage(const void *data);

    friend class IndexManager;
private:
    FileHandle fh;

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

    RC initialize(IXFileHandle &, Attribute, const void*, const void*, bool, bool);
};

#endif
