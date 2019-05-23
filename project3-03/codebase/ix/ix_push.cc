#include "ix.h"

#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"

#include <vector>
#include <string>
#include <cstring>
#include <iostream>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();

    if (pfm->createFile(fileName.c_str()) != 0)
    {
        return -1;
    }

    // Open the file we just created
    IXFileHandle handle;

    if (openFile(fileName, handle) != 0)
    {
        return -1;
    }

    void *pageData = malloc(PAGE_SIZE);
    memset(pageData, 0, PAGE_SIZE);

    // Initialize the first page with metadata. root page will be page 1
    //MetaHeader meta;
    //meta.rootPage = 1;
    uint32_t root = 1;
    //setMetaData(meta, pageData);
    memcpy(pageData, &root, sizeof(uint32_t));
    if (handle.appendPage(pageData) != 0)
    {
        closeFile(handle);
        free(pageData);
        return -1;
    }

    // Initialize root page as internal node with a single child
    setNodeType(IX_TYPE_INTERNAL, pageData);
    InternalHeader header;
    header.numberOfSlots = 0;
    header.freeSpaceOffset = PAGE_SIZE;
    header.leftChildPage = 2;
    setInternalHeader(header, pageData);
    if (handle.appendPage(pageData) != 0)
    {
        closeFile(handle);
        free(pageData);
        return -1;
    }

    // Set up child of root as empty leaf
    setNodeType(IX_TYPE_LEAF, pageData);
    LeafHeader leafHeader;
    leafHeader.next = 0;
    leafHeader.prev = 0;
    leafHeader.numberOfSlots = 0;
    leafHeader.freeSpaceOffset = PAGE_SIZE;
    setLeafHeader(leafHeader, pageData);
    if (handle.appendPage(pageData) != 0)
    {
        closeFile(handle);
        free(pageData);
        return -1;
    }

    closeFile(handle);
    free(pageData);
    return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    if (pfm->destroyFile(fileName) != 0)
    {
        return -1;
    }
    return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    if (pfm->openFile(fileName, ixfileHandle.fh) != 0)
    {
        return -1;
    }
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    if (pfm->closeFile(ixfileHandle.fh) != 0)
    {
        return -1;
    }
    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    ChildEntry childEntry; //= {.key = NULL, .childPage = 0};
    int32_t rootPage = 1;
    return insert(attribute, key, rid, ixfileHandle, rootPage, childEntry);
}

RC IndexManager::insert(const Attribute &attribute, const void *key, const RID &rid, IXFileHandle &fileHandle, int32_t pageID, ChildEntry &childEntry) {
    void *pageData = malloc(PAGE_SIZE);
    if(pageData == NULL)
        return -1;
    if (fileHandle.readPage(pageID, pageData)) {
        free(pageData);
        return -1;
    }

    char type; //= getNodetype(pageData);
    memcpy(&type, pageData, sizeof(char));

    if (type == 0) {
        // try insert
        RC rc = insertIntoLeaf(attribute, key, rid, pageData);
        if (rc == 0) { // successful insert
            // write changes
            if (fileHandle.writePage(pageID, pageData))
                return -1;
            
            // clear childEntry to defaults
            free (childEntry.key);
            childEntry.key = NULL;
            childEntry.childPage = 0;
            
            free(pageData);
            pageData = NULL;
            return 0;
        }
        else if (rc == -2) { // Need split leaf
            rc = splitLeaf(fileHandle, attribute, key, rid, pageID, pageData, childEntry);
            free(pageData);
            pageData = NULL;
            return rc;
        }
        else { // should never happen
            free(pageData);
            pageData = NULL;
            free(childEntry.key);
            childEntry.key = NULL;
            return -1;
        }
    }
    else { // type == 1
        int32_t childPage = getNextChildPage(attribute, key, pageData);
        free (pageData);
        if (childPage == 0)
            return -1;
        
        // do recursive insert
        RC rc = insert(attribute, key, rid, fileHandle, childPage, childEntry);
        if (rc)
            return rc;
        if(childEntry.key == NULL)
            return 0;
        // Need to do split
        pageData = malloc(PAGE_SIZE);
        if (fileHandle.readPage(pageID, pageData)) {
            free(pageData);
            return -1;
        }
        
        rc = insertIntoInternal(attribute, childEntry, pageData);
        if (rc == 0) {
            rc = fileHandle.writePage(pageID, pageData);
            
            // clear childEntry to defaults
            free (childEntry.key);
            childEntry.key = NULL;
            childEntry.childPage = 0;
            free(pageData);
            pageData = NULL;
            return rc;
        }
        else if (rc == -2) {
            rc = splitInternal(fileHandle, attribute, pageID, pageData, childEntry);
            free(pageData);
            pageData = NULL;
            return rc;
        }
        else { // should never happen
            free(pageData);
            free(childEntry.key);
            childEntry.key = NULL;
            return -1;
        }
    }
}

RC IndexManager::splitLeaf(IXFileHandle &fileHandle, const Attribute &attribute, const void *ins_key, const RID ins_rid, const int32_t pageID, void *originalLeaf, ChildEntry &childEntry) {
    LeafHeader originalHeader = getLeafHeader(originalLeaf);

    // create new leaf
    void *newLeaf = calloc(PAGE_SIZE, 1);
    //setNodeType(IX_TYPE_LEAF, newLeaf);
    char type = 0;
    memcpy(newLeaf, &type, sizeof(char));
    LeafHeader newHeader;
    newHeader.prev = pageID;
    newHeader.next = originalHeader.next;
    newHeader.numberOfSlots = 0;
    newHeader.freeSpaceOffset = PAGE_SIZE;
    setLeafHeader(newHeader, newLeaf);

    int32_t newPageNum = fileHandle.getNumberOfPages();
    originalHeader.next = newPageNum;
    setLeafHeader(originalHeader, originalLeaf);

    int size = 0;
    int i;
    int lastSize = 0;
    for (i = 0; i < originalHeader.numberOfSlots; i++) {
        void *key = NULL;

        if (attribute.type == TypeVarChar)
        {
            int32_t vOffset;
            memcpy(&vOffset, (char*)originalLeaf + sizeof(char) + sizeof(LeafHeader)
                   + i * 12, 4);
            key = (char*)originalLeaf + vOffset;
        }
        else if (attribute.type == TypeInt)
        {
            int temp;
            memcpy(&temp, (char*)originalLeaf + sizeof(char) + sizeof(LeafHeader)
                   + i * 12, 4);
            key = &temp;
        }
        else // attribute.type == TypeReal
        {
            float temp;
            memcpy(&temp, (char*)originalLeaf + sizeof(char) + sizeof(LeafHeader)
                   + i * 12, 4);
            key = &temp;
        }
        lastSize = getKeyLengthLeaf(attribute, key);
        size += lastSize;
        if (size >= PAGE_SIZE / 2) {
            if (i >= originalHeader.numberOfSlots - 1 || compareLeafSlot(attribute, key, originalLeaf, i+1) != 0)
                break;
        }
    }
    // i is middle key
    childEntry.key = malloc(lastSize);
    if (childEntry.key == NULL)
        return -1;
    childEntry.childPage = newPageNum;
    int keySize = attribute.type == TypeVarChar ? lastSize - 12 : INT_SIZE;
    if (attribute.type != TypeVarChar)
    {
        int temp;
        memcpy(&temp, (char*)originalLeaf + sizeof(char) + sizeof(LeafHeader)
               + i * 12, 4);
        memcpy(childEntry.key, &temp, keySize);
    }
    else
    {
        int vOffset;
        memcpy(&vOffset, (char*)originalLeaf + sizeof(char) + sizeof(LeafHeader)
               + i * 12, 4);
        memcpy(childEntry.key, (char*)originalLeaf + vOffset,
               keySize);
    }

    void *moving_key = malloc (attribute.length + 4);
    // Grab data entries after the middle entry; delete and shift the remaining
    for (int j = 1; j < originalHeader.numberOfSlots - i; j++) {
        RID moving_rid;
        memcpy(&moving_rid, (char*)originalLeaf + sizeof(char) + sizeof(LeafHeader) + 4
               + (i + 1) * 12, 8);
        if (attribute.type != TypeVarChar) {
            int temp;
            memcpy(&temp, (char*)originalLeaf + sizeof(char) + sizeof(LeafHeader)
                   + (i + 1) * 12, 4);
            memcpy(moving_key, &temp, INT_SIZE);
        }
        else {
            int vOffset;
            memcpy(&vOffset, (char*)originalLeaf + sizeof(char) + sizeof(LeafHeader)
                   + (i + 1) * 12, 4);
            int32_t len;
            memcpy(&len, (char*)originalLeaf + vOffset, VARCHAR_LENGTH_SIZE);
            memcpy(moving_key, &len, VARCHAR_LENGTH_SIZE);
            memcpy((char*)moving_key + VARCHAR_LENGTH_SIZE, (char*)originalLeaf
                   + vOffset + VARCHAR_LENGTH_SIZE, len);
        }
        // move to new leaf
        insertIntoLeaf(attribute, moving_key, moving_rid, newLeaf);
        deleteEntryFromLeaf(attribute, moving_key, moving_rid, originalLeaf);
    }
    free(moving_key);

    // Still need to: Write back both (append new leaf)
    // Add new record to correct page
    if (compareLeafSlot(attribute, ins_key, originalLeaf, i) <= 0) {
        if (insertIntoLeaf(attribute, ins_key, ins_rid, originalLeaf)) {
            free(newLeaf);
            return -1;
        }
    }
    else {
        if (insertIntoLeaf(attribute, ins_key, ins_rid, newLeaf)) {
            free(newLeaf);
            return -1;
        }
    }

    if(fileHandle.writePage(pageID, originalLeaf)) {
        free(newLeaf);
        return -1;
    }
    if(fileHandle.appendPage(newLeaf)) {
        free(newLeaf);
        return -1;
    }
    free(newLeaf);
    return 0;
}

RC IndexManager::insertIntoInternal(const Attribute attribute, ChildEntry entry, void *pageData) {
    InternalHeader header = getInternalHeader(pageData);
    int len = getKeyLengthInternal(attribute, entry.key);

    if (getFreeSpaceInternal(pageData) < len)
        return -2;

    int i;
    for (i = 0; i < header.numberOfSlots; i++) {
        if (compareSlot(attribute, entry.key, pageData, i) <= 0)
            break;
    }

    // i is slot number where new entry will go
    // i is slot number to move
    //int start_offset = getOffsetOfInternalSlot(i);
    //int end_offset = getOffsetOfInternalSlot(header.numberOfSlots);
    int start_offset = sizeof(char) + sizeof(InternalHeader) + i * sizeof(IndexEntry);
    int end_offset = sizeof(char) + sizeof(InternalHeader) + header.numberOfSlots * sizeof(IndexEntry);
    // shift all data entries starting at start_offset to the right to make room for a new dataEntry
    memmove((char*)pageData + start_offset + sizeof(IndexEntry), (char*)pageData + start_offset, end_offset - start_offset);

    IndexEntry newEntry;
    newEntry.childPage = entry.childPage;
    if (attribute.type == TypeVarChar) {
        int32_t len;
        memcpy(&len, entry.key, VARCHAR_LENGTH_SIZE);
        newEntry.varcharOffset = header.freeSpaceOffset - (len + VARCHAR_LENGTH_SIZE);
        memcpy((char*)pageData + newEntry.varcharOffset, entry.key, len + VARCHAR_LENGTH_SIZE);
        header.freeSpaceOffset = newEntry.varcharOffset;
    }
    
    else if (attribute.type == TypeInt) {
        memcpy(&newEntry.integer, entry.key, INT_SIZE);
    }
    else { // attribute.type == TypeReal
        
        memcpy(&newEntry.real, entry.key, REAL_SIZE);
    }
    header.numberOfSlots += 1;
    setInternalHeader(header, pageData);
    setIndexEntry(newEntry, i, pageData);
    return 0;
}

RC IndexManager::insertIntoLeaf(const Attribute attribute, const void *key, const RID &rid, void *pageData) {
    LeafHeader header = getLeafHeader(pageData);

    int32_t key_len = getKeyLengthLeaf(attribute, key);
    if (getFreeSpaceLeaf(pageData) < key_len)
        return -2;

    int i;
    for (i = 0; i < header.numberOfSlots; i++) {
        if (compareLeafSlot(attribute, key, pageData, i) < 0)
            break;
    }

    // i is slot number to move
    //int start_offset = getOffsetOfLeafSlot(i);
    //int end_offset = getOffsetOfLeafSlot(header.numberOfSlots);
    int start_offset = sizeof(char) + sizeof(LeafHeader) + i * 12;
    int end_offset = sizeof(char) + sizeof(LeafHeader) + header.numberOfSlots * 12;
        // shift all data entries starting at start_offset to the right to make room for a new dataEntry
        memmove((char*)pageData + start_offset + 12, (char*)pageData + start_offset, end_offset - start_offset);

    unsigned slotOffset = sizeof(char) + sizeof(LeafHeader) + i * 12;
    if (attribute.type == TypeVarChar) {
        int32_t len;
        memcpy(&len, key, VARCHAR_LENGTH_SIZE);
        int32_t varcharOffset = header.freeSpaceOffset - (len + VARCHAR_LENGTH_SIZE);
        memcpy((char*)pageData + slotOffset, &varcharOffset, 4);
        memcpy((char*)pageData + varcharOffset, key, len + VARCHAR_LENGTH_SIZE);
        header.freeSpaceOffset = varcharOffset;
    }
    else {
        memcpy((char*) pageData + slotOffset, key, 4);
    }
    header.numberOfSlots += 1;
    setLeafHeader(header, pageData);
    memcpy((char*) pageData + slotOffset + 4, &rid, 12);
    return 0;
}

RC IndexManager::splitInternal(IXFileHandle &fileHandle, const Attribute &attribute, const int32_t pageID, void *original, ChildEntry &childEntry) {
    InternalHeader originalHeader = getInternalHeader(original);

    int32_t newPageNum = fileHandle.getNumberOfPages();

    int size = 0;
    int i;
    int lastSize = 0;
    for (i = 0; i < originalHeader.numberOfSlots; i++) {
        IndexEntry entry = getIndexEntry(i, original);
        void *key;

        if (attribute.type == TypeVarChar)
            key = (char*)original + entry.varcharOffset;
        else if (attribute.type == TypeInt)
            key = &(entry.integer);
        else // attribute.type == TypeReal
            key = &(entry.real);

        lastSize = getKeyLengthInternal(attribute, key);
        size += lastSize;
        if (size >= PAGE_SIZE / 2) {
            break;
        }
    }
    // i is now middle key
    IndexEntry middleEntry = getIndexEntry(i, original);

    // Create new leaf to hold overflow
    void *newIntern = calloc(PAGE_SIZE, 1);
    //setNodeType(IX_TYPE_INTERNAL, newIntern);
    char type = 1;
    memcpy(newIntern, &type, sizeof(char));
    InternalHeader newHeader;
    newHeader.numberOfSlots = 0;
    newHeader.freeSpaceOffset = PAGE_SIZE;
    newHeader.leftChildPage = middleEntry.childPage;
    setInternalHeader(newHeader, newIntern);

    // get size of middle key, keep middle key for later
    int keySize = attribute.type == TypeVarChar ? lastSize - sizeof(IndexEntry) : INT_SIZE;
    void *middleKey = malloc(keySize);
    if (attribute.type != TypeVarChar)
        memcpy(middleKey, &(middleEntry.integer), INT_SIZE);
    else
        memcpy(middleKey, (char*)original + middleEntry.varcharOffset, keySize);

    void *moving_key = malloc (attribute.length + 4);
    // Grab data entries after the middle entry; delete and shift the remaining
    for (int j = 1; j < originalHeader.numberOfSlots - i; j++) {
        IndexEntry entry = getIndexEntry(i + 1, original);
        int32_t moving_pagenum = entry.childPage;
        if (attribute.type != TypeVarChar) {
            memcpy(moving_key, &(entry.integer), INT_SIZE);
        }
        else {
            int32_t len;
            memcpy(&len, (char*)original + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
            memcpy(moving_key, &len, VARCHAR_LENGTH_SIZE);
            memcpy((char*)moving_key + VARCHAR_LENGTH_SIZE,(char*)original + entry.varcharOffset + VARCHAR_LENGTH_SIZE, len);
        }
        ChildEntry tmp;
        tmp.key = moving_key;
        tmp.childPage = moving_pagenum;
        insertIntoInternal(attribute, tmp, newIntern);
        deleteEntryFromInternal(attribute, moving_key, original);
    }
    free(moving_key);
    // delete middle entry
    deleteEntryFromInternal(attribute, middleKey, original);

    // if (the new key is less than the middle key) put it in original node,
    // else put it in new node
    if (compareSlot(attribute, childEntry.key, original, i) < 0) {
        if (insertIntoInternal(attribute, childEntry, original)) {
            free(newIntern);
            return -1;
        }
    }
    else {
        if (insertIntoInternal(attribute, childEntry, newIntern)) {
            free(newIntern);
            return -1;
        }
    }

    if(fileHandle.writePage(pageID, original)) {
        free(newIntern);
        return -1;
    }
    if(fileHandle.appendPage(newIntern)) {
        free(newIntern);
        return -1;
    }
    free(newIntern);

    // take the key of the middle entry and propogate up
    free(childEntry.key);
    childEntry.key = middleKey;
    childEntry.childPage = newPageNum;

    // check if we're root
    int32_t rootPage = 1;
    if (pageID == rootPage) {
        // create new page and set headers
        void *newRoot = calloc(PAGE_SIZE, 1);

        //setNodeType(IX_TYPE_INTERNAL, newRoot);
        char type = 1;
        memcpy(newRoot, &type, sizeof(char));
        InternalHeader rootHeader;
        rootHeader.numberOfSlots = 0;
        rootHeader.freeSpaceOffset = PAGE_SIZE;
        // left most will be the smaller of these two pages
        rootHeader.leftChildPage = pageID;
        setInternalHeader(rootHeader, newRoot);
        // insert larger of these two pages after
        insertIntoInternal(attribute, childEntry, newRoot);

        // update metadata page
        if(fileHandle.appendPage(newRoot))
            return -1;
        uint32_t root = 1;
        memcpy(newRoot, &root, sizeof(uint32_t));
        if(fileHandle.writePage(0, newRoot))
            return -1;
        // Free memory
        free(newRoot);
        free(childEntry.key);
        childEntry.key = NULL;
    }

    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    int32_t leafPage;
    RC rc = find(ixfileHandle, attribute, key, leafPage);
    if (rc)
        return rc;
    // leafPage is the page number of the leaf where this entry should be
    // read in page
    void *pageData = malloc(PAGE_SIZE);
    if (ixfileHandle.readPage(leafPage, pageData)) {
        free(pageData);
        return -1;
    }

    // delete from pageData
    rc = deleteEntryFromLeaf(attribute, key, rid, pageData);
    if (rc) {
        free(pageData);
        return rc;
    }

    rc = ixfileHandle.writePage(leafPage, pageData);
    free(pageData);
    return rc;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
                      const Attribute &attribute,
                      const void      *lowKey,
                      const void      *highKey,
                      bool            lowKeyInclusive,
                      bool            highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    return ix_ScanIterator.initialize(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
    int32_t rootPage = 1;
    //getRootPageNum(ixfileHandle, rootPage);
    //cout << "!!! printBtree: rootPageNum: " 
    //    << rootPage << endl;

    cout << "{";
    printBtree_rec(ixfileHandle, "  ", rootPage, attribute);
    cout << endl << "}" << endl;          
}

// Print comma from calling context.
void IndexManager::printBtree_rec(IXFileHandle &ixfileHandle, string prefix, const int32_t currPage, const Attribute &attr) const
{
    void *pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(currPage, pageData);

    char type; //= getNodetype(pageData);
    memcpy(&type, pageData, sizeof(char));
    cout << "\nprintBtree_rec: currPage: " << currPage 
        << " type: " << (int) type << endl;
    if (type == 0)
    {
        printLeafNode(pageData, attr);
    }
    else
    {
        printInternalNode(ixfileHandle, pageData, attr, prefix);
    }
    free(pageData);
}

void IndexManager::printInternalNode(IXFileHandle &ixfileHandle, void *pageData,
    const Attribute &attr, string prefix) const
{
    const unsigned offset = sizeof(char);
    InternalHeader header;
    memcpy(&header, (char*)pageData + offset, sizeof(InternalHeader));

    cout << "\n" << prefix << "\"keys\":[";
    for (int i = 0; i < header.numberOfSlots; i++)
    {
        if (i != 0)
            cout << ",";
        //printInternalSlot(attr, i, pageData);
        IndexEntry entry;
        memcpy(&entry, (char*)pageData + sizeof(char) + sizeof(InternalHeader) 
               + i * sizeof(IndexEntry), sizeof(IndexEntry));
        if (attr.type == TypeVarChar) {
            int32_t len;
            memcpy(&len, (char*)pageData + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
            char varchar[len + 1];
            varchar[len] = '\0';
            memcpy(varchar, (char*)pageData + entry.varcharOffset
                   + VARCHAR_LENGTH_SIZE, len);
            cout << varchar;
        }
        else if (attr.type == TypeInt) {
            cout << entry.integer;
        }
        else { // attr.type == TypeReal
            cout << entry.real;
        }
    }

    cout << "],\n" << prefix << "\"children\":[\n" << prefix;
    for (int i = 0; i <= header.numberOfSlots; i++)
    {
        if (i == 0)
        {
            cout << "{";
            printBtree_rec(ixfileHandle, prefix + "  ", header.leftChildPage, attr);
            cout << "}";
        }
        else
        {
            cout << ",\n" << prefix;
            const unsigned offset = sizeof(NodeType) + sizeof(InternalHeader);
            unsigned slotOffset = offset + (i - 1) * sizeof(IndexEntry);
            IndexEntry entry; //= getIndexEntry(i - 1, pageData);
            memcpy(&entry, (char*)pageData + slotOffset, sizeof(IndexEntry));

            cout << "{";
            printBtree_rec(ixfileHandle, prefix + "  ", entry.childPage, attr);
            cout << "}";
        }
    }
    cout << "\n" << prefix << "]";
}

void IndexManager::printLeafNode(void *pageData, const Attribute &attr) const
{
    //cout << "!!got in printLLeafNode!!" << endl;
    //const unsigned offset = sizeof(NodeType);
    LeafHeader header; //= getLeafHeader(pageData);
    memcpy(&header, (char*)pageData + sizeof(char), sizeof(LeafHeader));
    cout << "printLeafNode: LeafHeader: " << "numberOfSlots: " 
         << header.numberOfSlots << endl;
    void *key = NULL;
    if (attr.type != TypeVarChar)
        key = malloc (INT_SIZE);
    //bool first = true;
    vector<RID> key_rids;

    cout << "\"keys\":[" << endl;

    for (int i = 0; i < header.numberOfSlots; i++)
    {
        //const unsigned offset = sizeof(char) + sizeof(LeafHeader);
        unsigned slotOffset = sizeof(char) + sizeof(LeafHeader) + i * 12;

        {
            cout << "\"";
            if (attr.type == TypeVarChar) {
                //cout << (char*)key;
                //cout << "\n!!printLeafNode: key + 4: "
                //    << (char*) key + 4 << "!!!" << endl;
                
                int32_t vOffset;
                memcpy(&vOffset, (char*)pageData + slotOffset, 4);
                
                int len;
                memcpy(&len, (char*)pageData + vOffset, VARCHAR_LENGTH_SIZE);
                
                char name[len + 1];
                name[len] =  '\0';
                memcpy(name, (char*)pageData + vOffset + VARCHAR_LENGTH_SIZE, len);
                cout << name;
            }
            else if (attr.type == TypeInt) {
                memcpy(key, (char*)pageData + slotOffset, INT_SIZE);
                cout << *(int*)key;
            }
            else { // attr.type == TypeReal
                memcpy(key, (char*)pageData + slotOffset, REAL_SIZE);
                cout << *(float*)key;
            }

            RID rid;
            memcpy(&rid, (char*)pageData + slotOffset + 4, 8);

            {
                //cout << "\nprintLeafNode: RID is: pageNum: " << rid.pageNum 
                //     << " slotNum: " << rid.slotNum << endl;
                key_rids.push_back(rid);
            }
            
            cout << ":[";
            for (unsigned j = 0; j < key_rids.size(); j++)
            {
                if (j != 0)
                {
                    cout << ",";
                }
                cout << "(" << key_rids[j].pageNum << "," << key_rids[j].slotNum << ")";
            }
            cout << "]\"";
            key_rids.clear();
            //key_rids.push_back(rid);
        }
    }
    cout << "]}";
    free (key);
}

void IndexManager::printInternalSlot(const Attribute &attr, const int32_t slotNum, const void *data) const
{
    //IndexEntry entry = getIndexEntry(slotNum, data);
    //const unsigned offset = sizeof(char) + sizeof(InternalHeader);
    //unsigned slotOffset = sizeof(char) + sizeof(InternalHeader) 
    //     + slotNum * sizeof(IndexEntry);
    IndexEntry entry;
    memcpy(&entry, (char*)data + sizeof(char) + sizeof(InternalHeader) 
         + slotNum * sizeof(IndexEntry), sizeof(IndexEntry));
    if (attr.type == TypeVarChar) {
        int32_t len;
        memcpy(&len, (char*)data + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
        char varchar[len + 1];
        varchar[len] = '\0';
        memcpy(varchar, (char*)data + entry.varcharOffset + VARCHAR_LENGTH_SIZE, len);
        cout << varchar;
    }
    else if (attr.type == TypeInt) {
        cout << entry.integer;
    }
    else { // attr.type == TypeReal
        cout << entry.real;
    }
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::initialize(IXFileHandle &fh, Attribute attribute, const void *low, const void *high, bool lowInc, bool highInc) {
    // store parameters
    attr = attribute;
    fileHandle = &fh;
    lowKey = low;
    highKey = high;
    lowKeyInclusive = lowInc;
    highKeyInclusive = highInc;

    page = malloc(PAGE_SIZE);
    if (page == NULL)
        return -1;
    slotNum = 0;

    // find first page
    IndexManager *im = IndexManager::instance();
    int32_t startPageNum;
    RC rc = im->find(*fileHandle, attr, lowKey, startPageNum);
    if (rc) {
        free(page);
        return rc;
    }
    rc = fileHandle->readPage(startPageNum, page);
    if (rc) {
        free(page);
        return rc;
    }

    // find first entry
    LeafHeader header = im->getLeafHeader(page);
    int i = 0;
    for (i = 0; i < header.numberOfSlots; i++) {
        int cmp = (low == NULL ? -1 : im->compareLeafSlot(attr, lowKey, page, i));
        if (cmp < 0)
            break;
        if (cmp == 0 && lowKeyInclusive)
            break;
        if (cmp > 0)
            continue;
    }
    slotNum = i;
    return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    IndexManager *im = IndexManager::instance();
    LeafHeader header = im->getLeafHeader(page);
    // Chain to next leaf page
    if (slotNum >= header.numberOfSlots)
    {
        // no next page, EOF
        if (header.next == 0)
            return IX_EOF;
        slotNum = 0;
        fileHandle->readPage(header.next, page);
        return getNextEntry(rid, key);
    }
    // highkey null, always go to next entry; else check if key < highkey
    int cmp = highKey == NULL ? 1 : im->compareLeafSlot(attr, highKey, page, slotNum);
    if (cmp == 0 && !highKeyInclusive)
        return -1;
    if (cmp < 0)
        return -1;

    // get the data entry & its rid
    RID tRid;
    memcpy(&tRid, (char*)page + sizeof(char) + sizeof(LeafHeader) + 4 
           + slotNum * 12, 8);
    rid.pageNum = tRid.pageNum;
    rid.slotNum = tRid.slotNum;
    // get its key
    if (attr.type == TypeVarChar) {
        int32_t vOffset;
        memcpy(&vOffset, (char*)page + sizeof(char) + sizeof(LeafHeader)
               + slotNum * 12, 4);
        int len;
        memcpy(&len, (char*)page + vOffset, VARCHAR_LENGTH_SIZE);
        memcpy(key, &len, VARCHAR_LENGTH_SIZE);
        memcpy((char*)key + VARCHAR_LENGTH_SIZE, (char*)page + vOffset + VARCHAR_LENGTH_SIZE, len);
    }
    else {
        memcpy(key, (char*)page + sizeof(char) + sizeof(LeafHeader)
               + slotNum * 12, 4);
    }
    // increment slotNum for next getNextEntry
    slotNum++;
    return 0;
}

RC IX_ScanIterator::close()
{
    free(page);
    return SUCCESS;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    ixReadPageCounter++;
    return fh.readPage(pageNum, data);
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    ixWritePageCounter++;
    return fh.writePage(pageNum, data);
}

RC IXFileHandle::appendPage(const void *data)
{
    ixAppendPageCounter++;
    return fh.appendPage(data);
}

unsigned IXFileHandle::getNumberOfPages()
{
    return fh.getNumberOfPages();
}

// Private helpers -----------------------

void IndexManager::setMetaData(const MetaHeader header, void *pageData)
{
    memcpy(pageData, &header, sizeof(MetaHeader));
}

MetaHeader IndexManager::getMetaData(const void *pageData) const
{
    MetaHeader header;
    memcpy(&header, pageData, sizeof(MetaHeader));
    return header;
}

void IndexManager::setNodeType(const NodeType type, void *pageData)
{
    memcpy(pageData, &type, sizeof(NodeType));
}

NodeType IndexManager::getNodetype(const void *pageData) const
{
    NodeType result;
    memcpy(&result, pageData, sizeof(NodeType));
    return result;
}

void IndexManager::setInternalHeader(const InternalHeader header, void *pageData)
{
    memcpy((char*)pageData + sizeof(NodeType), &header, sizeof(InternalHeader));
}

InternalHeader IndexManager::getInternalHeader(const void *pageData) const
{
    InternalHeader header;
    memcpy(&header, (char*)pageData + sizeof(NodeType), sizeof(InternalHeader));
    return header;
}

void IndexManager::setLeafHeader(const LeafHeader header, void *pageData)
{
    memcpy((char*)pageData + sizeof(NodeType), &header, sizeof(LeafHeader));
}

LeafHeader IndexManager::getLeafHeader(const void *pageData) const
{
    LeafHeader header;
    memcpy(&header, (char*)pageData + sizeof(NodeType), sizeof(LeafHeader));
    return header;
}

void IndexManager::setIndexEntry(const IndexEntry entry, const int slotNum, 
    void *pageData)
{
    memcpy((char*) pageData + sizeof(NodeType) + sizeof(InternalHeader) + slotNum * sizeof(IndexEntry), &entry, sizeof(IndexEntry));
}

IndexEntry IndexManager::getIndexEntry(const int slotNum, const void *pageData) const
{
    IndexEntry entry;
    memcpy(&entry, (char*)pageData + sizeof(NodeType) + sizeof(InternalHeader) + slotNum * sizeof(IndexEntry), sizeof(IndexEntry));
    return entry;
}

RC IndexManager::getRootPageNum(IXFileHandle &fileHandle, int32_t &result) const
{
    void *metaPage = malloc(PAGE_SIZE);
    if (metaPage == NULL)
        return -1;
    RC rc = fileHandle.readPage(0, metaPage);
    if (rc)
    {
        free(metaPage);
        return -1;
    }

    MetaHeader header = getMetaData(metaPage);
    free(metaPage);
    result = header.rootPage;
    //cout << "getRootPageNum: result: " << result << endl;
    return SUCCESS;
}

RC IndexManager::find(IXFileHandle &handle, const Attribute attr, const void *key, int32_t &resultPageNum)
{
    int32_t rootPageNum;
    RC rc = getRootPageNum(handle, rootPageNum);
    if (rc)
        return rc;
    return treeSearch(handle, attr, key, rootPageNum, resultPageNum);
}

RC IndexManager::treeSearch(IXFileHandle &handle, const Attribute attr, const void *key, const int32_t currPageNum, int32_t &resultPageNum)
{
    void *pageData = malloc(PAGE_SIZE);

    if (handle.readPage(currPageNum, pageData))
    {
        free (pageData);
        return -1;
    }

    // Found our leaf!
    if (getNodetype(pageData) == 0)
    {
        resultPageNum = currPageNum;
        free(pageData);
        return 0;
    }

    int32_t nextChildPage = getNextChildPage(attr, key, pageData);

    free(pageData);
    return treeSearch(handle, attr, key, nextChildPage, resultPageNum);
}

int32_t IndexManager::getNextChildPage(const Attribute attr, const void *key, void *pageData)
{
    InternalHeader header = getInternalHeader(pageData);
    int32_t result = header.leftChildPage;
    if (key == NULL)
        return result;
    
    int i = 0;
    for (i = 0; i < header.numberOfSlots; i++)
    {
        // if key < slot key, then go down previous entry's child
        if (compareSlot(attr, key, pageData, i) <= 0)
            break;
    }
    if (i != 0)
    {
        IndexEntry entry = getIndexEntry(i - 1, pageData);
        result = entry.childPage;
    }
    return result;
}

int IndexManager::compareSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const
{
    IndexEntry entry = getIndexEntry(slotNum, pageData);
    if (attr.type == TypeVarChar) {
        int32_t key_size;
        memcpy(&key_size, key, VARCHAR_LENGTH_SIZE);
        char key_text[key_size + 1];
        key_text[key_size] = '\0';
        memcpy(key_text, (char*) key + VARCHAR_LENGTH_SIZE, key_size);
        
        int32_t value_offset = entry.varcharOffset;
        int32_t value_size;
        memcpy(&value_size, (char*)pageData + value_offset, VARCHAR_LENGTH_SIZE);
        char value_text[value_size + 1];
        value_text[value_size] = '\0';
        memcpy(value_text, (char*)pageData + value_offset + VARCHAR_LENGTH_SIZE, value_size);
        
        return compare(key_text, value_text);
    }
    else if (attr.type == TypeInt) {
        int32_t int_key;
        memcpy(&int_key, key, INT_SIZE);
        return compare(int_key, entry.integer);
    }
    else { // attr.type == TypeReal
        float real_key;
        memcpy(&real_key, key, REAL_SIZE);
        return compare(real_key, entry.real);
    }
    return 0;
}

int IndexManager::compareLeafSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const
{
    if (attr.type == TypeVarChar) {
        int32_t key_size;
        memcpy(&key_size, key, VARCHAR_LENGTH_SIZE);
        char key_text[key_size + 1];
        key_text[key_size] = '\0';
        memcpy(key_text, (char*) key + VARCHAR_LENGTH_SIZE, key_size);
        
        int vOffset;
        memcpy(&vOffset, (char*)pageData + sizeof(char) + sizeof(LeafHeader)
               + slotNum * 12, 4);
        int32_t value_offset = vOffset;
        int32_t value_size;
        memcpy(&value_size, (char*)pageData + value_offset, VARCHAR_LENGTH_SIZE);
        char value_text[value_size + 1];
        value_text[value_size] = '\0';
        memcpy(value_text, (char*)pageData + value_offset + VARCHAR_LENGTH_SIZE, value_size);
        
        return compare(key_text, value_text);
    }
    else if (attr.type == TypeInt) {
        int temp;
        memcpy(&temp, (char*)pageData + sizeof(char) + sizeof(LeafHeader)
               + slotNum * 12, 4);
        int32_t int_key;
        memcpy(&int_key, key, INT_SIZE);
        return compare(int_key, temp);
    }
    else { // attr.type == TypeReal
        float temp;
        memcpy(&temp, (char*)pageData + sizeof(char) + sizeof(LeafHeader)
               + slotNum * 12, 4);
        float real_key;
        memcpy(&real_key, key, REAL_SIZE);
        return compare(real_key, temp);
    }
    return 0; // suppress warnings
}

int IndexManager::compare(const int key, const int value) const
{
    if (key == value)
        return 0;
    else if (key > value)
        return 1;
    return -1;
}

int IndexManager::compare(const float key, const float value) const
{
    if (key == value)
        return 0;
    else if (key > value)
        return 1;
    return -1;
}

int IndexManager::compare(const char *key, const char *value) const
{
    return strcmp(key, value);
}

// Get size needed to insert key into page
int IndexManager::getKeyLengthInternal(const Attribute attr, const void *key) const
{
    int size = sizeof(IndexEntry);
    if (attr.type == TypeVarChar)
    {
        int32_t key_len;
        memcpy(&key_len, key, VARCHAR_LENGTH_SIZE);
        size += VARCHAR_LENGTH_SIZE;
        size += key_len;
    }
    return size;
}

int IndexManager::getKeyLengthLeaf(const Attribute attr, const void *key) const
{
    int size = 12;
    if (attr.type == TypeVarChar)
    {
        int32_t key_len;
        memcpy(&key_len, key, VARCHAR_LENGTH_SIZE);
        size += VARCHAR_LENGTH_SIZE;
        size += key_len;
    }
    return size;
}

int IndexManager::getFreeSpaceInternal(void *pageData) const
{
    InternalHeader header = getInternalHeader(pageData);
    return header.freeSpaceOffset - (sizeof(NodeType) 
        + sizeof(InternalHeader) + header.numberOfSlots * sizeof(IndexEntry));
}

int IndexManager::getFreeSpaceLeaf(void *pageData) const
{
    LeafHeader header = getLeafHeader(pageData);
    return header.freeSpaceOffset - (sizeof(NodeType) + sizeof(LeafHeader) 
        + header.numberOfSlots * 12);
}

RC IndexManager::deleteEntryFromLeaf(const Attribute attr, const void *key, const RID &rid, void *pageData) {
    LeafHeader header = getLeafHeader(pageData);

    int i;
    for (i = 0; i < header.numberOfSlots; i++) {
        // find slot with key and rid equal to given key and rid
        if(compareLeafSlot(attr, key, pageData, i) == 0) {
            RID temp;
            memcpy(&temp, (char*)pageData + sizeof(char) + sizeof(LeafHeader) + 4 
                   + i * 12, 8);
            if (temp.pageNum == rid.pageNum && temp.slotNum == rid.slotNum) {
                break;
            }
        }
    }
    // failed to find, error
    if (i == header.numberOfSlots) {
        return -1;
    }

    // get position where deleted entry starts & get position where entries end
    // move entries left, overwriting the deleted entry
    //unsigned slotStartOffset = getOffsetOfLeafSlot(i);
    //unsigned slotEndOffset = getOffsetOfLeafSlot(header.numberOfSlots);
    unsigned slotStartOffset = sizeof(char) + sizeof(LeafHeader) + i * 12;
    unsigned slotEndOffset = sizeof(char) + sizeof(LeafHeader) + header.numberOfSlots * 12;
    memmove((char*)pageData + slotStartOffset, (char*)pageData + slotStartOffset + 12, slotEndOffset - slotStartOffset - 12);

    header.numberOfSlots -= 1;

    // if index of varchars, need shift varchars also
    if (attr.type == TypeVarChar) {
        int32_t varcharOffset;
        memcpy(&varcharOffset, (char*)pageData + sizeof(char) + sizeof(LeafHeader) 
               + i * 12, 4);
        int32_t varchar_len;
        memcpy(&varchar_len, (char*)pageData + varcharOffset, VARCHAR_LENGTH_SIZE);
        int32_t entryLen = varchar_len + VARCHAR_LENGTH_SIZE;

        // take everything from the start of the free space to the start of the varchar being deleted, and move it over the deleted varchar
        memmove((char*)pageData + header.freeSpaceOffset + entryLen, (char*)pageData + header.freeSpaceOffset, varcharOffset - header.freeSpaceOffset);
        header.freeSpaceOffset += entryLen;
        // Update all of the slots that are moved over
        for (i = 0; i < header.numberOfSlots; i++) {
            int32_t vOffset;
            RID tRid;
            memcpy(&vOffset, (char*)pageData + sizeof(char) + sizeof(LeafHeader) 
                   + i * 12, 4);
            memcpy(&tRid, (char*)pageData + sizeof(char) + sizeof(LeafHeader) + 4 
                   + i * 12, 8);
            if (vOffset < varcharOffset)
                vOffset += entryLen;
            memcpy((char*) pageData + sizeof(char) + sizeof(LeafHeader) 
                   + i * 12, &vOffset, 4);
            memcpy((char*) pageData + sizeof(char) + sizeof(LeafHeader) + 4
                   + i * 12, &tRid, 8);
        }
    }
    setLeafHeader(header, pageData);
    return 0;
}

RC IndexManager::deleteEntryFromInternal(const Attribute attr, const void *key, void *pageData) {
    InternalHeader header = getInternalHeader(pageData);

    int i;
    for (i = 0; i < header.numberOfSlots; i++) {
        // scan through until find matching key
        if(compareSlot(attr, key, pageData, i) == 0)
        {
            break;
        }
    }
    if (i == header.numberOfSlots) { // no match, error
        return -1;
    }

    IndexEntry entry = getIndexEntry(i, pageData);

    // get positions where the deleted entry starts and ends
    //unsigned slotStartOffset = getOffsetOfInternalSlot(i);
    //unsigned slotEndOffset = getOffsetOfInternalSlot(header.numberOfSlots);
    unsigned slotStartOffset = sizeof(char) + sizeof(InternalHeader) + i * sizeof(IndexEntry);
    unsigned slotEndOffset = sizeof(char) + sizeof(InternalHeader) + header.numberOfSlots * sizeof(IndexEntry);

    // move entries over, overwriting the deleted slot
    memmove((char*)pageData + slotStartOffset, (char*)pageData + slotStartOffset + sizeof(IndexEntry), slotEndOffset - slotStartOffset - sizeof(IndexEntry));
    // update numberOfSlots
    header.numberOfSlots -= 1;

    // if index of varchars, need shift varchars also
    if (attr.type == TypeVarChar) {
        int32_t varcharOffset = entry.varcharOffset;
        int32_t varchar_len;
        memcpy(&varchar_len, (char*)pageData + varcharOffset, VARCHAR_LENGTH_SIZE);
        int32_t entryLen = varchar_len + VARCHAR_LENGTH_SIZE;

        // take everything from start of free space until the starting offset of the varchar, and move it over where the varchar once was
        memmove((char*)pageData + header.freeSpaceOffset + entryLen, (char*)pageData + header.freeSpaceOffset, varcharOffset - header.freeSpaceOffset);
        header.freeSpaceOffset += entryLen;
        // Update all of the slots that are moved over
        for (i = 0; i < header.numberOfSlots; i++)
        {
            entry = getIndexEntry(i, pageData);
            if (entry.varcharOffset < varcharOffset)
                entry.varcharOffset += entryLen;
            setIndexEntry(entry, i, pageData);
        }
    }
    setInternalHeader(header, pageData);
    return 0;
}
