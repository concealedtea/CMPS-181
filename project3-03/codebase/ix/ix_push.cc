
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

    if (PagedFileManager::instance()->openFile(fileName, handle.fh) != 0)
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
    //setNodeType(IX_TYPE_INTERNAL, pageData);
    char type = 1;
    memcpy(pageData, &type, sizeof(char));
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
    //setNodeType(IX_TYPE_LEAF, pageData);
    type = 0;
    memcpy(pageData, &type, sizeof(char));
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

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
    int32_t rootPage = 1;
    //getRootPageNum(ixfileHandle, rootPage);
    //cout << "!!! printBtree: rootPageNum: " 
    //    << rootPage << endl;

    cout << "{";
    printBtree_rec(ixfileHandle, "  ",rootPage, attribute);
    cout << endl << "}" << endl;
}

// Print comma from calling context.
void IndexManager::printBtree_rec(IXFileHandle &ixfileHandle, string prefix, const int32_t currPage, const Attribute &attr) const
{
    void *pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(currPage, pageData);

    char type; //= getNodetype(pageData);
    memcpy(&type, pageData, sizeof(char));
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
        printInternalSlot(attr, i, pageData);
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
            const unsigned offset = sizeof(char) + sizeof(InternalHeader);
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
    //const unsigned offset = sizeof(NodeType);
    LeafHeader header; //= getLeafHeader(pageData);
    memcpy(&header, (char*)pageData + sizeof(char), sizeof(LeafHeader));
    void *key = NULL;
    if (attr.type != TypeVarChar)
        key = malloc (INT_SIZE);
    //bool first = true;
    vector<RID> key_rids;

    cout << "\"keys\":[";

    for (int i = 0; i <= header.numberOfSlots; i++)
    {
        //const unsigned offset = sizeof(char) + sizeof(LeafHeader);
        unsigned slotOffset = sizeof(char) + sizeof(LeafHeader) + i * 12;
        //DataEntry entry; //= getDataEntry(i, pageData);
        //memcpy(&entry, (char*)pageData + sizeof(char) 
        //    + sizeof(LeafHeader) + i * 12, sizeof(DataEntry));
        //memcpy(&entry, (char*)pageData + slotOffset, 12);
        if (i == 0) //(first && i < header.numberOfSlots)
        {
            key_rids.clear();
            //first = false;
            if (attr.type == TypeInt)
                //memcpy(key, &(entry.integer), INT_SIZE);
                memcpy(key, (char*)pageData + slotOffset, INT_SIZE);
            else if (attr.type == TypeReal)
                //memcpy(key, &(entry.real), REAL_SIZE);
                memcpy(key, (char*)pageData + slotOffset, REAL_SIZE);
            else
            {
                // Deal with reading in varchar
                int32_t vOffset;
                memcpy(&vOffset, (char*)pageData + slotOffset, 4);

                int len;
                memcpy(&len, (char*)pageData + vOffset, 
                    VARCHAR_LENGTH_SIZE);

                free(key);
                key = malloc(len + VARCHAR_LENGTH_SIZE + 1);
                memcpy(key, &len, VARCHAR_LENGTH_SIZE);
                memcpy((char*)key + VARCHAR_LENGTH_SIZE, (char*)pageData 
                     + vOffset + VARCHAR_LENGTH_SIZE, len);
                memset((char*)key + VARCHAR_LENGTH_SIZE + len, 0, 1);
            }
        }

        RID rid;
        memcpy(&rid, (char*)pageData + slotOffset + 4, 8);
        if (i < header.numberOfSlots && compareLeafSlot(attr, key, pageData, i) == 0)
        {
            key_rids.push_back(rid);
        }
        else if (i != 0)
        {
            cout << "\"";
            if (attr.type == TypeInt)
            {
                cout << *(int*)key;
                //memcpy(key, &(entry.integer), INT_SIZE);
                memcpy(key, (char*)pageData + slotOffset, INT_SIZE);
            }
            else if (attr.type == TypeReal)
            {
                cout << *(float*)key;
                //memcpy(key, &(entry.real), REAL_SIZE);
                memcpy(key, (char*)pageData + slotOffset, REAL_SIZE);
            }
            else
            {
                cout << (char*)key + 4;

                int32_t vOffset;
                memcpy(&vOffset, (char*)pageData + slotOffset, 4);

                int len;
                memcpy(&len, (char*)pageData + vOffset, 
                    VARCHAR_LENGTH_SIZE);

                //int len;
                //memcpy(&len, (char*)pageData + entry.varcharOffset, 
                //    VARCHAR_LENGTH_SIZE);

                free(key);
                key = malloc(len + VARCHAR_LENGTH_SIZE + 1);
                memcpy(key, &len, VARCHAR_LENGTH_SIZE);
                memcpy((char*)key + VARCHAR_LENGTH_SIZE, (char*)pageData 
                    + vOffset + VARCHAR_LENGTH_SIZE, len);
                memset((char*)key + VARCHAR_LENGTH_SIZE + len, 0, 1);
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
            key_rids.push_back(rid);
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
    if (attr.type == TypeInt)
        cout << entry.integer;
    else if (attr.type == TypeReal)
        cout << entry.real;
    else
    {
        int32_t len;
        memcpy(&len, (char*)data + entry.varcharOffset, VARCHAR_LENGTH_SIZE);
        char varchar[len + 1];
        varchar[len] = '\0';
        memcpy(varchar, (char*)data + entry.varcharOffset + VARCHAR_LENGTH_SIZE, len);
        cout << varchar;
    }
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

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    ixReadPageCounter++;
    return fh.readPage(pageNum, data);
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


DataEntry IndexManager::getDataEntry(const int slotNum, const void *pageData) const
{
    DataEntry entry;
    memcpy(&entry, (char*)pageData + sizeof(char) + sizeof(LeafHeader) 
        + slotNum * sizeof(DataEntry), sizeof(DataEntry));
    //cout << "getDataEntry: size of DataEntry: " << sizeof(DataEntry) << endl;
    return entry;
}

int IndexManager::compareLeafSlot(const Attribute attr, const void *key, const void *pageData, const int slotNum) const
{
    DataEntry entry = getDataEntry(slotNum, pageData);
    if (attr.type == TypeInt)
    {
        int32_t int_key;
        memcpy(&int_key, key, INT_SIZE);
        return compare(int_key, entry.integer);
    }
    else if (attr.type == TypeReal)
    {
        float real_key;
        memcpy(&real_key, key, REAL_SIZE);
        return compare(real_key, entry.real);
    }
    else
    {
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
