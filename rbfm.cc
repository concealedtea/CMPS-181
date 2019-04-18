#include <stdio.h>
#include <iostream>
#include <cstring>
#include <cmath>
#include "rbfm.h"

using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{
    PagedFileManager *pfm = PagedFileManager::instance();
    pfm->createFile(fileName);
    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    PagedFileManager *pfm = PagedFileManager::instance();
    pfm->destroyFile(fileName);
    return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, 
                                    FileHandle &fileHandle) 
{
    PagedFileManager *pfm = PagedFileManager::instance();
    pfm->openFile(fileName, fileHandle);
    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    PagedFileManager *pfm = PagedFileManager::instance();
    pfm->closeFile(fileHandle);
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, 
    const vector<Attribute> &recordDescriptor, 
        const void *data, RID &rid) 
{
   int nullBytes = ceil(recordDescriptor.size() / 8.0);
   int size = sizeof (uint16_t) + (recordDescriptor.size()) * sizeof(uint16_t)
       + nullBytes;
   int offset = nullBytes;

    for (unsigned i = 0; i <= (unsigned) recordDescriptor.size(); i++)
    {
        if (recordDescriptor[i].type == TypeInt)
        {
            int field = 0;
            memcpy(&field, ((char*) data + offset), INT_SIZE);
            size += INT_SIZE;
            offset += INT_SIZE;
        }

        if (recordDescriptor[i].type == TypeReal)
        {
            float field = 0;
            memcpy(&field, ((char*) data + offset), REAL_SIZE);
            size += REAL_SIZE;
            offset += REAL_SIZE;
        }

        if (recordDescriptor[i].type == TypeVarChar)
        {
            int varcharSize = 0;
            memcpy(&varcharSize, ((char*) data + offset), VARCHAR_SIZE);
            size += varcharSize;
            offset += varcharSize + VARCHAR_SIZE;
        }
    }

    void *page = malloc(PAGE_SIZE);
    bool oldPage = false;
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    unsigned pageId = 0;

    for (pageId = 0; pageId < numberOfPages; pageId++)
    {
        if (fileHandle.readPage(pageId, page))
            return -1;

        SlotDirectoryHeader slotHeader;
        memcpy(&slotHeader, page, sizeof(SlotDirectoryHeader));
        unsigned freeSpace = slotHeader.freeSpaceOffset - 
            slotHeader.recordEntriesNumber * 
            sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);

        if (freeSpace >= sizeof(SlotDirectoryRecordEntry) + size)
        {
            oldPage = true;
            break;
        }
    }

    if (!oldPage)
    {
        memset(page, 0, PAGE_SIZE);
        SlotDirectoryHeader slotHeader;
        slotHeader.freeSpaceOffset = PAGE_SIZE;
        slotHeader.recordEntriesNumber = 0;
        memcpy(page, &slotHeader, sizeof(SlotDirectoryHeader));
    }

    SlotDirectoryHeader slotHeader;
    memcpy(&slotHeader, page, sizeof(SlotDirectoryHeader));
    rid.pageNum = pageId;
    rid.slotNum = slotHeader.recordEntriesNumber;

    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = size;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - size;
    memcpy(((char*) page + sizeof(SlotDirectoryHeader) 
        + rid.slotNum * sizeof(SlotDirectoryRecordEntry)),
        &newRecordEntry,
        sizeof(SlotDirectoryRecordEntry));

    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    memcpy(page, &slotHeader, sizeof(SlotDirectoryHeader));

    char nullFlags[nullBytes];
    memset(nullFlags, 0, nullBytes);
    memcpy(nullFlags, (char*) data, nullBytes);

    char *record = (char*) page + newRecordEntry.offset;
    unsigned dataOffset = nullBytes;
    unsigned headerOffset = 0;

    uint16_t len = recordDescriptor.size();
    memcpy(record + headerOffset, &len, sizeof(len));
    headerOffset += sizeof(len);

    memcpy(record + headerOffset, nullFlags, nullBytes);
    headerOffset += nullBytes;

    uint16_t recordOffset = headerOffset + (recordDescriptor.size()) 
        * sizeof(uint16_t);

    unsigned recordIdx = 0;
    for (recordIdx = 0; recordIdx < recordDescriptor.size(); recordIdx++)
    {
        int32_t mask = 1 << (8 - 1 - recordIdx % 8);
        if (nullFlags[recordIdx / 8] & mask)
        {
            continue;
        }            
        char *dataStart = (char*) data + dataOffset;

        if (recordDescriptor[recordIdx].type == TypeInt)
        {
            memcpy(record + recordOffset, dataStart, INT_SIZE);
            recordOffset += INT_SIZE;
            dataOffset += INT_SIZE;
        }
        else if (recordDescriptor[recordIdx].type == TypeReal)
        {
            memcpy(record + recordOffset, dataStart, REAL_SIZE);
            recordOffset += REAL_SIZE;
            dataOffset += REAL_SIZE;
        }
        else if (recordDescriptor[recordIdx].type == TypeVarChar)
        {
            unsigned varcharSize;
            memcpy(&varcharSize, dataStart, VARCHAR_SIZE);
            memcpy(record + recordOffset, dataStart + VARCHAR_SIZE, varcharSize);
            recordOffset += varcharSize;
            dataOffset += VARCHAR_SIZE + varcharSize;
        }
        
        memcpy(record + headerOffset, &recordOffset, sizeof(uint16_t));
        headerOffset += sizeof(uint16_t);
    }

    if (oldPage)
    {
        if (fileHandle.writePage(pageId, page))
            return -1;
    }
    else
    {
        if (fileHandle.appendPage(page))
            return -1;
    }

    free(page);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, 
    const vector<Attribute> &recordDescriptor, 
    const RID &rid, void *data) 
{
    void *page = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, page))
        return -1;

    SlotDirectoryHeader slotHeader;
    memcpy(&slotHeader, page, sizeof(SlotDirectoryHeader));
    if (slotHeader.recordEntriesNumber < rid.slotNum)
        return -1;

    SlotDirectoryRecordEntry recordEntry;
    memcpy(&recordEntry,
        ((char*) page + sizeof(SlotDirectoryHeader) 
        + rid.slotNum * sizeof(SlotDirectoryRecordEntry)),
        sizeof(SlotDirectoryRecordEntry));

    char *record = (char*) page + recordEntry.offset;
    uint16_t len = 0;
    memcpy (&len, record, sizeof(uint16_t));
    int recordNullBytes = ceil(len / 8.0);

    int nullBytes = ceil(recordDescriptor.size() / 8.0);
    char nullFlags[nullBytes];
    memset(nullFlags, 0, nullBytes);

    memcpy(nullFlags, record + sizeof(uint16_t), recordNullBytes);
    memcpy(data, nullFlags, nullBytes);
    unsigned recordOffset = sizeof(uint16_t) + recordNullBytes 
         + len * sizeof(uint16_t);
    unsigned dataOffset = nullBytes;

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t mask = 1 << (8 - 1 - i % 8);
        if (nullFlags[i / 8] & mask)
        {
            continue;
        }

        uint16_t fieldEnd;
        memcpy(&fieldEnd, 
            record + sizeof(uint16_t) + recordNullBytes
            + i * sizeof(uint16_t), sizeof(uint16_t));

        uint32_t fieldSize = fieldEnd - recordOffset;

        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + dataOffset, &fieldSize, VARCHAR_SIZE);
            dataOffset += VARCHAR_SIZE;
        }

        memcpy((char*) data + dataOffset, record + recordOffset, fieldSize);
        recordOffset += fieldSize;
        dataOffset += fieldSize;
    }

    free(page);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, 
    const void *data) 
{
    uint16_t nullBytes = ceil(recordDescriptor.size() / 8.0);
    char nullFlags[nullBytes];
    memset(nullFlags, 0, nullBytes);
    memcpy(nullFlags, data, nullBytes);
    uint16_t offset = nullBytes;

    for (unsigned i = 0; i < recordDescriptor.size(); i++) 
    {        
        int32_t mask = 1 << (8 - 1 - i % 8);
        if (nullFlags[i / 8] & mask)
        {
            continue;
        }
        if (recordDescriptor[i].type == TypeInt) 
        {
            int value;
            memcpy(&value, ((char*) data + offset), INT_SIZE);
            cout << recordDescriptor[i].name << ": " << value << endl;
            offset += INT_SIZE; 
        } 
        else if (recordDescriptor[i].type == TypeReal) 
        {
            float value;
            memcpy(&value, ((char*) data + offset), REAL_SIZE);
            cout << recordDescriptor[i].name << ": " << value << endl;
            offset += REAL_SIZE; 
        }
        else if (recordDescriptor[i].type == TypeVarChar) 
        {
            int length;
            memcpy(&length, ((char*) data + offset), VARCHAR_SIZE);
            offset += VARCHAR_SIZE;

            char *value = (char*) malloc(length + 1);
            memcpy(value, ((char*) data + offset), length);
            value[length] = '\0';
            cout << recordDescriptor[i].name << ": " << value << endl;
            offset += length;
            free(value);
        }        
    }
    
    return 0;
}
