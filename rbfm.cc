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

RC RecordBasedFileManager::getNullIndicatorSize(int fieldCount)
{
	return ceil((double) fieldCount / CHAR_BIT);
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{
    PagedFileManager pfm;
    pfm.createFile(const string &fileName);
    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    PagedFileManager pfm;
    pfm.destroyFile(const string &fileName);
    return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, 
    FileHandle &fileHandle) 
{
    PagedFileManager pfm;
    pfm.openFile(const string &fileName, FileHandle &fileHandle);
    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    PagedFileManager pfm;
    pfm.closeFile(FileHandle &fileHandle);
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, 
    const vector<Attribute> &recordDescriptor, 
        const void *data, RID &rid) 
{
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, 
    const vector<Attribute> &recordDescriptor, 
    const RID &rid, void *data) 
{
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, 
    const void *data) 
{
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    for (unsigned i = 0; i <= (unsigned) recordDescriptor.size(); i++)
    {
        int offset = nullIndicatorSize;
        if (recordDescriptor[i].type == TypeInt)
        {
            int record = 0;
            offset += INT_SIZE;
            memcpy(&record, ((char*) data + offset), INT_SIZE);
            cout << record << endl;
        }

        if (recordDescriptor[i].type == TypeReal)
        {
            int record = 0;
            offset += REAL_SIZE;
            memcpy(&record, ((char*) data + offset), REAL_SIZE);
            cout << record << endl;
        }

        if (recordDescriptor[i].type == TypeVarChar)
        {
            int record = 0;
            offset += INT_SIZE;
            memcpy(&record, ((char*) data + offset), INT_SIZE);
            cout << record << endl;
        }
    }    
    return 0;
}