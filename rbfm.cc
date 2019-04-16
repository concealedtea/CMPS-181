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
    int offset = nullIndicatorSize;

    for (unsigned i = 0; i <= (unsigned) recordDescriptor.size(); i++)
    {
        if (recordDescriptor[i].type == TypeInt)
        {
            int field = 0;
            memcpy(&field, ((char*) data + offset), INT_SIZE);
            offset += INT_SIZE;
            cout << field << endl;
        }

        if (recordDescriptor[i].type == TypeReal)
        {
            float field = 0;
            memcpy(&field, ((char*) data + offset), REAL_SIZE);
            offset += REAL_SIZE;
            cout << field << endl;
        }

        if (recordDescriptor[i].type == TypeVarChar)
        {
            int varcharSize = 0;
            memcpy(&varcharSize, ((char*) data + offset), VARCHAR_SIZE);
            cout << "varcharSize is: " << varcharSize << endl;
            offset += VARCHAR_SIZE;
            
            char *data_string = (char*) malloc(varcharSize + 1);
            memcpy(data_string, ((char*) data + offset), varcharSize);
            offset += varcharSize;
            cout << data_string << endl;
            free(data_string);
        }
    }    
    return 0;
}
