#include "pfm.h"
#include <stdio.h>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{
}

int PagedFileManager::fileExists(const char *fname)
{
    FILE *file;
    if ((file = fopen(fname, "rb")))
    {
        fclose(file);
        return 1;
    }
    return 0;
}

RC PagedFileManager::createFile(const string &fileName)
{
    if (fileExists(fileName.c_str()))
    {
        return -1;
    }

    FILE *in;
    in = fopen(fileName.c_str(), "wb");
    if (in == NULL)
    {
        fprintf(stderr, "Can't open file!\n");
        return -1;
    }
    fclose(in);

    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
    if (remove(fileName.c_str()) != 0)
    {
        return -1;
    }
    return 0;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    //check if file exists

    if(fileHandle.getFileHandle() != NULL)
    {
        return -1;
    }

    FILE *fpointer;
    fpointer = fopen(fileName.c_str(), "rb");
    fileHandle.setFileHandle(fpointer);
    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    FILE *fpointer = fileHandle.getFileHandle();
    if (fpointer == NULL)
    {
        return 1;
    }

    fclose(fpointer);
    fileHandle.setFileHandle(NULL);
    return 0;
}

FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle()
{
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    readPageCounter = readPageCounter + 1;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return 0;
}

RC FileHandle::appendPage(const void *data)
{
    return 0;
}

unsigned FileHandle::getNumberOfPages()
{
    return 0;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, 
    unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCounter = readPageCount;
    writePageCounter = writePageCount;
    appendPageCounter = appendPageCount;
    return 0;
}

RC FileHandle::setFileHandle(FILE *fd)
{
    fileUsed = fd;
    return 0; 
}

FILE * FileHandle::getFileHandle()
{
    return fileUsed; 
}
