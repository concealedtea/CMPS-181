#include "pfm.h"
#include <stdio.h>
#include <iostream>
#include <sys/stat.h>

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

bool PagedFileManager::fileExists(const string &fileName)
{
    struct stat stBuff;
    if (stat(fileName.c_str(), &stBuff) == 0)
    {
        return true;
    }
    else
    {
        return false;
    }
}

RC PagedFileManager::createFile(const string &fileName)
{
    if (fileExists(fileName))
    {
        cerr << fileName << "file exists already!" << endl;
        return -1;
    }

    FILE *in;
    in = fopen(fileName.c_str(), "wb+");
    if (in == NULL)
    {
        cerr << "Can't open file:" << fileName << endl;
        return -1;
    }
    fclose(in);

    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
    if (remove(fileName.c_str()) != 0)
    {
        cerr << "Failed to remove file: " << fileName << endl;
        return -1;
    }
    return 0;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (fileHandle.getFileHandle() != NULL)
    {
        return -1;
    }

    if (!fileExists(fileName))
    {
        return -1;
    }

    FILE *fpointer;
    fpointer = fopen(fileName.c_str(), "rb+");
    if (fpointer == NULL)
    {
        cerr << "Cannot open file: " << fileName << endl;
        return -1;
    }
    fileHandle.setFileHandle(fpointer);
    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    FILE *fpointer = fileHandle.getFileHandle();
    if (fpointer == NULL)
    {
        return -1;
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
    fileUsed = NULL;
}

FileHandle::~FileHandle()
{
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (getNumberOfPages() < pageNum)
    {
        return -1;
    }
    fseek(fileUsed, pageNum * PAGE_SIZE, SEEK_SET);
    fread(data, 1, PAGE_SIZE, fileUsed);
    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    fseek(fileUsed, pageNum * PAGE_SIZE, SEEK_SET);
    fwrite(data, 1, PAGE_SIZE, fileUsed);
    writePageCounter++;
    return 0;
}

RC FileHandle::appendPage(const void *data)
{
    fseek(fileUsed, 0, SEEK_END);
    fwrite(data, 1, PAGE_SIZE, fileUsed);
    //fflush(fileUsed);
    appendPageCounter++;
    return 0;
}

unsigned FileHandle::getNumberOfPages()
{
    struct stat st;
    if (fstat(fileno(fileUsed), &st) != 0)
    {
        return 0;
    }
    return st.st_size / PAGE_SIZE;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, 
    unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
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
