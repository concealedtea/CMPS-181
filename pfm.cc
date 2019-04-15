#include "pfm.h"

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
    if ((file = fopen(fname, "r")))
    {
        fclose(file);
        return 1;
    }
    return 0;
}


RC PagedFileManager::createFile(const string &fileName)
{
    return -1;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    return -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if(!fileExists(fileName.c_str())){
        cout << "Error: File does not exist. Cannot open." << endl;
        return -1; 
    }

    if(fileHandle.getFileHandle() != NULL){
        cout << "Error: FileHandle already exists for this file." << endl;
        return -1;
    }

    FILE * fpointer;
    fpointer = fopen(filename.c_str(), "rb");
    fileHandle.setFileHandle(fpointer);
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return -1;
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
    if(fileUsed == NULL){
        cout << "Error: This FileHandle has no File." << endl;
        return -1;
    }

    //check that page exists

    fseek(fileUsed, (pageNum*PAGE_SIZE), SEEK_SET);
    fread(data, 1, PAGE_SIZE, fileUsed);
    readPageCounter++;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(fileUsed == NULL){
        cout << "Error: This FileHandle has no File." << endl;
        return -1;
    }

    //check that page exists

    fseek(fileUsed, (pageNum*PAGE_SIZE), SEEK_SET);
    fwrite(data, 1, PAGE_SIZE, fileUsed);
    writePageCounter++;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    if(fileUsed == NULL){
        cout << "Error: This FileHandle has no File." << endl;
        return -1;
    }
    
    fseek(fileUsed, 0, SEEK_END);
    fwrite(data, 1, PAGE_SIZE, fileUsed);
    writePageCounter++;
    return 0;

}


unsigned FileHandle::getNumberOfPages()
{
    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
	return 0;
}

RC FileHandle::setFileHandle(FILE * fd)
{
    fileUsed = fd;
    return 0; 
}

FILE * FileHandle::getFileHandle()
{
    return fileUsed; 
}
