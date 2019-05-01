#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

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

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting up the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;
    
    // Check if can use empty slot
    SlotDirectoryRecordEntry recordEntry;
    for (unsigned j = 0; j < slotHeader.recordEntriesNumber; j++) {
        recordEntry = getSlotDirectoryRecordEntry(pageData, j);
        if (recordEntry.offset == 0 && recordEntry.length == 0) {
            rid.slotNum = j;
            slotHeader.recordEntriesNumber -= 1; // so don't have to change later code
            break;
        }
    }

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    
    // Check that the slot is not empty
    if (recordEntry.offset == 0)
        return RBFM_RECORD_DN_EXIST;
    
    // Check if using forwarding address
    if (recordEntry.offset < 0) {
        RID forwardRid;
        forwardRid.pageNum = -1 * recordEntry.offset;
        forwardRid.slotNum = recordEntry.length;
        readRecord(fileHandle, recordDescriptor, forwardRid, data);
        free(pageData);
        return SUCCESS;
    }
    
    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator and save it into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;
    
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;
    
    // Check that the slot is not empty
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    if (recordEntry.offset == 0)
        return RBFM_RECORD_DN_EXIST;
    
    // Check if using forwarding address
    if (recordEntry.offset < 0) {
        // delete record
        RID forwardRid;
        forwardRid.pageNum = -1 * recordEntry.offset;
        forwardRid.slotNum = recordEntry.length;
        deleteRecord(fileHandle, recordDescriptor, forwardRid);
        // set deleted record's entry in the slot directory to 0,0.
        recordEntry.offset = 0;
        recordEntry.length = 0;
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
        
        if (fileHandle.writePage(rid.pageNum, pageData))
            return RBFM_WRITE_FAILED;
        free(pageData);
        return SUCCESS;
    }
    
    // Get length of deleted data
    uint32_t deletedDataSize = recordEntry.length;
    int32_t oldOffset = recordEntry.offset;
    
    // Delete the data by moving data over the deleted record.
    uint32_t compactableDataSize = oldOffset - slotHeader.freeSpaceOffset;
    unsigned newFreeSpaceOffset = slotHeader.freeSpaceOffset + deletedDataSize;
    char *compactableData = (char*) malloc(compactableDataSize);
    if (compactableData == NULL)
        return RBFM_MALLOC_FAILED;
    memcpy(compactableData, ((char*) pageData + slotHeader.freeSpaceOffset), compactableDataSize);
    memcpy((char*) pageData + newFreeSpaceOffset, compactableData, compactableDataSize);
    free(compactableData);
    
    // Update slot directory record entries with offsets closer to middle
    for (unsigned i = 0; i < slotHeader.recordEntriesNumber; i++) {
        recordEntry = getSlotDirectoryRecordEntry(pageData, i);
        // recordEntry.offset > 0, don't want to update deleted or forwarded records
        if (recordEntry.offset > 0 && recordEntry.offset < oldOffset) {
            recordEntry.offset += deletedDataSize;
            setSlotDirectoryRecordEntry(pageData, i, recordEntry);
        }
    }
    
    // Set the deleted record's reference in the slot directory to 0,0.
    recordEntry.offset = 0;
    recordEntry.length = 0;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
    
    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newFreeSpaceOffset;
    setSlotDirectoryHeader(pageData, slotHeader);
    
    if (fileHandle.writePage(rid.pageNum, pageData))
        return RBFM_WRITE_FAILED;
    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;
    
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;
    
    // Check that the slot is not empty
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    if (recordEntry.offset == 0)
        return RBFM_RECORD_DN_EXIST;
    // Check if using forwarding address
    if (recordEntry.offset < 0) {
        // Delete old record
        RID forwardRid;
        forwardRid.pageNum = -1 * recordEntry.offset;
        forwardRid.slotNum = recordEntry.length;
        deleteRecord(fileHandle, recordDescriptor, forwardRid);
        // Set record entry so may be inserted into this page
        recordEntry.length = 0;
        recordEntry.offset = slotHeader.freeSpaceOffset;
    }
    
    // Get the old size and new size of the record.
    unsigned oldRecordSize = recordEntry.length;
    unsigned newRecordSize = getRecordSize(recordDescriptor, data);
    int32_t oldOffset = recordEntry.offset;
    unsigned newFreeSpaceOffset = slotHeader.freeSpaceOffset;
    // Check if enough space for updated record
    if (getPageFreeSpaceSize(pageData) + oldRecordSize >= newRecordSize) {
        if (newRecordSize == oldRecordSize) { // only need to overwrite record
            setRecordAtOffset (pageData, oldOffset, recordDescriptor, data);
        }
        else if (newRecordSize < oldRecordSize) {
            unsigned sizeDifference = oldRecordSize - newRecordSize;
            newFreeSpaceOffset += sizeDifference;
            // Compact data
            uint32_t compactableDataSize = oldOffset - slotHeader.freeSpaceOffset;
            char *compactableData = (char*) malloc(compactableDataSize);
            if (compactableData == NULL)
                return RBFM_MALLOC_FAILED;
            memcpy(compactableData, ((char*) pageData + slotHeader.freeSpaceOffset), compactableDataSize);
            memcpy((char*) pageData + newFreeSpaceOffset, compactableData, compactableDataSize);
            free(compactableData);
            
            // Write in new data
            setRecordAtOffset (pageData, oldOffset + sizeDifference, recordDescriptor, data);
            
            // Update record entries
            for (unsigned i = 0; i < slotHeader.recordEntriesNumber; i++) {
                recordEntry = getSlotDirectoryRecordEntry(pageData, i);
                if (recordEntry.offset > 0 && recordEntry.offset < oldOffset) {
                    recordEntry.offset += sizeDifference;
                    setSlotDirectoryRecordEntry(pageData, i, recordEntry);
                }
            }
            recordEntry.offset = oldOffset + sizeDifference;
            recordEntry.length = newRecordSize;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
        }
        else { // newRecordSize > oldRecordSize
            unsigned sizeDifference = newRecordSize - oldRecordSize;
            newFreeSpaceOffset -= sizeDifference;
            if (oldRecordSize == 0) { // forwarding address was used, so nothing in this page
                setRecordAtOffset (pageData, oldOffset - sizeDifference, recordDescriptor, data);
            }
            else { // no forwarding address used, so need to loosen data
                // Loosen data
                uint32_t compactableDataSize = oldOffset - slotHeader.freeSpaceOffset;
                char *compactableData = (char*) malloc(compactableDataSize);
                if (compactableData == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(compactableData, ((char*) pageData + slotHeader.freeSpaceOffset), compactableDataSize);
                memcpy((char*) pageData + newFreeSpaceOffset, compactableData, compactableDataSize);
                free(compactableData);
                // Write in new data
                setRecordAtOffset (pageData, oldOffset - sizeDifference, recordDescriptor, data);
                // Update record entries
                for (unsigned i = 0; i < slotHeader.recordEntriesNumber; i++) {
                    recordEntry = getSlotDirectoryRecordEntry(pageData, i);
                    if (recordEntry.offset > 0 && recordEntry.offset < oldOffset) {
                        recordEntry.offset -= sizeDifference;
                        setSlotDirectoryRecordEntry(pageData, i, recordEntry);
                    }
                }
            }
            recordEntry.offset = oldOffset - sizeDifference;
            recordEntry.length = newRecordSize;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
        }
        // Updating the slot directory header.
        slotHeader.freeSpaceOffset = newFreeSpaceOffset;
        setSlotDirectoryHeader(pageData, slotHeader);
    }
    else { // Not enough space for updated record
        RID newRid;
        insertRecord(fileHandle, recordDescriptor, data, newRid);
        if (oldRecordSize != 0) { // a record exists to delete (did not have forwarding address before)
            deleteRecord(fileHandle, recordDescriptor, rid);
            free(pageData);
            pageData = malloc(PAGE_SIZE);
            if (fileHandle.readPage(rid.pageNum, pageData))
                return RBFM_READ_FAILED;
        }
        // Set forwarding address
        recordEntry.offset = -1 * newRid.pageNum;
        recordEntry.length = newRid.slotNum;
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
    }
    
    if (fileHandle.writePage(rid.pageNum, pageData))
        return RBFM_WRITE_FAILED;
    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;
    
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;
    
    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    
    // Check that the slot is not empty
    if (recordEntry.offset == 0)
        return RBFM_RECORD_DN_EXIST;
    
    // Check if using forwarding address
    if (recordEntry.offset < 0) {
        RID forwardRid;
        forwardRid.pageNum = -1 * recordEntry.offset;
        forwardRid.slotNum = recordEntry.length;
        readAttribute(fileHandle, recordDescriptor, forwardRid, attributeName, data);
        free(pageData);
        return SUCCESS;
    }
    
    // Retrieve the actual entry data
    getAttributeAtOffset(pageData, recordEntry.offset, recordDescriptor, attributeName, data);
    
    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute,
                                const CompOp compOp,                  // comparision type such as "<" and "="
                                const void *value,                    // used in the comparison
                                const vector<string> &attributeNames, // a list of projected attributes
                                RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator.fileHandle = &fileHandle;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = (void*) value;
    
    rbfm_ScanIterator.page = malloc(PAGE_SIZE);
    rbfm_ScanIterator.currPage = -1;
    rbfm_ScanIterator.nextSlot = 0;
    rbfm_ScanIterator.numSlots = 0;
    
    // Find out indices of attributes
    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        // Find indices for project attributes
        for (unsigned j = 0; j < attributeNames.size(); j++) {
            if (attributeNames[j] == recordDescriptor[i].name) {
                rbfm_ScanIterator.attributeIndices.push_back(i);
                break;
            }
        }
        // Find index for condition attribute
        if (conditionAttribute == recordDescriptor[i].name) {
            rbfm_ScanIterator.conditionAttributeIndex = i;
        }
    }
    
    return SUCCESS;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    // Loop until find a record that satisfies compOp, or RBFM_EOF
    bool found;
    do {
        // Get next record entry
        unsigned offset;
        do {
            // Need to read next page; while loop in case next page is empty
            while (nextSlot >= numSlots) {
                currPage++;
                if (fileHandle->readPage(currPage, page))
                    return RBFM_EOF;
                SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
                numSlots = slotHeader.recordEntriesNumber;
                nextSlot = 0;
            }
            SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(page, nextSlot);
            nextSlot++;
            offset = recordEntry.offset;
        } while (offset <= 0); // offset < 0 mean forwarding address; offset = 0 mean no record
        
        // Below copied from getRecordAtOffset()
        // Pointer to start of record
        char *start = (char*) page + offset;
        
        // Allocate space for null indicator.
        int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
        char nullIndicator[nullIndicatorSize];
        memset(nullIndicator, 0, nullIndicatorSize);
        
        // Get number of columns and size of the null indicator for this record
        RecordLength len = 0;
        memcpy (&len, start, sizeof(RecordLength));
        int recordNullIndicatorSize = getNullIndicatorSize(len);
        
        // Read in the existing null indicator
        memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);
        
        // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
        for (unsigned i = len; i < recordDescriptor.size(); i++)
        {
            int indicatorIndex = (i+1) / CHAR_BIT;
            int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
            nullIndicator[indicatorIndex] |= indicatorMask;
        }
        // Above copied from getRecordAtOffset()
        
        // Determine if record satisfies compOp
        if (compOp == NO_OP) { // no compOp mean every record
            found = true;
        }
        else if (fieldIsNull(nullIndicator, conditionAttributeIndex)) { // any compOp with NULL is false
            found = false;
        }
        else { // compOp != NO_OP && field is not null
            // Get condition attribute offsets
            ColumnOffset leftOffset, rightOffset;
            if (conditionAttributeIndex == 0) {
                leftOffset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
            }
            else {
                memcpy(&leftOffset, start + sizeof(RecordLength) + recordNullIndicatorSize + (conditionAttributeIndex - 1) * sizeof(ColumnOffset), sizeof(ColumnOffset));
            }
            memcpy(&rightOffset, start + sizeof(RecordLength) + recordNullIndicatorSize + conditionAttributeIndex * sizeof(ColumnOffset), sizeof(ColumnOffset));
            uint32_t fieldSize = rightOffset - leftOffset;
            // Get attribute and do comparison
            switch (recordDescriptor[conditionAttributeIndex].type) {
                case TypeInt:
                    int data_integer;
                    memcpy(&data_integer, start + leftOffset, fieldSize);
                    found = compareValue(data_integer);
                    break;
                case TypeReal:
                    float data_real;
                    memcpy(&data_real, start + leftOffset, fieldSize);
                    found = compareValue(data_real);
                    break;
                case TypeVarChar:
                    string data_string = string(start + leftOffset, fieldSize);
                    found = compareValue(data_string);
                    break;
            }
        }
        
        // Get desired attributes from record
        int dataNullIndicatorSize = getNullIndicatorSize(attributeIndices.size());
        char dataNullIndicator[dataNullIndicatorSize];
        memset(dataNullIndicator, 0, dataNullIndicatorSize);
        ColumnOffset leftOffset, rightOffset;
        unsigned data_offset = dataNullIndicatorSize;
        unsigned attrIndex;
        for (unsigned i = 0; i < attributeIndices.size(); i++) {
            attrIndex = attributeIndices[i];
            // If null in record, set null indicator in data
            if (fieldIsNull(nullIndicator, attrIndex)) {
                int indicatorIndex = (i+1) / CHAR_BIT;
                int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
                dataNullIndicator[indicatorIndex] |= indicatorMask;
            }
            else {
                // Get attribute offsets
                if (attrIndex == 0) {
                    leftOffset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
                }
                else {
                    memcpy(&leftOffset, start + sizeof(RecordLength) + recordNullIndicatorSize + (attrIndex - 1) * sizeof(ColumnOffset), sizeof(ColumnOffset));
                }
                memcpy(&rightOffset, start + sizeof(RecordLength) + recordNullIndicatorSize + attrIndex * sizeof(ColumnOffset), sizeof(ColumnOffset));
                uint32_t fieldSize = rightOffset - leftOffset;
                // Get attribute
                // Special case for varchar, we must give data the size of varchar first
                if (recordDescriptor[attrIndex].type == TypeVarChar)
                {
                    memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
                    data_offset += VARCHAR_LENGTH_SIZE;
                }
                // Next we copy bytes equal to the size of the field and increase our offsets
                memcpy((char*) data + data_offset, start + leftOffset, fieldSize);
                data_offset += fieldSize;
            }
        }
        // Write out data's null indicator
        memcpy(data, dataNullIndicator, dataNullIndicatorSize);
    } while (!found);
    
    rid.pageNum = currPage;
    rid.slotNum = nextSlot - 1;
    return SUCCESS;
}

RC RBFM_ScanIterator::close() {
    free(page);
    return SUCCESS;
}

//
bool RBFM_ScanIterator::compareValue(const int val1) {
    int val2 = *(int*)value;
    switch (compOp) {
        case EQ_OP: return (val1 == val2);
        case LT_OP: return (val1 <  val2);
        case LE_OP: return (val1 <= val2);
        case GT_OP: return (val1 >  val2);
        case GE_OP: return (val1 >= val2);
        case NE_OP: return (val1 != val2);
        case NO_OP: return true;
    }
    return true;
}

bool RBFM_ScanIterator::compareValue(const float val1) {
    float val2 = *(float*)value;
    switch (compOp) {
        case EQ_OP: return (val1 == val2);
        case LT_OP: return (val1 <  val2);
        case LE_OP: return (val1 <= val2);
        case GT_OP: return (val1 >  val2);
        case GE_OP: return (val1 >= val2);
        case NE_OP: return (val1 != val2);
        case NO_OP: return true;
    }
    return true;
}

bool RBFM_ScanIterator::compareValue(const string val1) {
    string val2 = string((char*)(value));
    switch (compOp) {
        case EQ_OP: return (val1 == val2);
        case LT_OP: return (val1 <  val2);
        case LE_OP: return (val1 <= val2);
        case GT_OP: return (val1 >  val2);
        case GE_OP: return (val1 >= val2);
        case NE_OP: return (val1 != val2);
        case NO_OP: return true;
    }
    return true;
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
	memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getAttributeAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const string &attributeName, void *data) {
    // Pointer to start of record
    char *start = (char*) page + offset;
    
    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    
    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);
    
    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);
    
    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Above is same as getRecordAtOffset
    // Find index of desired attribute
    unsigned index;
    for (index = 0; index < recordDescriptor.size(); index++) {
        if (attributeName == recordDescriptor[index].name)
            break;
    }
    if (fieldIsNull(nullIndicator, index)) {
        // Set null indicator for data
        memset(data, 0x80, 1);
    }
    else {
        // Set null indicator for data
        memset(data, 0x00, 1);
        ColumnOffset leftOffset, rightOffset;
        if (index == 0) {
            leftOffset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
        }
        else {
            memcpy(&leftOffset, start + sizeof(RecordLength) + recordNullIndicatorSize + (index - 1) * sizeof(ColumnOffset), sizeof(ColumnOffset));
        }
        memcpy(&rightOffset, start + sizeof(RecordLength) + recordNullIndicatorSize + index * sizeof(ColumnOffset), sizeof(ColumnOffset));
        uint32_t fieldSize = rightOffset - leftOffset;
        if (recordDescriptor[index].type == TypeVarChar) {
            memcpy((char*) data + 1, &fieldSize, VARCHAR_LENGTH_SIZE);
            memcpy((char*) data + 1 + VARCHAR_LENGTH_SIZE, start + leftOffset, fieldSize);
        }
        else {
            memcpy((char*) data + 1, start + leftOffset, fieldSize);
        }
    }
}

// Copies of helper functions for RBFM_ScanIterator
int RBFM_ScanIterator::getNullIndicatorSize(int fieldCount)
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RBFM_ScanIterator::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

SlotDirectoryHeader RBFM_ScanIterator::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

SlotDirectoryRecordEntry RBFM_ScanIterator::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
             &recordEntry,
             ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
             sizeof(SlotDirectoryRecordEntry)
             );
    
    return recordEntry;
}
