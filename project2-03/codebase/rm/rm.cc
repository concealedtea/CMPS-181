#include "rm.h"

#include <algorithm>
#include <cstring>
#include <iostream>

using namespace std;

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tableDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableDescriptor.push_back(attr);

    attr.name = "system";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tableDescriptor.push_back(attr);

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnDescriptor.push_back(attr);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    if (rbfm->createFile("Tables.tbl") != 0)
    {
        return -1;
    }

    if (rbfm->createFile("Columns.tbl") != 0)
    {
        return -1;
    }

    if (insertTable(1, 1, "Tables") != 0)
    {
        return -1;
    }

    if (insertTable(2, 1, "Columns") != 0)
    {
        return -1;
    }

    if (insertColumns(1, tableDescriptor) != 0)
    {
        return -1;
    }

    if (insertColumns(2, columnDescriptor) != 0)
    {
        return -1;
    }

    return 0;
}

RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    if (rbfm->destroyFile("Tables.tbl") != 0)
    {
        return -1;
    }

    if (rbfm->destroyFile("Columns.tbl") != 0)
    {
        return -1;
    }

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    if (rbfm->createFile(tableName + ".tbl") != 0)
    {
        return -1;
    }

    int32_t id;

    if(rbfm->openFile("Tables.tbl", fileHandle) != 0)
    {
        return -1;
    }

    vector<string> projection;
    projection.push_back("table-id");

    RBFM_ScanIterator rbfm_si;
    if(rbfm->scan(fileHandle, tableDescriptor, "table-id", 
                  NO_OP, NULL, projection, rbfm_si) != 0) 
    {
        return -1;
    }

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    int32_t max_table_id = 0;
    while (rbfm_si.getNextRecord(rid, data) == 0)
    {
        int32_t tid;
        char nullByte = 0;
        memcpy(&nullByte, data, 1);
        memcpy(&tid, (char*) data + 1, INT_SIZE);
        if (tid > max_table_id)
            max_table_id = tid;
    }

    free(data);
    id = max_table_id + 1;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    if (insertTable(id, 0, tableName) != 0)
    {
        return -1;
    }

    if (insertColumns(id, attrs) != 0)
    {
        return -1;
    }

    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    if (rbfm->openFile("Tables.tbl", fileHandle) != 0)
    {
        return -1;
    }

    bool isSystem;
    if (isSystemTable(isSystem, tableName) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (isSystem)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (rbfm->destroyFile(tableName + ".tbl") != 0)
    {
        return -1;
    }

    int32_t id;
    if (getTableID(tableName, id) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    void *value = &id;

    if (rbfm->scan(fileHandle, tableDescriptor, "table-id", 
           EQ_OP, value, projection, rbfm_si) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    RID rid;
    if (rbfm_si.getNextRecord(rid, NULL) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    if (rbfm->openFile("Columns.tbl", fileHandle) != 0)
    {
        return -1;
    }

    rbfm->scan(fileHandle, columnDescriptor, "table-id", 
        EQ_OP, value, projection, rbfm_si);

    while(rbfm_si.getNextRecord(rid, NULL) == 0)
    {
        if(rbfm->deleteRecord(fileHandle, columnDescriptor, rid) != 0)
        {
            rbfm->closeFile(fileHandle);
            return -1;
        }
    }

    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    if (rbfm->openFile("Columns.tbl", fileHandle) != 0)
    {
        return -1;
    }

    attrs.clear();

    int32_t id;
    if (getTableID(tableName, id) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    void *value = &id;

    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back("column-name");
    projection.push_back("column-type");
    projection.push_back("column-length");
    projection.push_back("column-position");

    if (rbfm->scan(fileHandle, columnDescriptor, "table-id", 
                   EQ_OP, value, projection, rbfm_si) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    RID rid;
    void *data = malloc(COLUMNS_ATTR_SIZE);
    vector<IndexedAttr> iattrs;

    while (rbfm_si.getNextRecord(rid, data) == 0)
    {
        IndexedAttr attr;
        unsigned offset = 0;

        char nullByte;
        memcpy(&nullByte, data, 1);

        offset = 1;
        int32_t nameLen;
        memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char name[nameLen + 1];
        name[nameLen] = '\0';
        memcpy(name, (char*) data + offset, nameLen);
        offset += nameLen;
        attr.attr.name = string(name);

        int32_t type;
        memcpy(&type, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.type = (AttrType)type;

        int32_t length;
        memcpy(&length, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.length = length;

        int32_t pos;
        memcpy(&pos, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.pos = pos;

        iattrs.push_back(attr);
    }

    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);

    sort(iattrs.begin(), iattrs.end(), less<IndexedAttr>());
    
    for (auto attr : iattrs)
    {
        attrs.push_back(attr.attr);
    }
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    if (rbfm->openFile(tableName + ".tbl", fileHandle) != 0)
    {
        return -1;
    }

    bool isSystem;
    if (isSystemTable(isSystem, tableName) != 0 || isSystem)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rbfm->closeFile(fileHandle);

    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    if (rbfm->openFile(tableName + ".tbl", fileHandle) != 0)
    {
        return -1;
    }

    bool isSystem;
    if (isSystemTable(isSystem, tableName) != 0 || isSystem)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    
    if (rbfm->deleteRecord(fileHandle, recordDescriptor, rid) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rbfm->closeFile(fileHandle);

    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    if (rbfm->openFile(tableName + ".tbl", fileHandle) != 0)
    {
        return -1;
    }

    bool isSystem;
    if (isSystemTable(isSystem, tableName) != 0 || isSystem)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    if (rbfm->updateRecord(fileHandle, recordDescriptor, data, rid) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rbfm->closeFile(fileHandle);

    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    if (rbfm->openFile(tableName + ".tbl", fileHandle) != 0)
    {
        return -1;
    }

    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (rbfm->readRecord(fileHandle, recordDescriptor, rid, data) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    if (rbfm->openFile(tableName + ".tbl", fileHandle) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    if (rbfm->openFile("Columns.tbl", fileHandle) != 0)
    {
        return -1;
    }

    void *columnData = malloc(COLUMNS_ATTR_SIZE);
    RID rid;
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t pos = i+1;
        unsigned offset = 0;
        int32_t name_len = recordDescriptor[i].name.length();

        char null = 0;

        memcpy((char*) columnData + offset, &null, 1);
        offset += 1;

        memcpy((char*) columnData + offset, &id, INT_SIZE);
        offset += INT_SIZE;

        memcpy((char*) columnData + offset, &name_len, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        memcpy((char*) columnData + offset, recordDescriptor[i].name.c_str(), name_len);
        offset += name_len;

        int32_t type = recordDescriptor[i].type;
        memcpy((char*) columnData + offset, &type, INT_SIZE);
        offset += INT_SIZE;

        int32_t len = recordDescriptor[i].length;
        memcpy((char*) columnData + offset, &len, INT_SIZE);
        offset += INT_SIZE;

        memcpy((char*) columnData + offset, &pos, INT_SIZE);
        offset += INT_SIZE;

        if (rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid) != 0)
        {
            rbfm->closeFile(fileHandle);
            free(columnData);
            return -1;
        }
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return 0;
}

RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
    //cout << "insertTable: tableName is: " << tableName << endl;
    //cout << "insertTable: system is: " << system << endl;
    FileHandle fileHandle;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    if (rbfm->openFile("Tables.tbl", fileHandle) != 0)
    {
        return -1;
    }

    void *data = malloc (TABLES_ATTR_SIZE);

    uint8_t nullBytes = 0;
    unsigned offset = 0;
    memcpy((char*) data, &nullBytes, 1);
    offset += 1;

    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    int32_t name_len = tableName.length();
    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, tableName.c_str(), name_len);
    offset += name_len;

    string table_file_name = tableName + ".tbl";
    int32_t file_name_len = table_file_name.length();
    memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len;

    memcpy((char*) data + offset, &system, INT_SIZE);

    RID rid;
    rbfm->insertRecord(fileHandle, tableDescriptor, data, rid);

    rbfm->closeFile(fileHandle);
    free (data);
    return 0;
}

RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    if (rbfm->openFile("Tables.tbl", fileHandle) != 0)
    {
        return -1;
    }

    vector<string> projection;
    projection.push_back("table-id");
    
    void *value = malloc(4 + 50);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    RBFM_ScanIterator rbfm_si;
    if (rbfm->scan(fileHandle, tableDescriptor, "table-name", 
                   EQ_OP, value, projection, rbfm_si) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if (rbfm_si.getNextRecord(rid, data) == 0)
    {
        int32_t tid;
        char null = 0;
        memcpy(&null, data, 1);
        memcpy(&tid, (char*) data + 1, INT_SIZE);
        tableID = tid;
    }

    free(data);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return 0;
}

RC RelationManager::isSystemTable(bool &isSystem, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    if (rbfm->openFile("Tables.tbl", fileHandle) != 0)
    {
        return -1;
    }

    vector<string> projection;
    projection.push_back("system");

    void *value = malloc(4 + 50);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);
    
    RBFM_ScanIterator rbfm_si;
    if (rbfm->scan(fileHandle, tableDescriptor, "table-name", 
                   EQ_OP, value, projection, rbfm_si) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if (rbfm_si.getNextRecord(rid, data) == 0)
    {
        char nullByte = 0;
        memcpy(&nullByte, data, 1);
        memcpy(&isSystem, (char*) data + 1, INT_SIZE);
    }

    free(data);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return 0;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if(rbfm->openFile(tableName + ".tbl", rm_ScanIterator.fileHandle) != 0)
    {
        return -1;
    }

    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0)
    {
        rbfm->closeFile(rm_ScanIterator.fileHandle);
        return -1;
    }

    if (rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                   compOp, value, attributeNames, rm_ScanIterator.rbfm_iter) != 0)
    {
        rbfm->closeFile(rm_ScanIterator.fileHandle);
        return -1;
    }

    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}
