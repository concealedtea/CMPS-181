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
    _tableAttrs.push_back(Attribute("table-id", TypeInt, (AttrLength)4));
    _tableAttrs.push_back(Attribute("table-name", TypeVarChar, (AttrLength)50));
    _tableAttrs.push_back(Attribute("file-name", TypeVarChar, (AttrLength)50));
    _tableAttrs.push_back(Attribute("system", TypeInt,(AttrLength)4));

    _columnAttrs.push_back(Attribute("table-id", TypeInt, (AttrLength)4));
    _columnAttrs.push_back(Attribute("column-name", TypeVarChar, (AttrLength)50));
    _columnAttrs.push_back( Attribute("column-type", TypeInt, (AttrLength)4));
    _columnAttrs.push_back(Attribute("column-length", TypeInt, (AttrLength)4));
    _columnAttrs.push_back(Attribute("column-position", TypeInt, (AttrLength)4));
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

    if (insertColumns(1, _tableAttrs) != 0)
    {
        return -1;
    }

    if (insertColumns(2, _columnAttrs) != 0)
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

    vector<string> columns;
    columns.push_back("table-id");

    RBFM_ScanIterator iter;
    if(rbfm->scan(fileHandle, _tableAttrs, "table-id", 
                  NO_OP, NULL, columns, iter) != 0) 
    {
        return -1;
    }

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    int32_t max_table_id = 0;
    while (iter.getNextRecord(rid, data) == 0)
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
    iter.close();

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

    bool system;
    if (tableSystem(system, tableName) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (system)
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

    RBFM_ScanIterator iter;
    vector<string> columns;
    void *value = &id;

    if (rbfm->scan(fileHandle, _tableAttrs, "table-id", 
           EQ_OP, value, columns, iter) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    RID rid;
    if (iter.getNextRecord(rid, NULL) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    rbfm->deleteRecord(fileHandle, _tableAttrs, rid);
    rbfm->closeFile(fileHandle);
    iter.close();

    if (rbfm->openFile("Columns.tbl", fileHandle) != 0)
    {
        return -1;
    }

    rbfm->scan(fileHandle, _columnAttrs, "table-id", 
        EQ_OP, value, columns, iter);

    while(iter.getNextRecord(rid, NULL) == 0)
    {
        if(rbfm->deleteRecord(fileHandle, _columnAttrs, rid) != 0)
        {
            rbfm->closeFile(fileHandle);
            return -1;
        }
    }

    rbfm->closeFile(fileHandle);
    iter.close();

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

    int32_t id;
    if (getTableID(tableName, id) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    void *value = &id;

    RBFM_ScanIterator iter;
    vector<string> colAttrs({"column-name", "column-type", 
        "column-length", "column-position"});

    if (rbfm->scan(fileHandle, _columnAttrs, "table-id", 
                   EQ_OP, value, colAttrs, iter) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    RID rid;
    void *data = malloc(COLUMNS_ATTR_SIZE);
    vector<ColumnAttrs> columns;

    while (iter.getNextRecord(rid, data) == 0)
    {
        ColumnAttrs col;
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
        col.attr.name = string(name);

        int32_t type;
        memcpy(&type, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        col.attr.type = (AttrType)type;

        int32_t length;
        memcpy(&length, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        col.attr.length = length;

        int32_t pos;
        memcpy(&pos, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        col.pos = pos;

        columns.push_back(col);
    }

    sort(columns.begin(), columns.end(), less<ColumnAttrs>());

    attrs.clear();
    for (auto attr : columns)
    {
        attrs.push_back(attr.attr);
    }

    iter.close();
    rbfm->closeFile(fileHandle);
    free(data);

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

    bool system;
    if (tableSystem(system, tableName) != 0 || system)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    vector<Attribute> attrs;
    if (getAttributes(tableName, attrs) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (rbfm->insertRecord(fileHandle, attrs, data, rid) != 0)
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

    bool system;
    if (tableSystem(system, tableName) != 0 || system)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    vector<Attribute> attrs;
    if (getAttributes(tableName, attrs) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    
    if (rbfm->deleteRecord(fileHandle, attrs, rid) != 0)
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

    bool system;
    if (tableSystem(system, tableName) != 0 || system)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    vector<Attribute> attrs;
    if (getAttributes(tableName, attrs) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    if (rbfm->updateRecord(fileHandle, attrs, data, rid) != 0)
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

    vector<Attribute> attrs;
    if (getAttributes(tableName, attrs) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (rbfm->readRecord(fileHandle, attrs, rid, data) != 0)
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

    vector<Attribute> attrs;
    if (getAttributes(tableName, attrs) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    if (rbfm->openFile("Columns.tbl", fileHandle) != 0)
    {
        return -1;
    }

    void *columnData = malloc(COLUMNS_ATTR_SIZE);
    RID rid;
    for (unsigned i = 0; i < attrs.size(); i++)
    {
        int32_t pos = i+1;
        unsigned offset = 0;
        int32_t name_len = attrs[i].name.length();

        char null = 0;

        memcpy((char*) columnData + offset, &null, 1);
        offset += 1;

        memcpy((char*) columnData + offset, &id, INT_SIZE);
        offset += INT_SIZE;

        memcpy((char*) columnData + offset, &name_len, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        memcpy((char*) columnData + offset, attrs[i].name.c_str(), name_len);
        offset += name_len;

        int32_t type = attrs[i].type;
        memcpy((char*) columnData + offset, &type, INT_SIZE);
        offset += INT_SIZE;

        int32_t len = attrs[i].length;
        memcpy((char*) columnData + offset, &len, INT_SIZE);
        offset += INT_SIZE;

        memcpy((char*) columnData + offset, &pos, INT_SIZE);
        offset += INT_SIZE;

        if (rbfm->insertRecord(fileHandle, _columnAttrs, columnData, rid) != 0)
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
    rbfm->insertRecord(fileHandle, _tableAttrs, data, rid);

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

    vector<string> columns;
    columns.push_back("table-id");

    RBFM_ScanIterator iter;
    if (rbfm->scan(fileHandle, _tableAttrs, "table-name", 
                   EQ_OP, tableName.c_str(), columns, iter) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if (iter.getNextRecord(rid, data) == 0)
    {
        int32_t tid;
        char nullBytes = 0;
        memcpy(&nullBytes, data, 1);
        memcpy(&tid, (char*) data + 1, INT_SIZE);
        tableID = tid;
    }

    free(data);
    rbfm->closeFile(fileHandle);
    iter.close();
    return 0;
}

RC RelationManager::tableSystem(bool &system, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    if (rbfm->openFile("Tables.tbl", fileHandle) != 0)
    {
        return -1;
    }

    vector<string> columns;
    columns.push_back("system");

    RBFM_ScanIterator iter;
    if (rbfm->scan(fileHandle, _tableAttrs, "table-name", 
                   EQ_OP, tableName.c_str(), columns, iter) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if (iter.getNextRecord(rid, data) == 0)
    {
        char nullByte = 0;
        memcpy(&nullByte, data, 1);
        memcpy(&system, (char*) data + 1, INT_SIZE);
    }

    free(data);
    rbfm->closeFile(fileHandle);
    iter.close();
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
    if (rbfm->openFile(tableName + ".tbl", rm_ScanIterator.fileHandle) != 0)
    {
        return -1;
    }

    vector<Attribute> attrs;
    if (getAttributes(tableName, attrs) != 0)
    {
        rbfm->closeFile(rm_ScanIterator.fileHandle);
        return -1;
    }

    if (rbfm->scan(rm_ScanIterator.fileHandle, attrs, conditionAttribute,
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
