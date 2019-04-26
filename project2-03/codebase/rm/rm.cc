
#include "rm.h"

RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager RelationManager::*_rbfm = NULL;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    // Create files for catalog
    RC tableRC = _rbfm->createFile("Tables.cata");
    RC columnRC = _rbfm->createFile("Columns.cata");
    if (tableRC != SUCCESS || columnRC != SUCCESS)
        return RM_CREATE_CATALOG_FAILED;
    
    // Create record descriptors for Tables and Columns
    vector<Attribute> tableAttr;
    vector<Attribute> columnAttr;
    createTablesAttrs(tableAttr);
    createColumnsAttrs(columnAttr);
    
    // Insert records for Tables
    FileHandle fh;
    void* data = nullptr;
    RID rid;
    _rbfm->openFile("Tables.cata", fh);
    prepareTablesRecord(1, "Tables", "Tables.cata", data);
    _rbfm->insertRecord(fh, tableAttr, data, rid);
    // Insert records for Columns
    _rbfm->openFile("Columns.cata", fh);
    
    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

void RelationManager::createTablesAttrs(vector<Attribute> &attrs) {
    Attribute attr;
    
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);
    
    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    attrs.push_back(attr);
    
    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    attrs.push_back(attr);
}

void RelationManager::createColumnsAttrs(vector<Attribute> &attrs) {
    Attribute attr;
    
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);
    
    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    attrs.push_back(attr);
    
    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);
    
    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);
    
    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);
}

void prepareTablesRecord(const int tableId, const string &tableName, const string &fileName, void *data) {
    
}

void prepareColumnsRecord(const int tableId, const string &columnName, const int columnType, const int columnLength, const int columnPosition, void *data) {
    
}
