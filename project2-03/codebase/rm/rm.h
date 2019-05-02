
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator() {};
    ~RM_ScanIterator() {};

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data);
    RC close();

    friend class RelationManager;
private:
    RBFM_ScanIterator rbfm_iter;
    FileHandle fileHandle;
};


// Relation Manager
class RelationManager
{
public:
    const int TABLES_ATTR_SIZE = 1 + 4 * 4 + 50 + 50;
    const int COLUMNS_ATTR_SIZE = 1 + 5 * 4 + 50;
    static RelationManager* instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const string &tableName, const vector<Attribute> &attrs);

    RC deleteTable(const string &tableName);

    RC getAttributes(const string &tableName, vector<Attribute> &attrs);

    RC insertTuple(const string &tableName, const void *data, RID &rid);

    RC deleteTuple(const string &tableName, const RID &rid);

    RC updateTuple(const string &tableName, const void *data, const RID &rid);

    RC readTuple(const string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const vector<Attribute> &attrs, const void *data);

    RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const string &tableName,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);


protected:
    RelationManager();
    ~RelationManager();

private:
    typedef struct ColumnAttrs
    {
        int32_t pos;
        Attribute attr;
        bool operator < (const ColumnAttrs& t) const
        {
            return (pos < t.pos);
        }
    } ColumnAttrs;

    static RelationManager *_rm;
    vector<Attribute> _tableAttrs;
    vector<Attribute> _columnAttrs;


    RC insertColumns(int32_t id, const vector<Attribute> &recordDescriptor);

    RC insertTable(int32_t id, int32_t system, const string &tableName);

    RC getTableID(const string &tableName, int32_t &tableID);

    RC tableSystem(bool &system, const string &tableName);

};

#endif
