#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

#define IS_NULL -1
#define BUFFER_SIZE 200 // same as bufSize from qe_test_util.h

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
public:
    virtual RC getNextTuple(void *data) = 0;
    virtual void getAttributes(vector<Attribute> &attrs) const = 0;
    virtual ~Iterator() {};
protected:
    int getValue(const string &name, const vector<Attribute> &attrs, const void* data, void* value);
    int getNullIndicatorSize(int fieldCount);
    bool fieldIsNull(char *nullIndicator, int i);
    void setFieldNull(char *nullIndicator, int i);
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    string tableName;
    vector<Attribute> attrs;
    vector<string> attrNames;
    RID rid;
    
    TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
    {
        //Set members
        this->tableName = tableName;
        
        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);
        
        // Get Attribute Names from RM
        unsigned i;
        for(i = 0; i < attrs.size(); ++i)
        {
            // convert to char *
            attrNames.push_back(attrs.at(i).name);
        }
        
        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        
        // Set alias
        if(alias) this->tableName = alias;
    };
    
    // Start a new iterator given the new compOp and value
    void setIterator()
    {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };
    
    RC getNextTuple(void *data)
    {
        return iter->getNextTuple(rid, data);
    };
    
    void getAttributes(vector<Attribute> &attrs) const
    {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;
        
        // For attribute in vector<Attribute>, name it as rel.attr
        for(i = 0; i < attrs.size(); ++i)
        {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs.at(i).name;
            attrs.at(i).name = tmp;
        }
    };
    
    ~TableScan()
    {
        iter->close();
    };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    string tableName;
    string attrName;
    vector<Attribute> attrs;
    char key[PAGE_SIZE];
    RID rid;
    
    IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
    {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;
        
        
        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);
        
        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);
        
        // Set alias
        if(alias) this->tableName = alias;
    };
    
    // Start a new iterator given the new key range
    void setIterator(void* lowKey,
                     void* highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive)
    {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                     highKeyInclusive, *iter);
    };
    
    RC getNextTuple(void *data)
    {
        int rc = iter->getNextEntry(rid, key);
        if(rc == 0)
        {
            rc = rm.readTuple(tableName.c_str(), rid, data);
        }
        return rc;
    };
    
    void getAttributes(vector<Attribute> &attrs) const
    {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;
        
        // For attribute in vector<Attribute>, name it as rel.attr
        for(i = 0; i < attrs.size(); ++i)
        {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs.at(i).name;
            attrs.at(i).name = tmp;
        }
    };
    
    ~IndexScan()
    {
        iter->close();
    };
};


class Filter : public Iterator {
    // Filter operator
public:
    Iterator *iter;
    Condition cond;
    vector<Attribute> attrs;
    void *value;
    
    Filter(Iterator *input,               // Iterator of input R
           const Condition &condition     // Selection condition
    );
    ~Filter();
    
    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;
private:
    bool compare(const void* val1, const void* val2);
    bool compareValue(const int val1, const int val2);
    bool compareValue(const float val1, const float val2);
    bool compareValue(const string &val1, const string &val2);
};


class Project : public Iterator {
    // Projection operator
public:
    Iterator* iter;
    vector<string> attrNames;
    vector<Attribute> attrs;
    void *oldData;
    void *value;
    
    Project(Iterator *input,                    // Iterator of input R
            const vector<string> &attrNames);   // vector containing attribute names
    ~Project();
    
    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
public:
    Iterator *outer;
    IndexScan *inner;
    Condition cond;
    vector<Attribute> outerAttrs;
    vector<Attribute> innerAttrs;
    bool needNextOuterValue;
    void *outerData;
    void *innerData;
    void *value;
    
    INLJoin(Iterator *leftIn,           // Iterator of input R
            IndexScan *rightIn,          // IndexScan Iterator of input S
            const Condition &condition   // Join condition
    );
    ~INLJoin();
    
    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;
private:
    void concatData(const void *outerData, const void *innerData, void *data);
    int getLengthOfFields(const vector<Attribute> &attrs, const void *data);
};


#endif
