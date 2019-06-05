
#include "qe.h"
#include <cmath>
#include <cstring>

int Iterator::getValue(const string &name, const vector<Attribute> &attrs, const void* data, void* value) {
    int nullIndicatorSize = getNullIndicatorSize(attrs.size());
    int offset = nullIndicatorSize;
    uint32_t size = 0;
    char nullIndicator[nullIndicatorSize];
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    for (unsigned i = 0; i < attrs.size(); i++) {
        // at desired attribute
        if (name == attrs[i].name) {
            if (fieldIsNull(nullIndicator, i)) {
                return IS_NULL;
            }
            else { // extract value
                size = 0;
                if (attrs[i].type == TypeVarChar) {
                    memcpy(&size, (char*)data + offset, 4);
                    memcpy((char*)value, &size, 4); // 4 bytes for size of varchar
                    memcpy((char*)value + 4, (char*)data + offset + 4, size); // varchar
                }
                else {
                    memcpy((char*)value, (char*)data + offset, 4);
                }
                size += 4;
            }
            break;
        }
        // increment offset
        else if (!fieldIsNull(nullIndicator, i)) { // check not null
            if (attrs[i].type == TypeVarChar) {
                memcpy(&size, (char*)data + offset, 4);
                offset += size;
            }
            offset += 4;
        }
    }
    return size;
}

int Iterator::getNullIndicatorSize(int fieldCount) {
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool Iterator::fieldIsNull(char *nullIndicator, int i) {
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void Iterator::setFieldNull(char *nullIndicator, int i) {
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    nullIndicator[indicatorIndex] |= indicatorMask;
}

Filter::Filter(Iterator* input, const Condition &condition) {
    iter = input;
    cond = condition;
    input->getAttributes(attrs);
    value = malloc(BUFFER_SIZE);
}

Filter::~Filter() {
    free(value);
}

RC Filter::getNextTuple(void *data) {
    // loop until find a tuple that satisfies cond
    while (true) {
        if (iter->getNextTuple(data) == QE_EOF) // EOF
            return QE_EOF;
        if (cond.op == NO_OP) // NO_OP, so found
            break;
        if (getValue(cond.lhsAttr, attrs, data, value) == IS_NULL) // null, so not found
            continue;
        if (compare(value, cond.rhsValue.data)) // do compare, if true, then found
            break;
    }
    return SUCCESS;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = this->attrs;
}

bool Filter::compare(const void* val1, const void* val2) {
    bool result = false;
    switch (cond.rhsValue.type) {
        case TypeInt:
            int i1, i2;
            memcpy(&i1, val1, 4);
            memcpy(&i2, val2, 4);
            result = compareValue(i1, i2);
            break;
        case TypeReal:
            float r1, r2;
            memcpy(&r1, val1, 4);
            memcpy(&r2, val2, 4);
            result = compareValue(r1, r2);
            break;
        case TypeVarChar:
            uint32_t size1, size2;
            string s1, s2;
            memcpy(&size1, val1, 4);
            memcpy(&size2, val2, 4);
            s1 = string((char*)val1 + 4, size1);
            s2 = string((char*)val2 + 4, size2);
            result = compareValue(s1, s2);
            break;
    }
    return result;
}

bool Filter::compareValue(const int val1, const int val2) {
    switch (cond.op) {
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

bool Filter::compareValue(const float val1, const float val2) {
    switch (cond.op) {
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

bool Filter::compareValue(const string &val1, const string &val2) {
    switch (cond.op) {
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

Project::Project(Iterator *input, const vector<string> &attrNames) {
    iter = input;
    this->attrNames = attrNames;
    input->getAttributes(attrs);
    oldData = malloc(BUFFER_SIZE);
    value = malloc(BUFFER_SIZE);
}

Project::~Project() {
    free(oldData);
    free(value);
}

RC Project::getNextTuple(void *data) {
    if (iter->getNextTuple(oldData) == QE_EOF)
        return QE_EOF;
    
    // initialize result data's null indicator to 0's
    int nullIndicatorSize = getNullIndicatorSize(attrNames.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    
    int offset = nullIndicatorSize;
    int size = 0;
    for (unsigned i = 0; i < attrNames.size(); i++) {
        size = getValue(attrNames[i], attrs, oldData, value);
        if (size == IS_NULL) { // set attribute in null indicator to 1
            setFieldNull(nullIndicator, i);
        }
        else { // copy value into data
            memcpy((char*)data + offset, value, size);
            offset += size;
        }
    }
    memcpy(data, nullIndicator, nullIndicatorSize); // copy null indicator into data
    return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    for (auto &name : this->attrNames) {
        for (auto &attr : this->attrs) {
            if (name == attr.name)
                attrs.push_back(attr);
        }
    }
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    outer = leftIn;
    inner = rightIn;
    cond = condition;
    leftIn->getAttributes(outerAttrs);
    rightIn->getAttributes(innerAttrs);
    needNextOuterValue = true;
    outerData = malloc(BUFFER_SIZE);
    innerData = malloc(BUFFER_SIZE);
    value = malloc(BUFFER_SIZE);
}

INLJoin::~INLJoin() {
    free(outerData);
    free(innerData);
    free(value);
}

RC INLJoin::getNextTuple(void *data) {
    // loop until find a tuple that satisfies cond
    while (true) {
        if (needNextOuterValue) {
            if (outer->getNextTuple(outerData) == QE_EOF)
                return QE_EOF;
            getValue(cond.lhsAttr, outerAttrs, outerData, value);
            inner->setIterator(value, value, true, true);
            needNextOuterValue = false;
        }
        if (inner->getNextTuple(innerData) == QE_EOF) {
            needNextOuterValue = true;
            continue;
        }
        break;
    }
    concatData(outerData, innerData, data);
    return SUCCESS;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    for (auto &attr : outerAttrs)
        attrs.push_back(attr);
    for (auto &attr : innerAttrs)
        attrs.push_back(attr);
}

void INLJoin::concatData(const void *outerData, const void *innerData, void *data) {
    // Make result data's null indicator
    int outerNullIndicatorSize = getNullIndicatorSize(outerAttrs.size());
    int innerNullIndicatorSize = getNullIndicatorSize(innerAttrs.size());
    int resultNullIndicatorSize = getNullIndicatorSize(outerAttrs.size() + innerAttrs.size());
    
    // initialize result null indicator to 0, then put in outer null indicator
    char resultNullIndicator[resultNullIndicatorSize];
    memset(resultNullIndicator, 0, resultNullIndicatorSize);
    memcpy(resultNullIndicator, outerData, outerNullIndicatorSize);
    
    // get inner null indicator to concat onto result null indicator
    char innerNullIndicator[innerNullIndicatorSize];
    memcpy(innerNullIndicator, innerData, innerNullIndicatorSize);
    
    // Look for nulls in inner null indicator, then set null in result null indicator
    int concatNullOffset = outerAttrs.size(); // where to start concat null indicator
    for (unsigned i = 0; i < innerAttrs.size(); i++) {
        if (fieldIsNull(innerNullIndicator, i)) {
            setFieldNull(resultNullIndicator, concatNullOffset + i);
        }
    }
    // set result data's null indicator
    memcpy((char*)data, resultNullIndicator, resultNullIndicatorSize);
    // concat the fields into result data
    int outerDataSize = getLengthOfFields(outerAttrs, outerData);
    int innerDataSize = getLengthOfFields(innerAttrs, innerData);
    memcpy((char*)data + resultNullIndicatorSize,
           (char*)outerData + outerNullIndicatorSize,
           outerDataSize);
    memcpy((char*)data + resultNullIndicatorSize + outerDataSize,
           (char*)innerData + innerNullIndicatorSize,
           innerDataSize);
}

int INLJoin::getLengthOfFields(const vector<Attribute> &attrs, const void *data) {
    int nullIndicatorSize = getNullIndicatorSize(attrs.size());
    int offset = nullIndicatorSize;
    uint32_t size = 0;
    char nullIndicator[nullIndicatorSize];
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // get offset to end of data
    for (unsigned i = 0; i < attrs.size(); i++) {
        if (!fieldIsNull(nullIndicator, i)) {
            if (attrs[i].type == TypeVarChar) {
                memcpy(&size, (char*)data + offset, 4);
                offset += size;
            }
            offset += 4;
        }
    }
    
    // length of fields = offset - size of null indicator
    return offset - nullIndicatorSize;
}
