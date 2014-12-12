
#include "qe.h"

/**
 * Utitliy functions
 */
static void printValue(void *data, unsigned size) {
    char val[size+1];
    memcpy(val, (char *)data, size);
    val[size] = 0;
    for (unsigned i = 0; i < size; i++) {
        cout << (int) val[i] << " ";
    }
    cout << endl;
}

// Read an attribute value given attribute name as well as the descriptor
// Return: value data and value data length
static RC readValue(const void *data, const void *value, const string &attrName,
        const vector<Attribute> &attrs, unsigned &valueLength) {
    if (data == nullptr) {
        return ERR_NO_INPUT;
    }

    unsigned offset = 0;
    for (auto it = attrs.begin(); it != attrs.end(); ++it) {
        unsigned size = 0;
        switch (it->type) {
        case TypeInt:
            size = sizeof(int);
            break;
        case TypeReal:
            size = sizeof(float);
            break;
        case TypeVarChar:
            memcpy((char *)&size, (char *)data + offset, sizeof(int));
            size += sizeof(int);
            break;
        default:
            __trace();
            break;
        }
        if (attrName.compare(it->name) == 0) {
            valueLength = size;
            memcpy((char *)value, (char *)data + offset, size);
            return SUCCESSFUL;
        }
        offset += size;
    }

    valueLength = 0;
    return ERR_NO_ATTR;
}

// Append a new value to the existing data chain (assuming the buffer size is enough)
static void appendValue(void *now, unsigned nowSize, void *newval, unsigned valSize) {
    memcpy((char *)now + nowSize, newval, valSize);
}

// Find whether two key values meet the given criterion
static bool isMatch(const void *leftVal, const void *rightVal, AttrType type, CompOp &op) {
    KeyValue lhs(leftVal, type);
    KeyValue rhs(rightVal, type);

    int cmp = lhs.compare(rhs);

    switch (op) {
    case EQ_OP: return cmp == 0;
    case LT_OP: return cmp < 0;
    case GT_OP: return cmp > 0;
    case LE_OP: return cmp <= 0;
    case GE_OP: return cmp >= 0;
    case NE_OP: return cmp != 0;
    case NO_OP:
        // Fall through
    default:
        break;
    }

    // Shouldn't reach here
    __trace();
    return false;
}

// Check if two raw values are equal
static bool isEqual(const void *v1, unsigned &size1, const void *v2, unsigned &size2) {
    if (size1 != size2) {
        return false;
    }
    return memcmp(v1, v2, size1) == 0;
}

/**
 * Filter
 */
Filter::Filter(Iterator* input, const Condition &condition)
    : _iterator(input), _condition(condition) {
    // right hand side of the condition must be a literal value
    assert(!condition.bRhsIsAttr);
}

RC Filter::getNextTuple(void *data) {
    if (_iterator == nullptr) {
        return QE_EOF;
    }

    vector<Attribute> attrs;
    getAttributes(attrs);
    while (_iterator->getNextTuple(data) != QE_EOF) {
        char val[PAGE_SIZE];
        unsigned valsize = 0;
        if (readValue(data, val, _condition.lhsAttr, attrs, valsize) != SUCCESSFUL) {
            __trace();
            cout << "Condition attribute name: " << _condition.lhsAttr << endl;
            return QE_EOF;
        }
        if (isMatch(val, _condition.rhsValue.data, _condition.rhsValue.type, _condition.op)) {
            return SUCCESSFUL;
        }
    }

    return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    if (_iterator != nullptr) {
        _iterator->getAttributes(attrs);
    }
}

/**
 * Project
 */
Project::Project(Iterator *input, const vector<string> &attrNames)
        : _iterator(input), _attrNames(attrNames) {
    if (input == nullptr) {
        __trace();
        return;
    }
}

RC Project::getNextTuple(void *data) {
//    __trace();
    vector<Attribute> origin;
    _iterator->getAttributes(origin);
    char buf[PAGE_SIZE];
    if (_iterator->getNextTuple(buf) != QE_EOF) {
        unsigned offset = 0;
        for (auto it = _attrNames.begin(); it != _attrNames.end(); ++it) {
            char value[PAGE_SIZE];
            unsigned len;
            if (readValue(buf, value, *it, origin, len) != SUCCESSFUL) {
                __trace();
                return QE_EOF;
            }
            appendValue(data, offset, value, len);
            offset += len;
        }
//        __trace();
        return SUCCESSFUL;
    }
//    __trace();
    return QE_EOF;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
//    __trace();
    attrs.clear();
    vector<Attribute> desc;
    _iterator->getAttributes(desc);
    for (auto it = _attrNames.begin(); it != _attrNames.end(); ++it) {
        for (auto jt = desc.begin(); jt != desc.end(); ++jt) {
            if (it->compare(jt->name) == 0) {
//                cout << "Attribute: " << jt->name << endl;
                attrs.push_back(*jt);
            }
        }
    }
//    __trace();
}

/**
 * Grace hash join
 */
// PartitionBuilder
PartitionBuilder::PartitionBuilder(const string &partitionName,
        const vector<Attribute> &attrs)
    : _fileName(partitionName), _attrs(attrs),
    _rbfm(RecordBasedFileManager::instance()),
    _sm(SpaceManager::instance()),
    _pfm(PagedFileManager::instance()) {

    RC err = init();
    assert(err == SUCCESSFUL);
}

PartitionBuilder::~PartitionBuilder() {
//    __trace();
//    cout << "Destroying partition: " << _fileName << endl;
//    RC err = _rbfm->destroyFile(_fileName);
//    assert(err == SUCCESSFUL);
}

RC PartitionBuilder::init() {
    RC err;
    if ((err = _rbfm->createFile(_fileName)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    if ((err = _rbfm->openFile(_fileName, _fileHandle)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    _sm->initCleanPage(_buffer);
    return SUCCESSFUL;
}

RC PartitionBuilder::insertTuple(void *tuple) {
    RC err;

    unsigned tupleSize;
    if ((err = _rbfm->countRecordSize(_attrs, tuple, tupleSize)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    if (_sm->getPageFreeSize(_buffer) < tupleSize) {
        if ((err = _fileHandle.appendPage(_buffer)) != SUCCESSFUL) {
            __trace();
            return err;
        }
        _sm->initCleanPage(_buffer);
    }
    unsigned freePtr = _sm->getFreePtr(_buffer);
    unsigned slotCount = _sm->getSlotCount(_buffer);
    _sm->writeRecord(_buffer, tuple, freePtr, tupleSize);
    _sm->setSlot(_buffer, slotCount, freePtr, tupleSize);
    _sm->setSlotCount(_buffer, slotCount + 1);
    _sm->setFreePtr(_buffer, freePtr + tupleSize);

    return SUCCESSFUL;
}

RC PartitionBuilder::flushLastPage() {
    RC err;
    if ((err = _fileHandle.appendPage(_buffer)) != SUCCESSFUL) {
        __trace();
        return err;
    }
    return SUCCESSFUL;
}

void PartitionBuilder::getAttributes(vector<Attribute> &attrs) {
    attrs = _attrs;
}

string PartitionBuilder::getPartitionName() {
    return _fileName;
}

// PartitionReader
PartitionReader::PartitionReader(const string &partitionName,
        const vector<Attribute> &attrs)
      : _fileName(partitionName), _attrs(attrs),
        _curPageNum(0), _curSlotNum(0),
        _rbfm(RecordBasedFileManager::instance()),
        _sm(SpaceManager::instance()),
        _pfm(PagedFileManager::instance()) {

    RC err = init();
    assert(err == SUCCESSFUL);
}

PartitionReader::~PartitionReader() {
    // Deallocate buffer
    for (unsigned i = 0; i < _pageCount; i++) {
        delete[] _buffer[i];
    }
    delete[] _buffer;

    // Delete partitions created by associated PartitionBuilder
    RC err = _rbfm->destroyFile(_fileName);
    assert(err == SUCCESSFUL);
}

RC PartitionReader::init() {
    RC err;

    // Open partition file
    if ((err = _rbfm->openFile(_fileName, _fileHandle)) != SUCCESSFUL) {
        __trace();
        return err;
    }

    _pageCount = _fileHandle.getNumberOfPages();

    // Allocate buffer
    _buffer = new char*[_pageCount];
    for (unsigned i = 0; i < _pageCount; i++) {
        _buffer[i] = new char[PAGE_SIZE];
    }

    return SUCCESSFUL;
}

RC PartitionReader::getNextTuple(void *tuple, RID &rid, unsigned &size) {
    while (_curPageNum < _pageCount) {
        if (_curSlotNum == 0) {
            if (_fileHandle.readPage(_curPageNum, _buffer[_curPageNum]) != SUCCESSFUL) {
                __trace();
                return QE_EOF;
            }
        }
        unsigned slotCount = _sm->getSlotCount(_buffer[_curPageNum]);
        if (_curSlotNum >= slotCount) {
            _curSlotNum = 0;
            _curPageNum++;
        } else {
            unsigned start = _sm->getSlotStartPos(_buffer[_curPageNum], _curSlotNum);
            unsigned len = _sm->getSlotLength(_buffer[_curPageNum], _curSlotNum);
            _sm->readRecord(_buffer[_curPageNum], tuple, start, len);
            rid.pageNum = _curPageNum;
            rid.slotNum = _curSlotNum;
            size = len;
            _curSlotNum++;
            return SUCCESSFUL;
        }
    }
    return QE_EOF;
}

RC PartitionReader::getTupleFromCache(void *tuple, unsigned &size, const RID &rid) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;

    if (pageNum >= _pageCount) {
        __trace();
        return ERR_OUT_OF_BOUND;
    }
    unsigned slotCount = _sm->getSlotCount(_buffer[pageNum]);
    if (slotNum >= slotCount) {
        __trace();
        return ERR_OUT_OF_BOUND;
    }

    unsigned start = _sm->getSlotStartPos(_buffer[pageNum], slotNum);
    unsigned len = _sm->getSlotLength(_buffer[pageNum], slotNum);
    _sm->readRecord(_buffer[pageNum], tuple, start, len);
    size = len;
    return SUCCESSFUL;
}

void PartitionReader::getAttributes(vector<Attribute> &attrs) {
    attrs = _attrs;
}

unsigned PartitionReader::getPageCount() {
    return _pageCount;
}

string PartitionReader::getPartitionName() {
    return _fileName;
}

// GHJoin
GHJoin::GHJoin(Iterator *leftIn,
        Iterator *rightIn,
        const Condition &condition,
        const unsigned numPartitions)
    : _joinNumber(_joinNumberGlobal++) ,_leftIn(leftIn), _rightIn(rightIn),
      _condition(condition), _numPartitions(numPartitions),
      _leftReader(nullptr), _rightReader(nullptr), _curPartition(0) {
    assert(leftIn != nullptr);
    assert(rightIn != nullptr);
    _leftIn->getAttributes(_leftAttrs);
    _rightIn->getAttributes(_rightAttrs);
    assert(_condition.op == EQ_OP);
    assert(_condition.bRhsIsAttr);

    _curLeftMapIndex = 0;

    // Partition first
    allocatePartition(leftIn, LEFT);
    allocatePartition(rightIn, RIGHT);
    assert(partition(leftIn, LEFT) == SUCCESSFUL);
    assert(partition(rightIn, RIGHT) == SUCCESSFUL);

}

GHJoin::~GHJoin() {
    __trace();
    deallocatePartition(LEFT);
    deallocatePartition(RIGHT);
}

// Buffers used in GHJoin
char GHJoin::_rtuple[PAGE_SIZE];
unsigned GHJoin::_rsize = 0;

// Now assume that left partitions are always used for building hash map.
RC GHJoin::getNextTuple(void *data) {
    // Process cached value first (if possible)
    if (_leftReader != nullptr && _rightReader != nullptr
            && matchTuples(data) == SUCCESSFUL) {
        return SUCCESSFUL;
    }
    while (_curPartition < _numPartitions) {
        // Check if we need to load a new partition
        if (_leftReader == nullptr && _rightReader == nullptr) {
            // Time to scan the next partition
            _leftReader = new PartitionReader(getPartitionName(LEFT, _curPartition), _leftAttrs);
            _rightReader = new PartitionReader(getPartitionName(RIGHT, _curPartition), _rightAttrs);
            // Build in-memory hash map for left partition
            RID rid;
            char tuple[PAGE_SIZE];
            unsigned tsize;
            while (_leftReader->getNextTuple(tuple, rid, tsize) != QE_EOF) {
                char val[PAGE_SIZE];
                unsigned valsize = 0;
                if (readValue(tuple, val, _condition.lhsAttr, _leftAttrs, valsize) != SUCCESSFUL) {
                    __trace();
                    return QE_EOF;
                }
                unsigned p = hash2(val, valsize);
                _hashMap[p].push_back(rid);
            }
        }

        // Rehash tuples from right partition and find a match
        RID rrid;
        while (_rightReader->getNextTuple(_rtuple, rrid, _rsize) != QE_EOF) {
            // Get right value
            _curLeftMapIndex = 0;  // Reset the map index
            if (matchTuples(data) == SUCCESSFUL) {
                return SUCCESSFUL;
            }
        }

        // Now all tuples in the current partition have been gone through.
        // Clear the current partition and buffer and prepare to load the next
        // partition (if exists).
        if (_leftReader != nullptr) {
            delete _leftReader;
            _leftReader = nullptr;
        }
        if (_rightReader != nullptr) {
            delete _rightReader;
            _rightReader = nullptr;
        }
        _hashMap.clear();
        _curPartition++;
    }

    return QE_EOF;
}

RC GHJoin::matchTuples(void *data) {
    char rval[PAGE_SIZE];
    unsigned rvalsize = 0;
    if (readValue(_rtuple, rval, _condition.rhsAttr, _rightAttrs, rvalsize) != SUCCESSFUL) {
        __trace();
        return QE_EOF;
    }

    unsigned p = hash2(rval, rvalsize);
    vector<RID> &leftRIDs = _hashMap[p];
    // Get left value and compare
    for (; _curLeftMapIndex < leftRIDs.size(); ++_curLeftMapIndex) {
        char ltuple[PAGE_SIZE];
        unsigned lsize = 0;
        if (_leftReader->getTupleFromCache(ltuple, lsize, leftRIDs[_curLeftMapIndex]) != SUCCESSFUL) {
            __trace();
            _curLeftMapIndex = leftRIDs.size();
            return QE_EOF;
        }
        char lval[PAGE_SIZE];
        unsigned lvalsize = 0;
        if (readValue(ltuple, lval, _condition.lhsAttr, _leftAttrs, lvalsize) != SUCCESSFUL) {
            __trace();
            _curLeftMapIndex = leftRIDs.size();
            return QE_EOF;
        }
        if (isEqual(lval, lvalsize, rval, rvalsize)) {
            // Find a match, join two tuples
            appendValue(data, 0, ltuple, lsize);
            appendValue(data, lsize, _rtuple, _rsize);
            ++_curLeftMapIndex;
            return SUCCESSFUL;
        }
    }
    return QE_EOF;
}

void GHJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    // [Left.attr1, Left.attr2, ..., Right.attr1, ...]
    attrs.insert(attrs.begin(), _rightAttrs.begin(), _rightAttrs.end());
    attrs.insert(attrs.begin(), _leftAttrs.begin(), _leftAttrs.end());
}

RC GHJoin::partition(Iterator *iter, IterType iterType) {
    RC err;

    string attrName = (iterType == LEFT) ? _condition.lhsAttr : _condition.rhsAttr;
    vector<PartitionBuilder *> &partitions = (iterType == LEFT) ? _leftPartitions : _rightPartitions;
    vector<Attribute> attrs;
    iter->getAttributes(attrs);

    char tuple[PAGE_SIZE];
    while (iter->getNextTuple(tuple) != QE_EOF) {
        // Read the join value
        char val[PAGE_SIZE];
        unsigned valsize = 0;
        if (readValue(tuple, val, attrName, attrs, valsize) != SUCCESSFUL) {
            __trace();
            cout << "Condition attribute name: " << attrName << endl;
            return QE_EOF;
        }
        // Hash and find the right partition
        unsigned p = hash1(val, valsize);
        if ((err = partitions[p]->insertTuple(tuple)) != SUCCESSFUL) {
            __trace();
            return err;
        }
    }

    // Flush all remaining pages
    for (unsigned i = 0; i < _numPartitions; i++) {
        if ((err = partitions[i]->flushLastPage()) != SUCCESSFUL) {
            __trace();
            return err;
        }
    }

    return SUCCESSFUL;
}

void GHJoin::allocatePartition(Iterator *iter, IterType iterType) {
    vector<PartitionBuilder *> &partitions = (iterType == LEFT) ? _leftPartitions : _rightPartitions;
    vector<Attribute> attrs;
    iter->getAttributes(attrs);
    for (unsigned i = 0; i < _numPartitions; i++) {
        string name = getPartitionName(iterType, i);
        partitions.push_back(new PartitionBuilder(name, attrs));
    }
}

string GHJoin::getPartitionName(IterType iterType, unsigned num) {
    string prefix = (iterType == LEFT) ? "left_" : "right_";
    stringstream ss;
    ss << prefix << _joinNumber << "_" << num;
    return ss.str();
}

void GHJoin::deallocatePartition(IterType iterType) {
    vector<PartitionBuilder *> &partitions = (iterType == LEFT) ? _leftPartitions : _rightPartitions;
    for (auto it = partitions.begin(); it != partitions.end(); ++it) {
        delete *it;
    }
}

unsigned GHJoin::hash1(char *value, unsigned size) {
    int sum = 0;
    std::hash<int> intHash;
    for (unsigned i = 0; i < size; i++) {
        sum += static_cast<int>(value[i]);
    }
    return intHash(sum) % _numPartitions;
}

unsigned GHJoin::hash2(char *value, unsigned size) {
    int sum = 0;
    bool sign = false;
    std::hash<int> intHash;
    for (unsigned i = 0; i < size; i++) {
        if (!sign) {
            sum += static_cast<int>(value[i]);
        } else {
            sum -= static_cast<int>(value[i]);
        }
        sign = !sign;
    }
    return intHash(sum) % _numPartitions;
}

int GHJoin::_joinNumberGlobal = 0;


/**
 * Index nested loop join.
 */
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
    : _leftIn(leftIn), _rightIn(rightIn), _condition(condition),
     _rbfm(RecordBasedFileManager::instance()) {
    assert(_leftIn != nullptr);
    assert(_rightIn != nullptr);
    _leftIn->getAttributes(_leftAttrs);
    _rightIn->getAttributes(_rightAttrs);
    assert(_condition.op == EQ_OP);
    assert(_condition.bRhsIsAttr);
}

char INLJoin::_ltuple[PAGE_SIZE];
unsigned INLJoin::_lsize = 0;

RC INLJoin::getNextTuple(void *data) {
    static bool initialized = false;
    // Deal with remaining of inner relation index scan
    if (initialized && matchTuples(data) == SUCCESSFUL) {
        return SUCCESSFUL;
    }
    initialized = true;
    while (_leftIn->getNextTuple(_ltuple) != QE_EOF) {
        char lval[PAGE_SIZE];
        unsigned lvalsize = 0;
        if (readValue(_ltuple, lval, _condition.lhsAttr, _leftAttrs, lvalsize) != SUCCESSFUL) {
            __trace();
            return QE_EOF;
        }

        // Set up right index scan iterator
        _rightIn->setIterator(lval, lval, true, true);
        if (matchTuples(data) == SUCCESSFUL) {
            return SUCCESSFUL;
        }
    }
    return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    // [Left.attr1, Left.attr2, ..., Right.attr1, ...]
    attrs.insert(attrs.begin(), _rightAttrs.begin(), _rightAttrs.end());
    attrs.insert(attrs.begin(), _leftAttrs.begin(), _leftAttrs.end());
}

RC INLJoin::matchTuples(void *data) {
    if (_rbfm->countRecordSize(_leftAttrs, _ltuple, _lsize) != SUCCESSFUL) {
        __trace();
        return QE_EOF;
    }

    char lval[PAGE_SIZE];
    unsigned lvalsize = 0;
    if (readValue(_ltuple, lval, _condition.lhsAttr, _leftAttrs, lvalsize) != SUCCESSFUL) {
        __trace();
        return QE_EOF;
    }

    char rtuple[PAGE_SIZE];
    if (_rightIn->getNextTuple(rtuple) != QE_EOF) {
        unsigned rsize;
        if (_rbfm->countRecordSize(_rightAttrs, rtuple, rsize) != SUCCESSFUL) {
            __trace();
            return QE_EOF;
        }

        char rval[PAGE_SIZE];
        unsigned rvalsize = 0;
        if (readValue(rtuple, rval, _condition.rhsAttr, _rightAttrs, rvalsize) != SUCCESSFUL) {
            __trace();
            return QE_EOF;
        }

        // Compare left and right value
        if (isEqual(lval, lvalsize, rval, rvalsize)) {
            // Find a match, join two tuples
            appendValue(data, 0, _ltuple, _lsize);
            appendValue(data, _lsize, rtuple, rsize);
            return SUCCESSFUL;
        }
    }
    return QE_EOF;
}

/**
 * Aggregate
 */
static const string aggOpNames[] = { "MIN", "MAX", "SUM", "AVG", "COUNT" };

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                     Attribute aggAttr,        // The attribute over which we are computing an aggregate
                     AggregateOp op            // Aggregate operation
) : _iterator(input), _aggAttr(aggAttr), _op(op), _gotResult(false),
    _count(0), _intSum(0), _floatSum(0.0), _intMin(INT_MAX),
    _floatMin(FLT_MAX), _intMax(INT_MIN), _floatMax(FLT_MIN) {
    assert(process() == SUCCESSFUL);
//    __trace();
//    cout << "Count: " << _count << ", int sum: " << _intSum << ", float sum: " << _floatSum
//         << ", int max: " << _intMax << ", float max: " << _floatMax
//         << ", int min: " << _intMin << ", float min: " << _floatMin << endl;
}

RC Aggregate::getNextTuple(void *data) {
//    __trace();

    if (!_gotResult) {
        switch (_op) {
        case MIN:
            if (_aggAttr.type == TypeInt) {
                memcpy((char *)data, (char *) &_intMin, sizeof(int));
            } else {
                memcpy((char *)data, (char *) &_floatMin, sizeof(float));
            }
            break;
        case MAX:
            if (_aggAttr.type == TypeInt) {
                memcpy((char *)data, (char *) &_intMax, sizeof(int));
            } else {
                memcpy((char *)data, (char *) &_floatMax, sizeof(float));
            }
            break;
        case SUM:
            if (_aggAttr.type == TypeInt) {
                memcpy((char *)data, (char *) &_intSum, sizeof(int));
            } else {
                memcpy((char *)data, (char *) &_floatSum, sizeof(float));
            }
            break;
        case AVG:
            if (_aggAttr.type == TypeInt) {
                int avg = _intSum / _count;
                memcpy((char *)data, (char *) &avg, sizeof(int));
            } else {
                float avg = static_cast<float>(_floatSum / _count);
                memcpy((char *)data, (char *) &avg, sizeof(float));
            }
            break;
        case COUNT:
            memcpy((char *)data, (char *) &_count, sizeof(int));
            break;
        default:
//            __trace();
            return QE_EOF;
        }
        _gotResult = true;
//        __trace();
        return SUCCESSFUL;
    }

//    __trace();
    return QE_EOF;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
//    __trace();
    stringstream ss;
    ss << aggOpNames[_op] << "(" << _aggAttr.name << ")";
//    cout << ss.str() << endl;
    Attribute attr;
    attr.name = ss.str();
    attr.type = (_op == COUNT) ? TypeInt : _aggAttr.type;
    attr.length = _aggAttr.length;
    attrs.clear();
    attrs.push_back(attr);
}

RC Aggregate::process() {
    RC err;

    vector<Attribute> attrs;
    _iterator->getAttributes(attrs);

    char data[PAGE_SIZE];
    while (_iterator->getNextTuple(data) != QE_EOF) {
        _count++;
        if (_op == COUNT) {
            continue;
        }
        char val[PAGE_SIZE];
        unsigned valsize = 0;
        if ((err = readValue(data, val, _aggAttr.name, attrs, valsize)) != SUCCESSFUL) {
            __trace();
            return err;
        }

        if (_aggAttr.type == TypeInt) {
            int d = *((int *) val);
//            cout << "read int: " << d << endl;
            _intSum += d;
            _intMin = std::min(_intMin, d);
            _intMax = std::max(_intMax, d);
        } else if (_aggAttr.type == TypeReal) {
            float d = *((float *) val);
//            cout << "read real: " << d << endl;
            _floatSum += d;
            _floatMin = std::min(_floatMin, d);
            _floatMax = std::max(_floatMax, d);
        } else {
            __trace();
            return ERR_INV_TYPE;
        }
    }

    return SUCCESSFUL;
}



