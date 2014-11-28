#include "test_util.h"

void printScanResult(const string &tableName) {
    cout << "Entries in Index EmpName" << endl;
    scanIndex(tableName, "EmpName", TypeVarChar);

    cout << "Entries in Index Age" << endl;
    scanIndex(tableName, "Age", TypeInt);

    cout << "Entries in Index Height" << endl;
    scanIndex(tableName, "Height", TypeReal);

    cout << "Entries in Index Salary" << endl;
    scanIndex(tableName, "Salary", TypeInt);
}

void TEST_RM_3(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert Tuple
    // 2. Update Tuple **
    // 3. Read Tuple
    // Additional: scan index after updating an record; Delete indexes
    cout << "****In Test Case 3****" << endl;

    RID rid;
    int tupleSize = 0;
    int updatedTupleSize = 0;
    void *tuple = malloc(100);
    void *updatedTuple = malloc(100);
    void *returnedData = malloc(100);

    // Test Insert Tuple
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    RC rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);
    cout << "Original RID slot = " << rid.slotNum << endl;

    // Index after insertion
    cout << "Index after insertion: " << endl;
    printScanResult(tableName);

    // Test Update Tuple
    prepareTuple(6, "Newman", age, height, 100, updatedTuple, &updatedTupleSize);
    rc = rm->updateTuple(tableName, updatedTuple, rid);
    assert(rc == success);
    cout << "Updated RID slot = " << rid.slotNum << endl;

    // Index after update
    cout << "Index after update: " << endl;
    printScanResult(tableName);

    // Test Read Tuple
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc == success);
    cout << "Read RID slot = " << rid.slotNum << endl;

    // Print the tuples
    cout << "Insert Data:" << endl;
    printTuple(tuple, tupleSize);

    cout << "Updated data:" << endl;
    printTuple(updatedTuple, updatedTupleSize);

    cout << "Returned Data:" << endl;
    printTuple(returnedData, updatedTupleSize);

    // Destroy indexes
    cout << "Now destroy all indexes..." << endl;
    rc = rm->destroyIndex(tableName, "EmpName");
    assert(rc == success);
    rc = rm->destroyIndex(tableName, "Age");
    assert(rc == success);
    rc = rm->destroyIndex(tableName, "Height");
    assert(rc == success);
    rc = rm->destroyIndex(tableName, "Salary");
    assert(rc == success);

    if (memcmp(updatedTuple, returnedData, updatedTupleSize) == 0)
    {
        cout << "****Test case 3 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 3 failed****" << endl << endl;
    }

    free(tuple);
    free(updatedTuple);
    free(returnedData);
    return;
}

int main()
{
    cout << endl << "Test Update Tuple .." << endl;

    // Update Tuple
    TEST_RM_3("tbl_employee", 6, "Thomas", 28, 187.3, 4000);

    return 0;
}
