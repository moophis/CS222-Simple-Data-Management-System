#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_extra_1(const string &indexFileName, const Attribute &attribute)
{
	// Checks whether VARCHAR type is handled properly or not.
    //
	// Extra test case for Undergrad. Mandatory for Grad.
	// Pass: 5 extra points for Undergrad. No score deduction for Grad.
	// Fail: no extra points for Undergrad. Points will be deducted for Grad.
	//
    // Functions Tested:
    // 1. Create Index
    // 2. Open Index
    // 3. Insert Entry
    // 4. Scan
    // 5. Close Scan
    // 6. Close Index
    // 7. Destroy Index
    cout << endl << "****In Extra Test Case 1****" << endl;

    RC rc;
    RID rid;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned offset;
    unsigned numOfTuples;
    unsigned numberOfPages = 4;
    char key[100];
    unsigned count;

    //create index file
    rc = indexManager->createFile(indexFileName, numberOfPages);
    if(rc == success)
    {
        cout << "Index Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
    	return fail;
    }

    //open index file
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
    	indexManager->destroyFile(indexFileName);
    	return fail;
    }

    // insert entry
    numOfTuples = 5000;
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
    	count = ((i-1) % 26) + 1;
    	*(int *)key = count;
    	for(unsigned j = 0; j < count; j++)
    	{
    		*(key+4+j) = 96+count;
    	}

        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(ixfileHandle, attribute, key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        	indexManager->closeFile(ixfileHandle);
        	return fail;
        }
    }

    //scan
    offset = 20;
    *(int *)key = offset;
    for(unsigned j = 0; j < offset; j++)
    {
        *(key+4+j) = 96+offset;
    }

    rc = indexManager->scan(ixfileHandle, attribute, key, key, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
    	indexManager->closeFile(ixfileHandle);
    	return fail;
    }

    //iterate
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << "returned rid: " << rid.pageNum << " " << rid.slotNum << endl;
    }
    cout << endl;

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
    	indexManager->closeFile(ixfileHandle);
    	return fail;
    }

    //close index file
    rc = indexManager->closeFile(ixfileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
    	indexManager->destroyFile(indexFileName);
    	return fail;
    }

    //destroy index file
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
    	return fail;
    }

    return success;

}


int main()
{
    //Global Initializations
    indexManager = IndexManager::instance();

	const string indexEmpNameFileName = "EmpName_idx";

	Attribute attrEmpName;
	attrEmpName.length = 100;
	attrEmpName.name = "EmpName";
	attrEmpName.type = TypeVarChar;

	RC result = testCase_extra_1(indexEmpNameFileName, attrEmpName);
    if (result == success) {
    	cout << "IX_Test Extra Case 1 passed." << endl;
    	return success;
    } else {
    	cout << "IX_Test Extra Case 1 failed. VarChar type is not handled properly." << endl;
    	return fail;
    }

}

