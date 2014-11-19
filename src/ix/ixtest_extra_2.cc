#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_extra_2(const string &indexFileName, const Attribute &attribute)
{
	// Checks whether deleting an entry after getNextEntry() for EXACT MATCH case is handled properly or not.
	//
	// Extra test case for Undergrads. Mandatory for Grads.
	// Pass: 5 extra credit points for Undergrads.
	//       No score deduction for Grads.
	// Fail: no extra points for Undergrads. Points will be deducted for Grads.
	
    // Functions tested
    // 1. Create Index File
    // 2. OpenIndex File
    // 3. Insert entry
    // 4. Scan entries - EXACT MATCH, and delete entries
    // 5. Scan close
    // 6. CloseIndex File
    // 7. DestroyIndex File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Extra Test Case 2****" << endl;

    RC rc;
    RID rid;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;
    float compVal = 100.0;
    unsigned numOfTuples = 500;
    unsigned numberOfPages = 4;
    float key;

    // create index file
    rc = indexManager->createFile(indexFileName, numberOfPages);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
    	return fail;
    }

    // open index file
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

    // InsertEntry
    key = compVal;

    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        	indexManager->closeFile(ixfileHandle);
        	return fail;
        }
    }

    // scan - EXACT MATCH
    rc = indexManager->scan(ixfileHandle, attribute, &compVal, &compVal, true, true, ix_ScanIterator);
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

    // DeleteEntry in IndexScan Iterator
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
    	if (rid.pageNum % 100 == 0) {
            cout << "returned rid: " << rid.pageNum << " " << rid.slotNum << endl;
    	}

        rc = indexManager->deleteEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
        	ix_ScanIterator.close();
        	return fail;
        }
    }
    cout << endl;

    // close scan
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

    // Open Scan again
    rc = indexManager->scan(ixfileHandle, attribute, &compVal, &compVal, true, true, ix_ScanIterator);
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

    bool testFailed = false;

    //iterate
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << "Entry returned: " << rid.pageNum << " " << rid.slotNum << " --- Test failed." << endl;
        testFailed = true;

        if(rid.pageNum > 500)
        {
            cout << "Wrong entries output... failure" << endl;
        	ix_ScanIterator.close();
        	return fail;
        }
        
    }

    if (testFailed) {
    	cout << "Wrong entries output... failure" << endl;
    	ix_ScanIterator.close();
    	return fail;
    }

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

    //close index file file
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

    //destroy index file file
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

	const string indexFileName = "height_idx";
	Attribute attrHeight;
	attrHeight.length = 4;
	attrHeight.name = "height";
	attrHeight.type = TypeReal;

	RC result = testCase_extra_2(indexFileName, attrHeight);
    if (result == success) {
    	cout << "IX_Test Extra Case 2 passed." << endl;
    	return success;
    } else {
    	cout << "IX_Test Extra Case 2 failed.  Deleting an entry after getNextEntry() for EXACT MATCH is not  properly handled." << endl;
    	return fail;
    }

}

