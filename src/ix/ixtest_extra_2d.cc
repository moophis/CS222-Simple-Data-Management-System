#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <algorithm>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_extra_2d(const string &indexFileName, const Attribute &attribute)
{
	// Checks whether deleting an entry after getNextEntry() for RANGE SCAN case is handled properly or not.
	//
	// Extra test case for Undergrads and Grads.
	// Pass: 5 extra credit points for Undergrads if their code passes all of 2a - 2d.
	//       5 extra credit points for Grads if their code passes all of 2a - 2d.
	// Fail: no extra points for Undergrads and Grads.
	//
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex
    // 3. Insert entry
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. Insert entries again
    // 7. Scan entries
    // 8. CloseIndex
    // 9. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Extra Test Case 2d****" << endl;

    RC rc;
    RID rid;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;
    float compVal;
    int numOfTuples;
    unsigned numberOfPages = 4;
    float A[40000];
    float B[30000];
    float key;
    int count;


    //create index file(s)
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
    numOfTuples = 40000;
    for(int i = 0; i < numOfTuples; i++)
    {
        A[i] = (float)i;
    }
    random_shuffle(A, A+numOfTuples);

    for(int i = 0; i < numOfTuples; i++)
    {
        key = A[i];
        rid.pageNum = i+1;
        rid.slotNum = i+1;

        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        	indexManager->closeFile(ixfileHandle);
        	return fail;
        }
    }

    //scan
    compVal = 30000.0;
    rc = indexManager->scan(ixfileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
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

    // Test DeleteEntry in IndexScan Iterator
    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if(count % 1000 == 0)
            cout << "returned rid: " << rid.pageNum << " " << rid.slotNum << endl;

        key = A[rid.pageNum-1];
        rc = indexManager->deleteEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
        	ix_ScanIterator.close();
        	return fail;
        }
        count++;
    }
    cout << "Number of deleted entries: " << count << endl;
    if (count != 30001)
    {
        cout << "Wrong entries output...failure" << endl;
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

    // insert entry Again
    numOfTuples = 30000;
    for(int i = 0; i < numOfTuples; i++)
    {
        B[i] = (float)(40000+i);
    }
    random_shuffle(B, B+numOfTuples);

    for(int i = 0; i < numOfTuples; i++)
    {
        float key = B[i];
        rid.pageNum = i+40001;
        rid.slotNum = i+40001;

        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        	indexManager->closeFile(ixfileHandle);
        	return fail;
        }
    }

    //scan
    compVal = 45000.0;
    rc = indexManager->scan(ixfileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
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

    cout << endl;
    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if (count % 1000 == 0)
            cout << "returned rid: " << rid.pageNum << " " << rid.slotNum << endl;

        if(rid.pageNum > 40000 && B[rid.pageNum-40001] > 45000)
        {
            cout << "Wrong entries output...failure" << endl;
        	ix_ScanIterator.close();
        	return fail;
        }
        count ++;
    }
    cout << "Number of scanned entries: " << count << endl;

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

    //close index file(s)
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

    //destroy index file(s)
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

	RC result = testCase_extra_2d(indexFileName, attrHeight);
    if (result == success) {
    	cout << "IX_Test Extra Case 2d passed." << endl;
    	return success;
    } else {
    	cout << "IX_Test Extra Case 2d failed. Deleting an entry after getNextEntry() for RANGE SCAN is not properly handled." << endl;
    	return fail;
    }

}

