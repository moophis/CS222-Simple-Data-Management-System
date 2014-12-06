#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe.h"

#ifndef _success_
#define _success_
const int success = 0;
#endif

// Global Initialization
RelationManager *rm = RelationManager::instance();
IndexManager *im = IndexManager::instance();

// Number of tuples in each relation
const int tupleCount = 100;

// Buffer size and character buffer size
const unsigned bufSize = 200;

int createLeftTable() {
	// Functions Tested;
	// 1. Create Table
	cout << "****Create Left Table****" << endl;

	vector<Attribute> attrs;

	Attribute attr;
	attr.name = "A";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "B";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "C";
	attr.type = TypeReal;
	attr.length = 4;
	attrs.push_back(attr);

	RC rc = rm->createTable("left", attrs);
	if (rc == success) {
		cout << "****Left Table Created!****" << endl;
	}
	return rc;
}

// Prepare the tuple to left table in the format conforming to Insert/Update/ReadTuple and readAttribute
void prepareLeftTuple(const int a, const int b, const float c, void *buf) {
	int offset = 0;

	memcpy((char *) buf + offset, &a, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buf + offset, &b, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buf + offset, &c, sizeof(float));
	offset += sizeof(float);
}

int populateLeftTable(vector<RID> &rids) {
	// Functions Tested
	// 1. InsertTuple
	RC rc = success;
	RID rid;
	
	void *buf = malloc(bufSize);
	for (int i = 0; i < tupleCount; ++i) {
		memset(buf, 0, bufSize);

		// Prepare the tuple data for insertion
                // a in [10, 109], b in [0,99], c in [50, 149.0]
		int a = i + 10;
		int b = i;
		float c = (float) (i + 50);
		prepareLeftTuple(a, b, c, buf);

		rc = rm->insertTuple("left", buf, rid);
		rids.push_back(rid);
		if (rc != success) {
			goto clean_up;
		}
	}

clean_up:
	free(buf);
	return rc;
}

int createIndexforLeftB() {
	return rm->createIndex("left", "B");
}

int QE_TEST_1(vector<RID> &rids) {
	cout << "**** In Test Case 1 ****" << endl;

	RC rc = success;
	string tableName = "left";
	void *data = malloc(bufSize);

	// Create the left table
	if (createLeftTable() != success)
	{
		cout << "** TEST_RM_PRIVATE_1 failed :-( **" << endl << endl;
		free(data);
		return -1;		
	}

	// Populate the table
	if (populateLeftTable(rids) != success) 
	{
		cout << "** TEST_RM_PRIVATE_1 failed :-( **" << endl << endl;
		free(data);
		return -1;	
	}

	// Create the left index
	if (createIndexforLeftB() != success)
	 {
		cout << "** TEST_RM_PRIVATE_1 failed :-( **" << endl << endl;
		free(data);
		return -1;	
	}

	// delete tuples
	for(int i = 0; i < tupleCount; i++)
	{
		if (i % 2 == 0)
		{
			rc = rm->deleteTuple(tableName, rids[i]);
			if (rc != success) 
			{
				cout << "** TEST_RM_PRIVATE_1 failed :-( **" << endl << endl;
				free(data);
				return -1;
        		}
		}
        }

	// IndexScan()
	IndexScan *leftIn = new IndexScan(*rm, "left", "B");
	while (leftIn->getNextTuple(data) != QE_EOF)
	{
		int val = *(int *) ((char *) data + 4);
		if (val % 2 == 0)
		{
			cout << "**** Test Case 1 Failed :-( ****" << endl;
			delete leftIn;			
			free(data);
			return -1;
		}		
		cout << "left.B = " << val << endl;
	}

	cout << "**** Test Case 1 passed :-) ****" << endl;
	delete leftIn;
	free(data);
	return rc;
}

int main() {
	vector<RID> rids;
	QE_TEST_1(rids);
}
