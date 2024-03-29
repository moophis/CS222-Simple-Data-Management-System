#ifndef _test_util_h_
#define _test_util_h_

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <fstream>
#include <iostream>
#include <cassert>
#include <sys/time.h>
#include <sys/resource.h>
#include <set>
#include "rm.h"

using namespace std;

RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

RelationManager *rm = RelationManager::instance();
const int success = 0;

void memProfile()
{
    int who = RUSAGE_SELF;
    struct rusage usage;
    getrusage(who,&usage);
    cout<<usage.ru_maxrss<<"KB"<<endl;
}

// Function to prepare the data in the correct form to be inserted/read/updated
void prepareTuple(const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *tupleSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    *tupleSize = offset;
}


// Function to parse the data in buffer and print each field
void printTuple(const void *buffer, const int tupleSize)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;

    int nameLength = 0;
    memcpy(&nameLength, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "nameLength: " << nameLength << endl;

    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    cout << "name: " << name << endl;

    int age = 0;
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;

    float height = 0.0;
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;

    int salary = 0;
    memcpy(&salary, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "salary: " << salary << endl;

    cout << "****Printing Buffer: End****" << endl << endl;
}


// Function to get the data in the correct form to be inserted/read after adding
// the attribute ssn
void prepareTupleAfterAdd(const int nameLength, const string &name, const int age, const float height, const int salary, const int ssn, void *buffer, int *tupleSize)
{
    int offset=0;

    memcpy((char*)buffer + offset, &(nameLength), sizeof(int));
    offset += sizeof(int);
    memcpy((char*)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char*)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char*)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char*)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    memcpy((char*)buffer + offset, &ssn, sizeof(int));
    offset += sizeof(int);

    *tupleSize = offset;
}


void printTupleAfterDrop( const void *buffer, const int tupleSize)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;

    int nameLength = 0;
    memcpy(&nameLength, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "nameLength: " << nameLength << endl;

    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    cout << "name: " << name << endl;

    int age = 0;
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;

    float height = 0.0;
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;

    cout << "****Printing Buffer: End****" << endl << endl;
}


void printTupleAfterAdd(const void *buffer, const int tupleSize)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;

    int nameLength = 0;
    memcpy(&nameLength, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "nameLength: " << nameLength << endl;

    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    cout << "name: " << name << endl;

    int age = 0;
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;

    float height = 0;
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;

	int salary = 0;
    memcpy(&salary, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "salary: " << salary << endl;

    int ssn = 0;
    memcpy(&ssn, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "SSN: " << ssn << endl;

    cout << "****Printing Buffer: End****" << endl << endl;
}


// Create an employee table
void createTable(const string &tableName)
{
    cout << "****Create Table " << tableName << " ****" << endl;

    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    attrs.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    RC rc = rm->createTable(tableName, attrs);
    assert(rc == success);
    cout << "****Table Created: " << tableName << " ****" << endl << endl;
}

void prepareLargeTuple(const int index, void *buffer, int *size)
{
    int offset = 0;

    // compute the count
    int count = index % 50 + 1;

    // compute the letter
    char text = index % 26 + 97;

    for(int i = 0; i < 10; i++)
    {
        memcpy((char *)buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        for(int j = 0; j < count; j++)
        {
            memcpy((char *)buffer + offset, &text, 1);
            offset += 1;
        }

        // compute the integer
        memcpy((char *)buffer + offset, &index, sizeof(int));
        offset += sizeof(int);

        // compute the floating number
        float real = (float)(index + 1);
        memcpy((char *)buffer + offset, &real, sizeof(float));
        offset += sizeof(float);
    }
    *size = offset;
}


// Create a large table for pressure test
void createLargeTable(const string &tableName)
{
    cout << "****Create Large Table " << tableName << " ****" << endl;

    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    int index = 0;
    char *suffix = (char *)malloc(10);
    for(int i = 0; i < 10; i++)
    {
        Attribute attr;
        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeVarChar;
        attr.length = (AttrLength)50;
        attrs.push_back(attr);
        index++;

        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        attrs.push_back(attr);
        index++;

        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeReal;
        attr.length = (AttrLength)4;
        attrs.push_back(attr);
        index++;
    }

    RC rc = rm->createTable(tableName, attrs);
    assert(rc == success);
    cout << "****Large Table Created: " << tableName << " ****" << endl << endl;

    free(suffix);
}

// Write RIDs to a disk - do not use this code.
//This is not a page-based operation. For test purpose only.
void writeRIDsToDisk(vector<RID> &rids)
{
	remove("rids_file");
	ofstream ridsFile("rids_file", ios::out | ios::trunc | ios::binary);

	if (ridsFile.is_open()) {
		ridsFile.seekp(0, ios::beg);
		for (int i = 0; i < rids.size(); i++) {
			ridsFile.write(reinterpret_cast<const char*>(&rids[i].pageNum),
					sizeof(unsigned));
			ridsFile.write(reinterpret_cast<const char*>(&rids[i].slotNum),
					sizeof(unsigned));
		}
		ridsFile.close();
	}
}

// Write sizes to a disk - do not use this code.
//This is not a page-based operation. For test purpose only.
void writeSizesToDisk(vector<int> &sizes)
{
	remove("sizes_file");
	ofstream sizesFile("sizes_file", ios::out | ios::trunc | ios::binary);

	if (sizesFile.is_open()) {
		sizesFile.seekp(0, ios::beg);
		for (int i = 0; i < sizes.size(); i++) {
			//cout << sizes[i] << endl;
			sizesFile.write(reinterpret_cast<const char*>(&sizes[i]),
					sizeof(int));
		}
		sizesFile.close();
	}
}

// Read rids from the disk - do not use this code.
//This is not a page-based operation. For test purpose only.
void readRIDsFromDisk(vector<RID> &rids, int numRecords)
{
	RID tempRID;
	unsigned pageNum;
	unsigned slotNum;

	ifstream ridsFile("rids_file", ios::in | ios::binary);
	if (ridsFile.is_open()) {
		ridsFile.seekg(0,ios::beg);
		for (int i = 0; i < numRecords; i++) {
			ridsFile.read(reinterpret_cast<char*>(&pageNum), sizeof(unsigned));
			ridsFile.read(reinterpret_cast<char*>(&slotNum), sizeof(unsigned));
			tempRID.pageNum = pageNum;
			tempRID.slotNum = slotNum;
			rids.push_back(tempRID);
		}
		ridsFile.close();
	}
}

// Read sizes from the disk - do not use this code.
//This is not a page-based operation. For test purpose only.
void readSizesFromDisk(vector<int> &sizes, int numRecords)
{
	int size;

	ifstream sizesFile("sizes_file", ios::in | ios::binary);
	if (sizesFile.is_open()) {

		sizesFile.seekg(0,ios::beg);
		for (int i = 0; i < numRecords; i++) {
			sizesFile.read(reinterpret_cast<char*>(&size), sizeof(int));
			sizes.push_back(size);
		}
		sizesFile.close();
	}
}


void printAttribute(void *data, AttrType &type) {
    int _int;
    float _float;
    string _varchar;
    cout << "Data: ";

    switch (type) {
    case TypeInt:
        memcpy((char *) &_int, (char *) data, sizeof(int));
        cout << _int;
        break;
    case TypeReal:
        memcpy((char *) &_float, (char *) data, sizeof(float));
        cout << _float;
        break;
    case TypeVarChar:
        int size;
        memcpy((char *) &size, (char *) data, sizeof(int));
        assert(size < PAGE_SIZE && size > 0);
        char buf[PAGE_SIZE];
        memcpy((char *) buf, (char *) data + sizeof(int), size);
        buf[size] = 0;
        _varchar.assign(buf);
        cout << _varchar;
        break;
    default:
        break;
    }
    cout << endl;
}

void scanIndex(const string &tableName, const string &attributeName, AttrType type) {
    RM_IndexScanIterator rm_IndexScanIterator;
    RC rc = rm->indexScan(tableName, attributeName, NULL, NULL, false, false, rm_IndexScanIterator);
    assert(rc == success);
    char key[PAGE_SIZE];
    RID rid;
    while (rm_IndexScanIterator.getNextEntry(rid, key) != IX_EOF) {
        cout << ">>>>" << endl;
        printAttribute(key, type);
        cout << "RID: <" << rid.pageNum << ", " << rid.slotNum << ">" << endl;
        cout << "<<<<" << endl;
    }
}

#endif
