#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "pfm.h"
#include "rbfm.h"

using namespace std;

const int success = 0;
unsigned total = 0;

// Check if a file exists
bool FileExists(string fileName) {
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) == 0)
		return true;
	else
		return false;
}

// Record Descriptor for TweetMessage
void createRecordDescriptorForTweetMessage(
		vector<Attribute> &recordDescriptor) {

	Attribute attr;
	attr.name = "tweetid";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "userid";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "sender_location";
	attr.type = TypeReal;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "send_time";
	attr.type = TypeReal;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "referred_topics";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 500;
	recordDescriptor.push_back(attr);

	attr.name = "message_text";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 500;
	recordDescriptor.push_back(attr);

}

// Function to prepare the data in the correct form to be inserted/read
void prepareRecordForTweetMessage(const int tweetid, const int userid,
		const float sender_location, const float send_time,
		const int referred_topicsLength, const string &referred_topics,
		const int message_textLength, const string &message_text, void *buffer,
		int *recordSize) {

	int offset = 0;

	// tweetid
	memcpy((char *) buffer + offset, &tweetid, sizeof(int));
	offset += sizeof(int);

	// userid
	memcpy((char *) buffer + offset, &userid, sizeof(int));
	offset += sizeof(int);

	// sender_location
	memcpy((char *) buffer + offset, &sender_location, sizeof(float));
	offset += sizeof(float);

	// send_time
	memcpy((char *) buffer + offset, &send_time, sizeof(float));
	offset += sizeof(float);

	// referred_topics
	memcpy((char *) buffer + offset, &referred_topicsLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, referred_topics.c_str(),
			referred_topicsLength);
	offset += referred_topicsLength;

	// message_text
	memcpy((char *) buffer + offset, &message_textLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, message_text.c_str(), message_textLength);
	offset += message_textLength;

	*recordSize = offset;

}

ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::in | std::ifstream::binary);
    in.seekg(0, std::ifstream::end);
    cout << filename << " - file size:" << in.tellg() << endl;
    return in.tellg();
}

int RBFTest_13(RecordBasedFileManager *rbfm) {
	// Functions Tested:
	// 1. Open the File created by test 12
	// 2. Read a record that does not exist - should fail
	// 3. Insert a record
	// 4. Read a record
	// 5. Close the File
	// 6. Destroy the File
	cout << "****In RBF Test Case 13****" << endl;

	RC rc;
	string fileName = "test12";

	// Open the file
	FileHandle fileHandle;
	rc = rbfm->openFile(fileName.c_str(), fileHandle);
	if (rc != success) {
		return -1;
	}
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	RID rid;
	int recordSize = 0;
	void *record = malloc(2000);
	void *returnedData = malloc(2000);
	void *returnedData2 = malloc(2000);

	vector<Attribute> recordDescriptorForTweetMessage;
	createRecordDescriptorForTweetMessage(recordDescriptorForTweetMessage);

	// Assign a RID that does not exist
	rid.pageNum = 100;
	rid.slotNum = 100;

	// Read a Record that does not exist - should fail
	rc = rbfm->readRecord(fileHandle, recordDescriptorForTweetMessage, rid, returnedData);
	if (rc == success) {
		cout << "The read Record should fail. However, it returned a success RC." << endl;
		free(record);
		free(returnedData);
		free(returnedData2);
		return -1;
	}
	assert(rc != success);

	// Insert a record into a file
	prepareRecordForTweetMessage(101, 1, 99.987101, 1013.45, 13, "shortcut_menu", 31, "Finding shortcut_menu was easy.", record, &recordSize);
	cout << "Insert Data:" << endl;
	rbfm->printRecord(recordDescriptorForTweetMessage, record);

	rc = rbfm->insertRecord(fileHandle, recordDescriptorForTweetMessage, record, rid);
	if (rc != success) {
		free(record);
		free(returnedData);
		free(returnedData2);
		return -1;
	}
	assert(rc == success);

	// Given the rid, read the record from file
	rc = rbfm->readRecord(fileHandle, recordDescriptorForTweetMessage, rid, returnedData2);
	if (rc != success) {
		free(record);
		free(returnedData);
		free(returnedData2);
		return -1;
	}
	assert(rc == success);

	cout << "Returned Data:" << endl;
	rbfm->printRecord(recordDescriptorForTweetMessage, returnedData2);

	free(record);
	free(returnedData);
	free(returnedData2);

	// Close the file
	rc = rbfm->closeFile(fileHandle);
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	int fsize = filesize(fileName.c_str());
	if (fsize == 0) {
		cout << "File Size should not be zero at this moment." << endl;
		return -1;
	}
	
	// Destroy the file
	rc = rbfm->destroyFile(fileName.c_str());
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	if (FileExists(fileName.c_str())) {
		cout << "Failed to destroy the file!" << endl;
		return -1;
	}

	return 0;
}

int main() {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); // To test the functionality of the record-based file manager

	int rc = RBFTest_13(rbfm);
	if (rc == 0) {
		cout << "Test Case 13 Passed!" << endl << endl;
		total += 4;
	} else {
		cout << "Test Case 13 Failed!" << endl << endl;
	}

	cout << "Score for Test Case 13: " << total << " / 4" << endl;

	return 0;
}
