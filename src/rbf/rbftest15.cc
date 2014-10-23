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

// Record Descriptor for TwitterUser
void createRecordDescriptorForTwitterUser(vector<Attribute> &recordDescriptor) {

	Attribute attr;
	attr.name = "userid";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "screen_name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 300;
	recordDescriptor.push_back(attr);

	attr.name = "lang";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 300;
	recordDescriptor.push_back(attr);

	attr.name = "friends_count";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "statuses_count";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "username";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 300;
	recordDescriptor.push_back(attr);

	attr.name = "followers_count";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

	attr.name = "satisfaction_score";
	attr.type = TypeReal;
	attr.length = (AttrLength) 4;
	recordDescriptor.push_back(attr);

}

// Function to prepare the data in the correct form to be inserted/read
void prepareRecordForTwitterUser(const int userid, const int screen_nameLength,
		const string &screen_name, const int langLength, const string &lang,
		const int friends_count, const int statuses_count,
		const int usernameLength, const string &username,
		const int followers_count, const float satisfaction_score, void *buffer,
		int *recordSize) {

	int offset = 0;

	// userid
	memcpy((char *) buffer + offset, &userid, sizeof(int));
	offset += sizeof(int);

	// screen_name
	memcpy((char *) buffer + offset, &screen_nameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, screen_name.c_str(), screen_nameLength);
	offset += screen_nameLength;

	// lang
	memcpy((char *) buffer + offset, &langLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, lang.c_str(), langLength);
	offset += langLength;

	// friends_count
	memcpy((char *) buffer + offset, &friends_count, sizeof(int));
	offset += sizeof(int);

	// statuses_count
	memcpy((char *) buffer + offset, &statuses_count, sizeof(int));
	offset += sizeof(int);

	// username
	memcpy((char *) buffer + offset, &usernameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, username.c_str(), usernameLength);
	offset += usernameLength;

	// followers_count
	memcpy((char *) buffer + offset, &followers_count, sizeof(int));
	offset += sizeof(int);

	// satisfaction_score
	memcpy((char *) buffer + offset, &satisfaction_score, sizeof(float));
	offset += sizeof(float);

	*recordSize = offset;
}

void prepareLargeRecordForTwitterUser(const int index, void *buffer,
		int *size) {
	int offset = 0;

	int count = index % 200 + 1;
	int text = index % 26 + 97;
	string suffix (count, text);

	// userid = index
	memcpy((char *) buffer + offset, &index, sizeof(int));
	offset += sizeof(int);

	// screen_name
	string screen_name = "NilaMilliron@" + suffix;
	int screen_nameLength = screen_name.length();

	memcpy((char *) buffer + offset, &screen_nameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, screen_name.c_str(), screen_nameLength);
	offset += screen_nameLength;

	// lang
	string lang = "En " + suffix;
	int langLength = lang.length();

	memcpy((char *) buffer + offset, &langLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, lang.c_str(), langLength);
	offset += langLength;

	// friends_count
	int friends_count = count - 1;

	memcpy((char *) buffer + offset, &friends_count, sizeof(int));
	offset += sizeof(int);

	// statuses_count
	int statuses_count = count + 1;

	memcpy((char *) buffer + offset, &statuses_count, sizeof(int));
	offset += sizeof(int);

	// username
	string username = "Nilla Milliron " + suffix;
	int usernameLength = username.length();

	memcpy((char *) buffer + offset, &usernameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, username.c_str(), usernameLength);
	offset += usernameLength;

	// followers_count
	int followers_count = count + 2;

	memcpy((char *) buffer + offset, &followers_count, sizeof(int));
	offset += sizeof(int);

	// satisfaction_score
	float satisfaction_score = (float) count + 0.1;

	memcpy((char *) buffer + offset, &satisfaction_score, sizeof(float));
	offset += sizeof(float);

	*size = offset;

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

void prepareLargeRecordForTweetMessage(const int index, void *buffer,
		int *size) {
	int offset = 0;

	int count = index % 400 + 1;
	int text = index % 26 + 65;
	string suffix (count, text);

	// tweetid = index
	int tweetid = index + 1;

	memcpy((char *) buffer + offset, &tweetid, sizeof(int));
	offset += sizeof(int);

	// userid
	memcpy((char *) buffer + offset, &index, sizeof(int));
	offset += sizeof(int);

	// sender_location
	float sender_location = (float) count + 0.1;

	memcpy((char *) buffer + offset, &sender_location, sizeof(float));
	offset += sizeof(float);

	// send_time
	float send_time = (float) index + 0.2;

	memcpy((char *) buffer + offset, &send_time, sizeof(float));
	offset += sizeof(float);

	// referred_topics
	string referred_topics = "shortcut_menu" + suffix;
	int referred_topicsLength = referred_topics.length();

	memcpy((char *) buffer + offset, &referred_topicsLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, referred_topics.c_str(),
			referred_topicsLength);
	offset += referred_topicsLength;

	// message_text
	string message_text = "shortcut-menu is helpful: " + suffix;
	int message_textLength = message_text.length();
	memcpy((char *) buffer + offset, &message_textLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, message_text.c_str(), message_textLength);
	offset += message_textLength;

	*size = offset;
}

bool compareFileSizes(string fileName1, string fileName2) {
	streampos s1, s2;
	ifstream in1(fileName1.c_str(), ifstream::in | ifstream::binary);
	in1.seekg(0, ifstream::end);
	s1 = in1.tellg();

	ifstream in2(fileName2.c_str(), ifstream::in | ifstream::binary);
	in2.seekg(0, ifstream::end);
	s2 = in2.tellg();

	cout << "File 1 size: " << s1 << endl;
	cout << "File 2 size: " << s2 << endl;

	if (s1 != s2) {
		return false;
	}
	return true;
}

ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::in | std::ifstream::binary);
    in.seekg(0, std::ifstream::end);
    cout << filename << " - file size:" << in.tellg() << endl;
    return in.tellg();
}

int RBFTest_15(RecordBasedFileManager *rbfm) {
	// Functions Tested:
	// 1. Open the File created by test 14 - test14a
	// 2. Read 50000 records - test14a
	// 3. Check correctness
	// 4. Close the File - test14a
	// 5. Destroy the File - test14a
	// 6. Open the File created by test 14 - test14b
	// 7. Read 50000 records - test14b
	// 8. Check correctness
	// 9. Close the File - test14b
	// 10. Destroy the File - test14b

	cout << "****In RBF Test Case 15****" << endl;

	RC rc;
	string fileName = "test14a";
	string fileName2 = "test14b";

	// Open the file "test14a"
	FileHandle fileHandle;
//	rc = rbfm->openFile(fileName.c_str(), fileHandle);
	rc = rbfm->openFile(fileName, fileHandle);
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	void *record = malloc(1000);
	void *returnedData = malloc(1000);
	int numRecords = 50000;

	vector<Attribute> recordDescriptorForTwitterUser, recordDescriptorForTweetMessage;
	createRecordDescriptorForTwitterUser(recordDescriptorForTwitterUser);

	vector<RID> rids, rids2;
	RID tempRID, tempRID2;

	// Read rids from the disk - do not use this code. This is not a page-based operation. For test purpose only.
	ifstream ridsFileRead("test14a_rids", ios::in | ios::binary);

	unsigned pageNum;
	unsigned slotNum;

	if (ridsFileRead.is_open()) {
		ridsFileRead.seekg(0,ios::beg);
		for (int i = 0; i < numRecords; i++) {
			ridsFileRead.read(reinterpret_cast<char*>(&pageNum), sizeof(unsigned));
			ridsFileRead.read(reinterpret_cast<char*>(&slotNum), sizeof(unsigned));
			if (i % 10000 == 0) {
				cout << "test14a - loaded RID #" << i << ": " << pageNum << ", " << slotNum << endl;
			}
			tempRID.pageNum = pageNum;
			tempRID.slotNum = slotNum;
			rids.push_back(tempRID);
		}
		ridsFileRead.close();
	}

	if (rids.size() != numRecords) {
		return -1;
	}

	// Compare records from the disk read with the record created from the method
	for (int i = 0; i < numRecords; i++) {
		memset(record, 0, 1000);
		memset(returnedData, 0, 1000);
		rc = rbfm->readRecord(fileHandle, recordDescriptorForTwitterUser, rids[i],
				returnedData);
		if (rc != success) {
			return -1;
		}
		assert(rc == success);

		int size = 0;
		prepareLargeRecordForTwitterUser(i, record, &size);
		if (memcmp(returnedData, record, size) != 0) {
			cout << "Comparison failed - Test Case 15 Failed! " << i << endl << endl;
			free(record);
			free(returnedData);
			return -1;
		}
	}

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

	// Destroy File
	rc = rbfm->destroyFile(fileName.c_str());
	if (rc != success) {
		return -1;
	}
	assert(rc == success);


	// Open the file "test14b"
	FileHandle fileHandle2;
//	rc = rbfm->openFile(fileName2.c_str(), fileHandle2);
	rc = rbfm->openFile(fileName2, fileHandle2);
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	createRecordDescriptorForTweetMessage(recordDescriptorForTweetMessage);

	cout << endl;

	// Read rids from the disk - do not use this code. This is not a page-based operation. For test purpose only.
	ifstream ridsFileRead2("test14b_rids", ios::in | ios::binary);

	if (ridsFileRead2.is_open()) {
		ridsFileRead2.seekg(0,ios::beg);
		for (int i = 0; i < numRecords; i++) {
			ridsFileRead2.read(reinterpret_cast<char*>(&pageNum), sizeof(unsigned));
			ridsFileRead2.read(reinterpret_cast<char*>(&slotNum), sizeof(unsigned));
			if (i % 10000 == 0) {
				cout << "test14b - loaded RID #" << i << ": " << pageNum << ", " << slotNum << endl;
			}
			tempRID2.pageNum = pageNum;
			tempRID2.slotNum = slotNum;
			rids2.push_back(tempRID2);
		}
		ridsFileRead2.close();
	}

	if (rids2.size() != numRecords) {
		return -1;
	}

	// Compare records from the disk read with the record created from the method
	for (int i = 0; i < numRecords; i++) {
		memset(record, 0, 1000);
		memset(returnedData, 0, 1000);
		rc = rbfm->readRecord(fileHandle2, recordDescriptorForTweetMessage, rids2[i],
				returnedData);
		if (rc != success) {
			return -1;
		}
		assert(rc == success);

		int size = 0;
		prepareLargeRecordForTweetMessage(i, record, &size);
		if (memcmp(returnedData, record, size) != 0) {
			cout << "Comparison failed - Test Case 15 Failed!" << endl << endl;
			free(record);
			free(returnedData);
			return -1;
		}
	}

	// Close the file
	rc = rbfm->closeFile(fileHandle2);
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	fsize = filesize(fileName2.c_str());
	if (fsize == 0) {
		cout << "File Size should not be zero at this moment." << endl;
		return -1;
	}

	// Destroy File
	rc = rbfm->destroyFile(fileName2.c_str());
	if (rc != success) {
		return -1;
	}
	assert(rc == success);

	free(record);
	free(returnedData);

	remove("test14a_rids");
	remove("test14b_rids");

	return 0;
}

int main() {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); // To test the functionality of the record-based file manager

	int rc = RBFTest_15(rbfm);
	if (rc == 0) {
		cout << "Test Case 15 Passed!" << endl << endl;
		total += 6;
	} else {
		cout << "Test Case 15 Failed!" << endl << endl;
	}

	cout << "Score for Test Case 15: " << total << " / 6" << endl;

	return 0;
}
