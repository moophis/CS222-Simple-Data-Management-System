#ifndef _pfm_h_
#define _pfm_h_

#include <cstdio>

typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096

class FileHandle;


class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file

protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    void setNumberOfPages(unsigned pages);                              // Set the number of pages in the file
    FILE *getFilePointer();                                             // Get the file pointer associated with the file
    void setFilePointer(FILE *ptr);                                     // Set the file pointer associated with the file

private:
    FILE *filePtr;                                            // Associated file pointer
    unsigned pageCount;                                          // Number of pages in the file
};

// Enum: status code
enum {
    SUCCESSFUL    =  0,         // successful operation
    ERR_EXIST     = -2,         // error: file already exists
    ERR_NOT_EXIST = -1,         // error: file does not exist
    ERR_LOCATE    = -3,         // error: cannot navigate certain position of the file
    ERR_WRITE     = -4,         // error: cannot write data into the file
    ERR_READ      = -5,         // error: cannot read data from the file
    ERR_NULLPTR   = -6,         // error: null pointer error
};

#endif
