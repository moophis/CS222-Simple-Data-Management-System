#include "pfm.h"
#include <iostream>
#include <cstdio>

/////////////////////////////////////////////////////

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}

/**
 * Create a file.
 *
 * @param fileName
 *          the name of the file to be created.
 * @return status
 */
RC PagedFileManager::createFile(const char *fileName)
{
    // Test existence
    FILE *fp = fopen(fileName, "r");
    if (fp) {
        return ERR_EXIST;
    } else {
        fp = fopen(fileName, "w");
        if (!fp) {
            return ERR_NOT_EXIST;
        }
        return SUCCESSFUL;
    }
}

/**
 * Delete a file.
 *
 * @param fileName
 *          the name of the file to be destroyed.
 * @return status
 */
RC PagedFileManager::destroyFile(const char *fileName)
{
    if (remove(fileName) != 0) {
        __trace();
        return ERR_NOT_EXIST;
    } else {
//        __trace();
        return SUCCESSFUL;
    }
}

/**
 * Open a file.
 *
 * @param fileName
 *          the name of the file to be opened.
 * @param fileHandle
 *          the file handle associated with this file.
 * @return status
 */
RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
    FILE *fp = fopen(fileName, "r+");
    if (!fp) {
//        __trace();
//        std::cout << "-->Cannot open file: " << fileName << std::endl;
        return ERR_NOT_EXIST;
    }

    // get the file size in pages(should be multiple of PAGE_SIZE)
    long fileSize = 0l;
    if (fseek(fp, 0, SEEK_END)) {
        __trace();
        return ERR_LOCATE;
    }
    if ((fileSize = ftell(fp)) == -1) {
        __trace();
        return ERR_LOCATE;
    }
    if (fileSize % PAGE_SIZE != 0) {
        // File size is not a multiple of PAGE_SIZE
        // Probably this file has been damaged
        return ERR_ALIGN;
    }

//    fileHandle.setNumberOfPages(fileSize / PAGE_SIZE);
    fileHandle.setFilePointer(fp);
    fileHandle.setFileName(fileName);
//    std::cout << "### In PagedFileManager::openFile(), set fileHandle: -> name: " << fileHandle.getFileName()
//         << ", # of pages: " << fileHandle.getNumberOfPages() << std::endl;

    return SUCCESSFUL;
}

/**
 * Close a file.
 *
 * @param fileName
 *          the name of the file to be closed.
 * @return status
 */
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (fclose(fileHandle.getFilePointer())) {
        return ERR_NOT_EXIST;
    }
    return SUCCESSFUL;
}


/////////////////////////////////////////////////////

FileHandle::FileHandle()
{
//    pageCount = 0;
    filePtr   = NULL;
//    fileName  = NULL;

    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}

/**
 * Read a page of data from the file
 * @param pageNum
 *              the page number to read
 * @param data
 *              the pointer to the buffer for storing the read data
 * @return status
 */
RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (!data) {
        return ERR_NULLPTR;
    }
    if (pageNum >= getNumberOfPages()) {
        __trace();
        std::cout << "--> pageNum: " << pageNum << ", pageCount: " << getNumberOfPages() << std::endl;
        return ERR_LOCATE;
    }

    long curPos = pageNum * PAGE_SIZE;

    if (fseek(filePtr, curPos, SEEK_SET)) {
        __trace();
        return ERR_LOCATE;
    }

    if (fread(data, sizeof(char), PAGE_SIZE, filePtr) != PAGE_SIZE) {
        return ERR_READ;
    }

    readPageCounter++;
//    std::cout << fileName << " readPageCounter " << readPageCounter << std::endl;
    return SUCCESSFUL;
}

/**
 * Write a page of data to the file given page number.
 * Here we assume that data size will not exceed the size of a page.
 *
 * @param pageNum
 *          the number of the page to be written.
 * @param data
 *          the data
 * @return status
 */
RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    bool append = false;
    unsigned totalPages = getNumberOfPages();
    if (pageNum > totalPages || pageNum < 0) {
        __trace();
        std::cout << "Trying to write page " << pageNum << " but total page count is " << totalPages << std::endl;
        return ERR_LOCATE;
    }
    if (pageNum == totalPages) {
        append = true;
    }

    long curPos = pageNum * PAGE_SIZE;

    if (fseek(filePtr, curPos, SEEK_SET)) {
        __trace();
        return ERR_LOCATE;
    }

    if (fwrite(data, sizeof(char), PAGE_SIZE, filePtr) != PAGE_SIZE) {
        __trace();
        return ERR_WRITE;
    }

    if (append) {
        appendPageCounter++;
//        std::cout << fileName << " appendPageCounter " << appendPageCounter << std::endl;
    } else {
        writePageCounter++;
//        std::cout << fileName << " writePageCounter " << writePageCounter << std::endl;
    }
    return SUCCESSFUL;
}

/**
 * Append a page of data to the file.
 * Here we assume that data size will not exceed the size of a page.
 *
 * @param data
 *           the data
 * @return status
 */
RC FileHandle::appendPage(const void *data)
{
    RC rc = writePage(getNumberOfPages(), data);
    if (rc == SUCCESSFUL) {
//        __trace();
//        std::cout << "--> appended a new page, pageCount now is " << getNumberOfPages() << std::endl;
    } else {
        __trace();
        std::cout << "--> Cannot write data in a new page, rc = " << rc
             << " current pageCount " << getNumberOfPages() << std::endl;
    }

    return rc;
}

/**
 * Get the number of pages.
 *
 * @return # of pages
 */
unsigned FileHandle::getNumberOfPages()
{
    // get the file size in pages(should be multiple of PAGE_SIZE)
    long fileSize = 0l;
    FILE *fp = getFilePointer();
    if (fseek(fp, 0, SEEK_END)) {
        __trace();
        perror("Cannot locate");
        exit(ERR_LOCATE);
    }
    if ((fileSize = ftell(fp)) == -1) {
        __trace();
        exit(ERR_LOCATE);
    }
    if (fileSize % PAGE_SIZE != 0) {
        // File size is not a multiple of PAGE_SIZE
        // Probably this file has been damaged
        exit(ERR_ALIGN);
    }
    return fileSize / PAGE_SIZE;
}

/**
 * @Depreciated
 * Set the number of pages.
 *
 * @param pages
 *          # of pages
 */
void FileHandle::setNumberOfPages(unsigned pages)
{
//    pageCount = pages;
}

/**
 * Get the file pointer in this file handle.
 *
 * @return file pointer
 */
FILE *FileHandle::getFilePointer()
{
    return filePtr;
}

/**
 * Set the file pointer in this file handle.
 *
 * @param ptr
 *          the file pointer
 */
void FileHandle::setFilePointer(FILE *ptr)
{
    filePtr = ptr;
}

/**
 * Get the file name.
 *
 * @return file name
 */
char *FileHandle::getFileName() {
    return (char *) fileName.c_str();
}

/**
 * Set the file name.
 *
 * @param file name
 */
void FileHandle::setFileName(const char *name) {
    fileName = std::string((char *)name);
}


/**
 * Collect statistics.
 */
RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
//    __trace();
//    std::cout << "fileName: " << fileName << " rc " << readPageCounter << " wc " << writePageCounter << " ac " << appendPageCounter << std::endl;

    return SUCCESSFUL;
}
