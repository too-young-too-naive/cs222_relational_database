
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	if(PagedFileManager::instance()->FileExists(fileName)){
		cout<<"The file has already existed!"<<endl;
		return -1;
	}
	else{
		PagedFileManager::instance()->createFile(fileName);
		return 0;
	}


}

RC IndexManager::destroyFile(const string &fileName)
{
	if(PagedFileManager::instance()->FileExists(fileName)!=0){
		cout<<"No file exists!"<<endl;
		return 0;
	}
	if(PagedFileManager::instance()->destroyFile(fileName)!=0){
		cout<<"Delete file failure!"<<endl;
		return -1;
	}
    return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	if(PagedFileManager::instance()->FileExists(fileName)){
		//cerr<<"openCounter= "<<ixfileHandle.openCounter<<endl;
		if(ixfileHandle.openCounter==1) return -1;//check that whether the file has already been opened.
		if(PagedFileManager::instance()->openFile(fileName.c_str(),ixfileHandle.fileHandle)!=0) return -1;

		ixfileHandle.openCounter++;
		if(ixfileHandle.fileHandle.getNumberOfPages()==0){
			void *newPage=malloc(PAGE_SIZE);
			memset(newPage,0,PAGE_SIZE);
		    ixfileHandle.fileHandle.appendPage(newPage);
		    free(newPage);
		}
		return 0;
	}
	else{

		cout<<"No file has already existed!"<<endl;
		return -1;
	}
    //return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	if(PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle)!=0) return -1;
    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	short int pageNum, pageType,keySize;
	short int freeSpacePointer, slotNum;
	short int aimNum;//the number of the entry that is in front of the insertion position
	short int recordLength,recordOffset;
	char* buffer=(char*)malloc(2000);
	pageNum=ixfileHandle.fileHandle.getNumberOfPages();
	void* newPage=malloc(PAGE_SIZE);
	memset(newPage,0,PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(rootNodeNum,newPage);
	memcpy(&pageType,(char*)newPage+PAGE_SIZE-4-4,4);//get the type of the node
	if(pageType==leafNode)
	{

		findPositionFromLeafNode();
		switch(attribute.type)
		{
		case TypeInt:{
			keySize=4;
			break;
		}
		case TypeReal:{
			keySize=4;
			break;
		}
		case TypeVarChar:{
			short int varcharLength;
			memcpy(&varcharLength,(char*)key,4);
			keySize=varcharLength+4;
			break;
		}

		}
		void* entry=malloc(keySize+8);
		memcpy((char*)entry,key,keySize);
		memcpy((char*)entry+keySize,&rid,8);
		if()
		/////////////////////move the rest records to make room for insertion/////////////////
		memcpy(&slotNum,(char*)newPage+PAGE_SIZE-4,2);
		for(int i=slotNum;i>aimNum;i--){

			memcpy(&recordOffset,(char*)newPage+PAGE_SIZE-4-4-4*i,2);
			memcpy(&recordLength,(char*)newPage+PAGE_SIZE-4-4-4*i+2,2);
			memcpy(buffer,(char*)newPage+recordOffset,recordLength);//copy the record into buffer
			memset((char*)newPage+recordOffset,0,recordLength);//erase the original record
			memcpy((char*)newPage+recordOffset+keySize,buffer,recordLength);//move the record from buffer to new place
			//not yet modify the slot to make the right order of it

		}
		////////////////////we now have maked space for insertion/////////////////////////////
		memcpy((char*)newPage+recordOffset,entry,keySize+8);//insert the entry, the entry`s size is keySize+8.






	}


    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    openCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount=fileHandle.readPageCounter;
    writePageCount=fileHandle.writePageCounter;
    appendPageCount=fileHandle.appendPageCounter;
    return 0;
}

