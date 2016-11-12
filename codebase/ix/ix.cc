
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

RC IndexManager::findMidnode(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,const unsigned int pagNum,unsigned int nextpagNum)
{
	int key_insert;
	memcmp(&key_insert,key,sizeof(key));
	//read root page
	char*recordMid = (char*)malloc(PAGE_SIZE);
	char*recordLeft = (char*)malloc(PAGE_SIZE);
	char*recordRight = (char*)malloc(PAGE_SIZE);//should not be page size
	int keyMid,keyLeft,keyRight;
	//ixfileHandle.fileHandle.readPage(pagNum,data);
    vector<Attribute> attrs;
    attrs.push_back(attribute);
    void*data = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pagNum,data);
    int offset = PAGE_SIZE-8;
	//read specific field to judge whether leaf
	//unsigned int flag;
	memcpy(&flag, offset + (char*) data, 4);
	RID rid_ix;
	RID rid_ixLeft;
	RID rid_ixRight;
	if(flag==1)
	{
		//not leaf
		//begin search
		//binary search
		//read all slotnum
		int a_begin,a_end, a_mid;
		int keyRight,keyLeft;
		while(a_begin>a_end)
		{
			int slotnum;
			int a_mid;
			a_mid=floor((a_begin+a_end)/2);
			//getattribute firstly
			rid_ix.pageNum = pagNum;
			rid_ix.slotNum = a_mid;
			RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordMid);//we should modify this cause we have already catch whole page in data
			memcpy(&keyMid,(char*)recordMid+4,sizeof(recordMid)-4);
			//read records a_mid
			//if attrs is varchar we need to cut record length....
			if(recordMid<key)
			{
				a_end = a_mid-1;
			}
			else
			{
				a_begin = a_mid+1; //calculate slot number
			}
			//get key of a_mid+1
			//get key of a_mid-1
			keyRight = a_mid+1;
			rid_ixRight.pageNum = pagNum;
			rid_ixRight.slotNum = keyRight;
			keyLeft  = a_mid-1;
			rid_ixLeft.pageNum = pagNum;
			rid_ixLeft.slotNum = keyLeft;
			RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordLeft);
			memcpy(&keyLeft,recordLeft+4,sizeof(recordLeft)-4);
			RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordRight);
			memcpy(&keyRight,recordRight+4,sizeof(recordRight)-4);
			if(key_insert<=keyRight&&key_insert>keyLeft)
			{
				//find the position in this page, then go to next level;
				//read pointer-> get next page num
				memcpy(&nextpagNum,recordRight,4);
				break;
			}
		}

		return 0;

	}
	else
	{
		return 0;
	}

}

RC IndexManager::findLeafnode(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, RID &rid,const unsigned int pagNum)
{
	int key_insert;
	memcmp(&key_insert,key,sizeof(key));
	char*recordMid = (char*)malloc(PAGE_SIZE);
	char*recordLeft = (char*)malloc(PAGE_SIZE);
	char*recordRight = (char*)malloc(PAGE_SIZE);//should not be page size
	int keyMid,keyLeft,keyRight;
	//ixfileHandle.fileHandle.readPage(pagNum,data);
    vector<Attribute> attrs;
    attrs.push_back(attribute);
    void*data = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pagNum,data);
    int offset = PAGE_SIZE-8;
	//read specific field to judge whether leaf
	//unsigned int flag;
	memcpy(&flag, offset + (char*) data, 4);
	RID rid_ix;
	RID rid_ixLeft;
	RID rid_ixRight;
	//not leaf
	//begin search
	//binary search
	//read all slotnum
	int a_begin,a_end, a_mid;
	while(a_begin>a_end)
	{
		int slotnum;
		int a_mid;
		a_mid=floor((a_begin+a_end)/2);
		//getattribute firstly
		rid_ix.pageNum = pagNum;
		rid_ix.slotNum = a_mid;
		RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordMid);//we should modify this cause we have already catch whole page in data
		memcpy(&keyMid,(char*)recordMid+4,sizeof(recordMid)-8);
		//read records a_mid
		//if attrs is varchar we need to cut record length....
		if(recordMid<key)
		{
			a_end = a_mid-1;
		}
		else
		{
			a_begin = a_mid+1; //calculate slot number
		}
		//get key of a_mid+1
		//get key of a_mid-1
		keyRight = a_mid+1;
		rid_ixRight.pageNum = pagNum;
		rid_ixRight.slotNum = keyRight;
		keyLeft  = a_mid-1;
		rid_ixLeft.pageNum = pagNum;
		rid_ixLeft.slotNum = keyLeft;
		RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordLeft);
		memcpy(&keyLeft,(char*)recordLeft+4,sizeof(recordLeft)-8);
		RecordBasedFileManager::instance()->readRecord(ixfileHandle.fileHandle,attrs,rid_ix,recordRight);
		memcpy(&keyRight,(char*)recordRight+4,sizeof(recordRight)-8);
		if(key_insert<=keyRight&&key_insert>keyLeft)
		{
			//find the position in this page, then go to next level;
			//read pointer-> get next page num
			memcpy(&rid.pageNum,(char*)recordRight+sizeof(recordRight)-8,4);
			memcpy(&rid.slotNum,(char*)recordRight+sizeof(recordRight)-4,4);
		}
	}

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

	unsigned int pagnum = rootNodeNum;
	unsigned int nextpagnum;

	// when empty in create Bplus tree, rootpagnum = page 0; flag =0; means it is also leaf

	//find exceted position of the key should be
	while(flag==1)
	{
		findMidnode(...,pagnum,nextpagnum);
		pagnum = nextpagnum;
	}

	if(flag ==0)
	{
		findLeafnode(...,pagnum);
		pagnum =...;
		slotnum =...;

	}
//get the position

	if(pageType==leafNode)
	{

	//	findMidnode();
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

RC IXFileHandle:: readRecord(const void* dataPage, const Attribute &attribute,void *dataRead,RID rid)
{
	void* newPage = malloc(PAGE_SIZE);
	memcpy(newPage,dataPage,PAGE_SIZE);
	char* pointer = (char*) newPage;
	short int recordLength, recordOffset;
	pointer = pointer + PAGE_SIZE - 4 - 4 * rid.slotNum;
	memcpy(&recordOffset, pointer, 2);
	pointer += 2;
	memcpy(&recordLength, pointer, 2);//length should contain 4 pointer bytes

	if(recordLength==-1)
	{
		return -1;

	}
	memcpy(dataRead, (char*) newPage + recordOffset, recordLength);
	free(newPage);
	return 0;
}
