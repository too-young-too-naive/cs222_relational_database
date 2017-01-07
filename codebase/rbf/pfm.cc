#include "pfm.h"
#include <stdio.h>
#include <iostream>
#include<cmath>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
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


RC PagedFileManager::createFile(const string &fileName)
{



if(FileExists(fileName.c_str())){
//	 cout<<"The file has already existed!"<<endl;
	 return -1;
}
else{
	FILE *fp;
	fp=fopen(fileName.c_str(),"wb+");
	if(fp==NULL){
				cout<<"Create file error!"<<endl;
				return -1;
			}
			else{

				fclose(fp);
				return 0;
			}
}

}


RC PagedFileManager::destroyFile(const string &fileName)
{

			remove(fileName.c_str());

}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{   //cout<<"Test1!"<<endl;
	FILE *fp;
	fp=fopen(fileName.c_str(),"rb+");
    if(fp==NULL){
    	if(errno==ENOENT){
    		cout<<"No file exists~"<<endl;
    		return -1;
    	}
    	else{
    		cout<<"file open failed."<<endl;
    		return -1;
    	}
    }
    else{
    	fileHandle.file=fp;
    	fileHandle.getNumberOfPages();

    	return 0;
    }


}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{


	FILE *fp=fileHandle.file;
 fclose(fp);
 return 0;

}

FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;

}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	//cout<<"have readddddddd"<<endl;
    FILE *fp=file;
    if(pageNum<getNumberOfPages()){
   // 	cout<<"enter read"<<endl;
    	fseek(fp,pageNum*PAGE_SIZE,SEEK_SET);
        fread(data,1,PAGE_SIZE,fp);
        readPageCounter++;
     //   cout<<"readPageCounter"<<readPageCounter<<endl;
        return 0;
    }
    else{
    	cout<<"No existing page to be read!"<<endl;
    	return -1;
    }

}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	FILE *fp=file;
   if(pageNum<getNumberOfPages())
   {
	   fseek(fp,pageNum*PAGE_SIZE,SEEK_SET);
	   fwrite(data,sizeof(char),PAGE_SIZE,fp);
	   writePageCounter++;
	   return 0;

   }
   else{
       	cout<<""<<endl;
       	return -1;
       }

}


RC FileHandle::appendPage(const void *data)
{
	FILE *fp=file;
	fseek(fp,0,SEEK_END);
	fwrite(data,1,PAGE_SIZE,fp);
	appendPageCounter++;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{   unsigned long fileLength, pageNumber;

	FILE *fp=file;
	fseek(fp,0,SEEK_END);
	fileLength=ftell(fp);
	pageNumber=ceil(fileLength/PAGE_SIZE);
	if(pageNumber<0){
		cout<<"File page number error!"<<endl;
		return -1;
	}
	else{
	//cout<<"pageNumber is "<<pageNumber<<endl;
    return pageNumber;
	}
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{

	readPageCount = readPageCounter;
    writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
	return 0;



}
