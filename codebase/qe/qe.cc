
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
	//filteriter = input;

	in=input;
//	 in->rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
		vector <Attribute> rd;
		in->getAttributes(rd);
		//cout<<rd[1].name<<"\n";
		cond=condition;

}

// ... the rest of your implementations go here
RC Filter::getNextTuple(void *data)
{
	while(in->getNextTuple(data)!=-1)
               	{
	//	cout<<"test3"<<endl;
				//	cout<<"satisfiesCondition(data,cond,in"<<satisfiesCondition(data,cond,in)<<endl;
               		if(satisfiesCondition(data,cond,in))
               			return 0;
               	}
               	return -1;

}


////////////////////////////////////////////////
int recordLen(const void *data,int start, const vector <Attribute> &recordDescriptor,int upto_attr)
{
	int *in;
	unsigned tot=0;
	for(unsigned i=0;i<recordDescriptor.size() && i<upto_attr;i++)
	{
		if(recordDescriptor[i].type==2)
		{
			in=(int *)((char *)data+start+tot);
			tot+=sizeof(int)+(*in);
		}
		else
			{
				if(recordDescriptor[i].type==0){
					tot+=sizeof(int);
				   }
				else if(recordDescriptor[i].type==1){
					tot+=sizeof(float);
				}
			}
	}
	return tot;
}



int AttrFromRecord(const vector<Attribute> &recordDescriptor,const string attributeName, const void *data,const void *pg)
{
	int attr_to_read=0;
		for(attr_to_read=0;attr_to_read<recordDescriptor.size();attr_to_read++)
		{
			if(recordDescriptor[attr_to_read].name==attributeName)
				break;
		}
		if(attr_to_read==recordDescriptor.size())
			return -1;
		int curr=recordLen(pg,0,recordDescriptor,attr_to_read);
			switch(recordDescriptor[attr_to_read].type)
			    	{
						case 2:
							//in=(int *)((char *)pg+skip+curr);
							memcpy((char *)data,(char *)pg+curr,sizeof(int));
							memcpy((char *)data+sizeof(int),(char *)pg+curr+sizeof(int),*(int *)data);
							return *((int *)data)+sizeof(int);
							break;
						case 0:
							memcpy((char *)data,(char *)pg+curr,sizeof(int));
							return sizeof(int);
							break;
						case 1:
							memcpy((char *)data,(char *)pg+curr,sizeof(float));
							return sizeof(float);
							break;
			    	}
			return -1;
}

template <class T>

bool checkSatisfy(T a,T b,CompOp compOp)
{
	if(compOp==EQ_OP)
	{
//		cout<<"enteraaa"<<endl;
	//	cout<<"a "<<a<<endl;
//		cout<<"b "<<b<<endl;
		return a==b;
	}
	if(compOp==LT_OP)
		return a<b;
	if(compOp==GT_OP)
		return a>b;
	if(compOp==LE_OP)
		return a<=b;
	if(compOp==GE_OP)
		return a>=b;
	if(compOp==NE_OP)
		return a!=b;
	if(compOp==NO_OP)
		return true;
	return false;
}

int anum = 0;

bool checkSatisfyvar(const char* a,const char* b,CompOp compOp)
{
	if(compOp==EQ_OP)
	{

		if(strcmp(a,b)==0)
		{
			anum =anum +1;
	//		cout<<"enteraaa"<<anum<<endl;
	//		cout<<"finish"<<a<<endl;

			return 1;
		}
	}
	if(compOp==LT_OP)
		return a<b;
	if(compOp==GT_OP)
		return a>b;
	if(compOp==LE_OP)
		return a<=b;
	if(compOp==GE_OP)
		return a>=b;
	if(compOp==NE_OP)
		return a!=b;
	if(compOp==NO_OP)
		return true;
	return false;
}



bool satisfiesCompOp(CompOp compOp,void *value,void *attr_value,int attr_type)
{
	if(compOp==NO_OP)
	return true;
	if(attr_type==0)
		return checkSatisfy(*(int *)attr_value,*(int *)value,compOp);
	if(attr_type==1)
		return checkSatisfy(*(int *)attr_value,*(int *)value,compOp);
	if(attr_type==2)
	{
		const char* a = (char*)value;
		const char* b = (char*)attr_value;
//		cout<<"a ---------------    "<<a<<endl;
//		cout<<"b  --------------"<<b<<endl;
	/*	string a="",b="";
		for(int i=0;i<*(int *)attr_value;i++)
		{
			a+=*((char *)attr_value+sizeof(int));
		}
		for(int i=0;i<*(int *)value;i++)
		{
			b+=*((char *)value+sizeof(int));
		}
		*/
		return checkSatisfyvar (b,a,compOp);
	}
}


bool Filter::satisfiesCondition(void *data,Condition cond,Iterator *in)
{
	//return true;
	/*if(*(int *)((char *)data+sizeof(int))<=25)
		return true;
	return false;*/
	vector <Attribute> rd;
	in->getAttributes(rd);
	int indL=0,indR=0;
	int offset = 0;
	int varcharLength = 0;
//	cout<<"rd.size()"<<rd.size()<<endl;
//	cout<<"cond.lhsAttr"<<cond.lhsAttr<<endl;
	int num = 0;
	for(indL=0;indL<rd.size();indL++)
	{
//		cout<<"rd1].name"<<rd[1].name<<endl;



		switch(rd[indL].type)
		{
			case 0:
			{
				int offinital = offset;
				offset = offset+4;
				int integ;
				memcpy(&integ,(char*)data+1+offinital,sizeof(int));
		//		cout<<"integ"<<integ<<endl;

				break;
			}
			case 1:
			{

				int offinital = offset;
				offset = offset+4;
				int real;
				memcpy(&real,(char*)data+1+offinital,sizeof(int));
		//		cout<<"real"<<real<<endl;

				break;

			}
			case 2:
			{

				memcpy(&varcharLength,(char*)data+1+offset,sizeof(int));
		//		cout<<"varchhhhhhhhhhh"<<varcharLength<<endl;
				offset=offset+4;
				void* var = malloc(varcharLength);
				memcpy(var,(char*)data+1+offset,varcharLength);
				offset = offset+varcharLength;
			//	cout<<"varrrrrrrrrrr "<<(char*)var<<endl;
			}
			defult: break;
		}
	//	cout<<"rdtype"<<rd[indL].type<<endl;
	//	cout<<"offset"<<offset<<endl;
	//	cout<<"rdname"<<rd[indL].name<<endl;
	//	cout<<"cond.lhaAttr"<<cond.lhsAttr<<endl;
		if(rd[indL].name==cond.lhsAttr)
		{
			num = indL;
			break;
		}
	}
	for(indR=0;indR<rd.size();indR++)
	{
		if(rd[indR].name==cond.rhsAttr)
			break;
	}
	//if(indL>=rd.size()||(cond.bRhsIsAttr&&indR>=rd.size())||rd[indL].type!=rd[indR].type)
		//return false;
//	int offset = 0;
//	int valueA = *(int *)((char *)data+1+offset);//1 null

//	cout << "offset " << offset<<endl;

//	offset = offset + sizeof(int);


//	cout << "left.B " << valueB<<endl;
	void *lhs=malloc(PAGE_SIZE);
	void *rhs=malloc(PAGE_SIZE);
	//cout<<indL<<"\n";
	//AttrFromRecord(rd,cond.lhsAttr,lhs,data);
//cout<<"indl"<<indL<<endl;
//	cout<<"offset for comopare"<<offset<<endl;
	switch(rd[num].type)//rd[indL].type??????????????????????
				{
					case 0:
					{
						offset = offset -4;
						int valueB = *(int *)((char *)data+1+offset);
						memcpy(lhs,&valueB,4);
						break;
					}
					case 1:
					{
						offset = offset -4;
						float valueB = *(float *)((char *)data+1+offset);
						memcpy(lhs,&valueB,4);
						break;

					}
					case 2:
					{
//						cout<<"varchar"<<varcharLength<<endl;
						offset=offset-varcharLength;
						memcpy(lhs,(char*)data+1+offset,varcharLength);
//						cout<<"varchasadar         *"<<(char*)lhs<<endl;
					}
				//	defult: break;
				}

	if(cond.bRhsIsAttr)
	AttrFromRecord(rd,cond.rhsAttr,rhs,data);
	else
	{
	//	cout<<"enter else"<<endl;
		int l=0;
		if(rd[indL].type==0)
		{
			l=sizeof(int);
			memcpy(rhs,(char*)cond.rhsValue.data,l);
		}
		if(rd[indL].type==1)
		{
			l=sizeof(float);
			memcpy(rhs,(char*)cond.rhsValue.data,l);
		}
		if(rd[indL].type==2)
		{
		//	cout<<"enter rhs"<<endl;
			l=*(int *)cond.rhsValue.data;
			memcpy(rhs,(char*)cond.rhsValue.data+4,l);
		}

	}
	//cout<<"valuedata  "<<cond.rhsValue.data<<endl;
	//cout<<"rhssss   --------"<<(char*)rhs<<endl;
//	cout<<(char*)lhs<<" aaaaaaaaaaaaaaa"<<(*(int *)rhs)<<"\n";
	bool res= satisfiesCompOp(cond.op,rhs,lhs,rd[indL].type);
	free(lhs);
	free(rhs);
	return res;
}





Project::Project(Iterator *input,const vector<string> &attrNames)
{
	in=input;
	projAttrs=attrNames;
}

RC Project::getNextTuple(void *data)
        {
        	void *rec=malloc(PAGE_SIZE);
        	if(in->getNextTuple(rec)==-1)
        	{
        		free(rec);
        		return -1;
        	}
        	vector <Attribute> recordDescriptor;
        	in->getAttributes(recordDescriptor);
        	int curr=0;
        	for(int i=0;i<projAttrs.size();i++)
        	{
        		curr=curr+AttrFromRecord(recordDescriptor,projAttrs[i],(void *)((char *)data+curr),(void *)((char *)rec));
        		//cout<<curr<<"\n";
        	}
        	free(rec);
        	return 0;
        }



BNLJoin::BNLJoin(Iterator *leftIn,TableScan *rightIn,const Condition &condition, const unsigned numPages)
{
	empty=false;
	left=leftIn;
	right=rightIn;
	cond=condition;
	leftIn->getAttributes(rdl);
	rightIn->getAttributes(rdr);
	ldata=malloc(PAGE_SIZE);
	if(leftIn->getNextTuple(ldata)==-1)
		empty=true;
}

bool BNLJoin::satisfiesCondition(void *ldata,void *rdata)
{
	//return true;
	int indL=0,indR=0;
		for(indL=0;indL<rdl.size();indL++)
		{
			if(rdl[indL].name==cond.lhsAttr)
				break;
		}
		for(indR=0;indR<rdr.size();indR++)
		{
			if(rdr[indR].name==cond.rhsAttr)
				break;
		}
		void *lhs=malloc(PAGE_SIZE);
		void *rhs=malloc(PAGE_SIZE);
			//cout<<indL<<"\n";
		AttrFromRecord(rdl,cond.lhsAttr,lhs,ldata);
		if(cond.bRhsIsAttr)
			AttrFromRecord(rdr,cond.rhsAttr,rhs,rdata);
		else
		{
			int l=0;
			if(rdl[indL].type==0)
				l=sizeof(int);
			if(rdl[indL].type==1)
				l=sizeof(float);
			if(rdl[indL].type==2)
				l=*(int *)cond.rhsValue.data+sizeof(int);
			rhs=memcpy(rhs,cond.rhsValue.data,l);
		}
		//return true;
		bool res= satisfiesCompOp(cond.op,rhs,lhs,rdl[indL].type);
		free(lhs);
		free(rhs);
		return res;
}

RC BNLJoin::getNextTuple(void *data)
{
	if(empty==true)
		return -1;
	void *data1=malloc(PAGE_SIZE);
	do
	{
	while(right->getNextTuple(data1)!=-1)
	{
		if(satisfiesCondition(ldata,data1))
		{
			int offset=recordLen(ldata,0,rdl,rdl.size());
			memcpy(data,ldata,offset);
			int l=recordLen(data1,0,rdr,rdr.size());
			memcpy((char *)data+offset,data1,l);
			free(data1);
			return 0;
		}
	}
	(*right).setIterator();
	}while(left->getNextTuple(ldata)!=-1);
	free(data1);
	return -1;
}



Aggregate ::Aggregate(Iterator *input,          // Iterator of input R
        Attribute aggAttr,        // The attribute over which we are computing an aggregate
        AggregateOp op)
{
	inputAggr = input;
	aggrOp = op;
	AggrAttr = aggAttr;
	keyint = 0;
	keyreal = 0;
	count = 0;
	finishget = false;
	agggroupAttr.name = "";
	finishgroup = false;


}

RC Aggregate::getNextTuple(void *data)
{
	vector<Attribute> attrs;
	inputAggr->getAttributes(attrs);
	int offset = 0;
	int varcharLength = 0;
	int num = 0;
if(agggroupAttr.name=="")
{

	for(num =0;num<attrs.size();num++)
		{

			if(attrs[num].name==AggrAttr.name)
				{
				//	num = num;
					break;
				}

			switch(attrs[num].type)
			{
				case 0:
				{
					int offinital = offset;
					offset = offset+4;
					int integ;
					memcpy(&integ,(char*)data+1+offinital,sizeof(int));

					break;
				}
				case 1:
				{

					int offinital = offset;
					offset = offset+4;
					int real;
					memcpy(&real,(char*)data+1+offinital,sizeof(int));

					break;

				}
				case 2:
				{

					memcpy(&varcharLength,(char*)data+1+offset,sizeof(int));
			//		cout<<"varchhhhhhhhhhh"<<varcharLength<<endl;
					offset=offset+4;
					void* var = malloc(varcharLength);
					memcpy(var,(char*)data+1+offset,varcharLength);
					offset = offset+varcharLength;
				//	cout<<"varrrrrrrrrrr "<<(char*)var<<endl;
				}
				defult: break;
			}

		}
	void* dataTuple = malloc(200);
	memset(dataTuple,0,200);
	while(inputAggr->getNextTuple(dataTuple)!=-1)
	{

		switch(AggrAttr.type)
		{
		case TypeInt:
			{
				count++;
				int dataint;
				memcpy(&dataint,(char*)dataTuple+1+offset,4);
				switch(aggrOp)
						{
							case MIN:
							{

								cout<<"enterrrr"<<endl;
								if(keyint>dataint)
									keyint = dataint;
								break;
							}
							case MAX:
							{
								cout<<"keyint"<<keyint<<endl;
								if(keyint<dataint)
									keyint = dataint;
								break;
							}
							case COUNT:
							{
								keyint = keyint +1;
								//cout<<"keyint"<<keyint<<endl;
								break;
							}
							case SUM:
							{
								keyint = keyint + dataint;
								break;
							}
							case AVG:
							{
								cout<<"keyint"<<keyint<<"count"<<count<<endl;
								keyint = (keyint*(count-1)+dataint)/count;//!!!float->int flase
								break;
							}

						}
				float a;
				a = (float)keyint;
		//		cout<<"aaaaaa"<<a<<endl;
				memcpy((char*)data+1,&a,4);
				break;
			}
		case TypeReal:
			{
				count++;
				float datareal;
				memcpy(&datareal,(char*)dataTuple+1,4);
				switch(aggrOp)
						{
							case MIN:
							{

								if(keyint>datareal)
									keyreal = datareal;
								break;
							}
							case MAX:
							{
								break;
							}
							case COUNT:
							{
								break;
							}
							case SUM:
							{
								break;
							}
							case AVG:
							{
								break;
							}

						}
				break;
			}
		case TypeVarChar:
			{
				count++;
				float datareal;
				memcpy(&datareal,(char*)dataTuple+1,4);
				switch(aggrOp)
						{
							case MIN:
							{

								if(keyint>datareal)
									keyreal = datareal;
								break;
							}
							case MAX:
							{
								break;
							}
							case COUNT:
							{
								break;
							}
							case SUM:
							{
								break;
							}
							case AVG:
							{
								break;
							}

						}
				break;
			}
		}

	}


	if(finishget==false&&inputAggr->getNextTuple(dataTuple)==-1)
	{
		finishget = true;

			return 0;

	}

}
else////////////////////////////GROUP BY !!!!!!!!!!
{

	int offsetgroup = 0;

	for(num =0;num<attrs.size();num++)
		{

			if(attrs[num].name==AggrAttr.name)
				{
				//	num = num;
					break;
				}

			switch(attrs[num].type)
			{
				case 0:
				{
					int offinital = offset;
					offset = offset+4;
					int integ;
					memcpy(&integ,(char*)data+1+offinital,sizeof(int));

					break;
				}
				case 1:
				{

					int offinital = offset;
					offset = offset+4;
					int real;
					memcpy(&real,(char*)data+1+offinital,sizeof(int));

					break;

				}
				case 2:
				{

					memcpy(&varcharLength,(char*)data+1+offset,sizeof(int));
			//		cout<<"varchhhhhhhhhhh"<<varcharLength<<endl;
					offset=offset+4;
					void* var = malloc(varcharLength);
					memcpy(var,(char*)data+1+offset,varcharLength);
					offset = offset+varcharLength;
				//	cout<<"varrrrrrrrrrr "<<(char*)var<<endl;
				}
			//	defult: break;
			}

		}


	for(num =0;num<attrs.size();num++)
		{

			if(attrs[num].name==agggroupAttr.name)
				{
				//	num = num;
					break;
				}

			switch(attrs[num].type)
			{
				case 0:
				{
					int offinital = offsetgroup;
					offsetgroup = offsetgroup+4;
					int integ;
					memcpy(&integ,(char*)data+1+offinital,sizeof(int));

					break;
				}
				case 1:
				{

					int offinital = offsetgroup;
					offsetgroup = offsetgroup+4;
					int real;
					memcpy(&real,(char*)data+1+offinital,sizeof(int));

					break;

				}
				case 2:
				{

					memcpy(&varcharLength,(char*)data+1+offsetgroup,sizeof(int));
			//		cout<<"varchhhhhhhhhhh"<<varcharLength<<endl;
					offsetgroup=offsetgroup+4;
					void* var = malloc(varcharLength);
					memcpy(var,(char*)data+1+offsetgroup,varcharLength);
					offsetgroup = offsetgroup+varcharLength;
				//	cout<<"varrrrrrrrrrr "<<(char*)var<<endl;
				}
			//	defult: break;
			}

		}
//	cout<<"offset"<<offset<<endl;
//	cout<<"gtoupbyoffset"<<offsetgroup<<endl;
	void* dataTuple = malloc(200);
	void* groupB = malloc(200);
	memset(groupB,0,200);
	memset(dataTuple,0,200);
/*	vector<int> groInt;
	while(inputAggr->getNextTuple(groupB)!=-1)
	{

		switch(agggroupAttr.type)
		{
			case TypeInt:
			{

				int dataintgro;
				int judge = 0;
				memcpy(&dataintgro,(char*)groupB+1+offsetgroup,4);
			//	cout<<"intaa"<<groInt.size()<<endl;
				int a = 0;
				int b = groInt.size();
				for(;a<=b-1;a++)
				{
					if(dataintgro==groInt[a])
					{
						judge = 1;//haved
					}
				}

				if(judge==0)
				{
					groInt.push_back(dataintgro);
					cout<<"groInt.push_back(dataintgro)"<<groInt.size()<<endl;
				}
	//			b = groInt.size();
			//	cout<<"------------                "<<endl;
	//			for(a=0;a<=b-1;a++)
	//							{
	//				cout<<"dddddddddddddddd------------------"<<groInt[a]<<endl;
	//							}
				break;
			}
			case TypeReal:
			{
				break;
			}
			case TypeVarChar:
			{
				break;
			}
		}
	cout<<"aa"<<endl;
	}

	cout<<"finishsssssssssssssssssss"<<endl;
	*/
	switch(aggrOp)
	{
		case MIN:
		{
				keyint = 1000;
			break;
		}
		case MAX:
		{
				keyint = 0;
			break;
		}
		case COUNT:
		{
			keyint = 0;
			//cout<<"keyint"<<keyint<<endl;
			break;
		}
		case SUM:
		{
			keyint = 0;
			cout<<"keyint"<<keyint<<endl;
			break;
		}
		case AVG:
		{

			keyint = 0;//!!!float->int flase
			break;
		}

	}


	while(groInt.size()>0)
	{
	//	cout<<"enter groInt.size()==0"<<endl;
	//	cout<<"groInt[groInt.size()-1]"<<groInt.size()<<endl;
	//	void* dat = malloc(200);
		inputAggr->setIterator();
	//	cout<<"inputAggr->getNextTuple(dataTuple)  aaa"<<	inputAggr->getNextTuple(dataTuple)<<endl;
	while(inputAggr->getNextTuple(dataTuple)!=-1)
	{
	//	cout<<"inputAggr->getNextTuple(dataTuple)!=-1"<<endl;
		int dataintgro;
	//	cout<<"groInt[groInt.size()-1]"<<groInt.size()<<endl;
		memcpy(&dataintgro,(char*)dataTuple+1+offsetgroup,4);
//		cout<<"dataint"<<dataintgro<<endl;

//		cout<<"offset"<<offset<<endl;
//		cout<<"offsetgroup"<<offsetgroup<<endl;
		int keyvalue;
		memcpy(&keyvalue,(char*)dataTuple+1+offset,4);
//		cout<<"keyvalue"<<keyvalue<<endl;
	//	keyint=keyvalue;
		if(dataintgro==groInt[groInt.size()-1])
		{
			cout<<"dataint"<<dataintgro<<endl;
//			cout<<"enter if---------------------------------------------------"<<endl;
			switch(AggrAttr.type)
					{
					case TypeInt:
						{
							count++;
							int dataint;
							memcpy(&dataint,(char*)dataTuple+1+offset,4);
//							cout<<"dataint"<<dataint<<endl;
							switch(aggrOp)
									{
										case MIN:
										{


											if(keyint>dataint)
												keyint = (float)dataint;
											cout<<"keyintmin"<<keyint<<endl;
											break;
										}
										case MAX:
										{
											cout<<"keyint"<<keyint<<endl;
											if(keyint<dataint)
												keyint = dataint;
											break;
										}
										case COUNT:
										{
											keyint = keyint +1;
											//cout<<"keyint"<<keyint<<endl;
											break;
										}
										case SUM:
										{
											keyint = keyint + dataint;
											cout<<"keyint"<<keyint<<endl;
											break;
										}
										case AVG:
										{
											cout<<"keyint"<<keyint<<"count"<<count<<endl;
											keyint = (keyint*(count-1)+dataint)/count;//!!!float->int flase
											break;
										}

									}
							float agro;
							agro = (float)keyint;
					//		cout<<"aaaaaa"<<a<<endl;
							memcpy((char*)data+1,&dataintgro,4);
							memcpy((char*)data+5,&agro,4);

							break;
						}
					case TypeReal:
						{
							count++;
							float datareal;
							memcpy(&datareal,(char*)dataTuple+1,4);
							switch(aggrOp)
									{
										case MIN:
										{

											if(keyint>datareal)
												keyreal = datareal;
											break;
										}
										case MAX:
										{
											break;
										}
										case COUNT:
										{
											break;
										}
										case SUM:
										{
											break;
										}
										case AVG:
										{
											break;
										}

									}
							break;
						}
					case TypeVarChar:
						{
							count++;
							float datareal;
							memcpy(&datareal,(char*)dataTuple+1,4);
							switch(aggrOp)
									{
										case MIN:
										{

											if(keyint>datareal)
												keyreal = datareal;
											break;
										}
										case MAX:
										{
											break;
										}
										case COUNT:
										{
											break;
										}
										case SUM:
										{
											break;
										}
										case AVG:
										{
											break;
										}

									}
							break;
						}
					}
		}


	}
	cout<<"----------------------------------------finish one"<<endl;
//end while
	groInt.erase(groInt.begin()+groInt.size()-1);
	cout<<"---------------------------------sizeafter-------finish one"<<groInt.size()<<endl;
	break;


	}
	//end search

	if(groInt.size()>=0&&inputAggr->getNextTuple(dataTuple)==-1)
	{
		inputAggr->setIterator();

			return 0;

	}

//	cout<<"inputAggr->getNextTuple(dataTuple)"<<inputAggr->getNextTuple(dataTuple)<<endl;

}
	return -1;

}

Aggregate:: Aggregate(Iterator *input,             // Iterator of input R
          Attribute aggAttr,           // The attribute over which we are computing an aggregate
          Attribute groupAttr,         // The attribute over which we are grouping the tuples
          AggregateOp op              // Aggregate operation
)
{

	inputAggr = input;
	aggrOp = op;
	AggrAttr = aggAttr;
	keyint = 0;
	keyreal = 0;
	count = 0;
	finishget = false;
	agggroupAttr = groupAttr;
	finishgroup = false;


	int offsetgroup = 0;

	vector<Attribute> attrs;
	inputAggr->getAttributes(attrs);
	int offset = 0;
	int varcharLength = 0;
	int num = 0;
	for(num =0;num<attrs.size();num++)
		{

			if(attrs[num].name==AggrAttr.name)
				{
				//	num = num;
					break;
				}

			switch(attrs[num].type)
			{
				case 0:
				{
					int offinital = offset;
					offset = offset+4;
					int integ;


					break;
				}
				case 1:
				{

					int offinital = offset;
					offset = offset+4;
					int real;


					break;

				}
				case 2:
				{


			//		cout<<"varchhhhhhhhhhh"<<varcharLength<<endl;
					offset=offset+4;
					void* var = malloc(varcharLength);

					offset = offset+varcharLength;
				//	cout<<"varrrrrrrrrrr "<<(char*)var<<endl;
				}
			//	defult: break;
			}

		}


	for(num =0;num<attrs.size();num++)
		{

			if(attrs[num].name==agggroupAttr.name)
				{
				//	num = num;
					break;
				}

			switch(attrs[num].type)
			{
				case 0:
				{
					int offinital = offsetgroup;
					offsetgroup = offsetgroup+4;
					int integ;


					break;
				}
				case 1:
				{

					int offinital = offsetgroup;
					offsetgroup = offsetgroup+4;
					int real;


					break;

				}
				case 2:
				{


			//		cout<<"varchhhhhhhhhhh"<<varcharLength<<endl;
					offsetgroup=offsetgroup+4;
					void* var = malloc(varcharLength);

					offsetgroup = offsetgroup+varcharLength;
				//	cout<<"varrrrrrrrrrr "<<(char*)var<<endl;
				}
			//	defult: break;
			}

		}
//	cout<<"offset"<<offset<<endl;
//	cout<<"gtoupbyoffset"<<offsetgroup<<endl;
	void* dataTuple = malloc(200);
	void* groupB = malloc(200);
	memset(groupB,0,200);
	memset(dataTuple,0,200);

	while(inputAggr->getNextTuple(groupB)!=-1)
	{

		switch(agggroupAttr.type)
		{
			case TypeInt:
			{

				int dataintgro;
				int judge = 0;
				memcpy(&dataintgro,(char*)groupB+1+offsetgroup,4);
			//	cout<<"intaa"<<groInt.size()<<endl;
				int a = 0;
				int b = groInt.size();
				for(;a<=b-1;a++)
				{
					if(dataintgro==groInt[a])
					{
						judge = 1;//haved
					}
				}

				if(judge==0)
				{
					groInt.push_back(dataintgro);
					cout<<"groInt.push_back(dataintgro)"<<groInt.size()<<endl;
				}
	//			b = groInt.size();
			//	cout<<"------------                "<<endl;
	//			for(a=0;a<=b-1;a++)
	//							{
	//				cout<<"dddddddddddddddd------------------"<<groInt[a]<<endl;
	//							}
				break;
			}
			case TypeReal:
			{
				break;
			}
			case TypeVarChar:
			{
				break;
			}
		}
//	cout<<"aa"<<endl;
	}

	cout<<"finishsssssssssssssssssss inital"<<endl;


}

/*
bool Aggregate::satisfiesCondition(void *data,AggregateOp op,Iterator *input)
{

	vector <Attribute> rd;
	input->getAttributes(rd);
	int indL=0,indR=0;
	int offset = 0;
	int varcharLength = 0;
	cout<<"rd.size()"<<rd.size()<<endl;
//	cout<<"cond.lhsAttr"<<cond.lhsAttr<<endl;
	int num = 0;
	for(indL=0;indL<rd.size();indL++)
	{
//		cout<<"rd1].name"<<rd[1].name<<endl;



		switch(rd[indL].type)
		{
			case 0:
			{
				int offinital = offset;
				offset = offset+4;
				int integ;
				memcpy(&integ,(char*)data+1+offinital,sizeof(int));
		//		cout<<"integ"<<integ<<endl;

				break;
			}
			case 1:
			{

				int offinital = offset;
				offset = offset+4;
				int real;
				memcpy(&real,(char*)data+1+offinital,sizeof(int));
		//		cout<<"real"<<real<<endl;

				break;

			}
			case 2:
			{

				memcpy(&varcharLength,(char*)data+1+offset,sizeof(int));
		//		cout<<"varchhhhhhhhhhh"<<varcharLength<<endl;
				offset=offset+4;
				void* var = malloc(varcharLength);
				memcpy(var,(char*)data+1+offset,varcharLength);
				offset = offset+varcharLength;
			//	cout<<"varrrrrrrrrrr "<<(char*)var<<endl;
			}
			defult: break;
		}
	//	cout<<"rdtype"<<rd[indL].type<<endl;
	//	cout<<"offset"<<offset<<endl;
	//	cout<<"rdname"<<rd[indL].name<<endl;
	//	cout<<"cond.lhaAttr"<<cond.lhsAttr<<endl;
		if(rd[indL].name==cond.lhsAttr)
		{
			num = indL;
			break;
		}
	}
	for(indR=0;indR<rd.size();indR++)
	{
		if(rd[indR].name==cond.rhsAttr)
			break;
	}

	bool res= satisfiesCompOp(cond.op,rhs,lhs,rd[indL].type);
	free(lhs);
	free(rhs);
	return res;
}
*/

/*

INLJoin::INLJoin(Iterator *leftIn,IndexScan *rightIn, const Condition &condition)
{

	this->leftInput=leftIn;
	this->rightInput=rightIn;
	this->condition=condition;
	this->flag=1;

	leftInput->getAttributes(leftAttrs);
	rightInput->getAttributes(rightAttrs);


}



RC INLJoin::getNextTuple(void* data)
{
	leftData=malloc(1000);
	rightData=malloc(1000);
	memset(leftData,0,1000);
	memset(rightData,0,1000);


	while(true){
		if(flag==0){
			if(leftInput->getNextTuple(leftData)!=0)
			{
				free(leftData);
				free(rightData);
				return QE_EOF;

			}
		}

		while(rightInput->getNextTuple(rightData)==0)
		{
			if(compareTuple(leftData,rightData)==0)
			{
				joinTuple(leftAttrs,rightAttrs,leftData,rightData,data);
				flag=1;
				free(rightData);
				free(leftData);
				return 0;
			}
		}
		flag=0;
		rightInput->setIterator(NULL,NULL,true,true);
	}
	return QE_EOF;

}

RC INLJoin::compareTuple(void* leftData, void*rightData)
{

	leftAttrs.clear();
	rightAttrs.clear();
	leftInput->getAttributes(leftAttrs);
	rightInput->getAttributes(rightAttrs);
	Value leftValue;
	Value rightValue;
	int leftOffset=ceil((double) leftAttrs.size() / 8.0);
	int rightOffset=ceil((double) rightAttrs.size() / 8.0);

//get the left hand tuple`s value
	for(unsigned int i=0;i<leftAttrs.size();i++)
	{

			leftValue.data=malloc(1000);
			switch(leftAttrs[i].type)
			{
			case TypeInt:{
				if(strcmp(leftAttrs[i].name.c_str(),condition.lhsAttr.c_str())==0){
					leftValue.type=leftAttrs[i].type;
					memcpy(leftValue.data,(char*)leftData+leftOffset,4);
				}
				leftOffset+=4;
				break;
			}

			case TypeReal:{
				if(strcmp(leftAttrs[i].name.c_str(),condition.lhsAttr.c_str())==0){
					leftValue.type=leftAttrs[i].type;
					memcpy(leftValue.data,(char*)leftData+leftOffset,4);

				}
				leftOffset+=4;
				break;

			}

			case TypeVarChar:{
				int varcharLength;
				memcpy(&varcharLength,(char*)leftData+leftOffset,4);
				leftOffset+=4;
				if(strcmp(leftAttrs[i].name.c_str(),condition.lhsAttr.c_str())==0){
					leftValue.type=leftAttrs[i].type;
					memcpy(leftValue.data,(char*)leftData+leftOffset,varcharLength);
				}
				leftOffset+=varcharLength;

				break;
			}
			break;
			}

	}


	//get right hand tuple`s value
	for(unsigned int i=0;i<rightAttrs.size();i++)
	{

		rightValue.data=malloc(1000);
			switch(rightAttrs[i].type)
			{
			case TypeInt:{
				if(strcmp(rightAttrs[i].name.c_str(),condition.rhsAttr.c_str())==0){
					rightValue.type=rightAttrs[i].type;
					memcpy(rightValue.data,(char*)rightData+rightOffset,4);
				}
				rightOffset+=4;
				break;
			}

			case TypeReal:{
				if(strcmp(rightAttrs[i].name.c_str(),condition.rhsAttr.c_str())==0){
					rightValue.type=rightAttrs[i].type;
					memcpy(rightValue.data,(char*)rightData+rightOffset,4);

				}
				rightOffset+=4;
				break;

			}

			case TypeVarChar:{
				int varcharLength;
				memcpy(&varcharLength,(char*)rightData+rightOffset,4);
				rightOffset+=4;
				if(strcmp(rightAttrs[i].name.c_str(),condition.rhsAttr.c_str())==0){
					rightValue.type=rightAttrs[i].type;
					memcpy(rightValue.data,(char*)rightData+rightOffset,varcharLength);
				}
				rightOffset+=varcharLength;

				break;
			}
			break;
			}

	}

	//check if leftValue==rightValue
	switch(leftValue.type){
	case TypeInt:{
		if(*(int*)leftValue.data==*(int*)rightValue.data) return 0;
		break;
	}
	case TypeReal:{
		if(*(float*)leftValue.data==*(float*)rightValue.data) return 0;
		break;
	}

	case TypeVarChar:{
		string leftStr=(char*)leftValue.data;
		string rightStr=(char*)rightValue.data;
		if(strcmp(leftStr.c_str(),rightStr.c_str())==0) return 0;
		break;

	}
	}

	return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	vector<Attribute> lAttrs;
	vector<Attribute> rAttrs;
	lAttrs.clear();
	rAttrs.clear();
	leftInput->getAttributes(lAttrs);
	rightInput->getAttributes(rAttrs);
	for(unsigned int i=0;i<lAttrs.size();i++){
		attrs.push_back(lAttrs[i]);
	}
	for(unsigned int i=0;i<rAttrs.size();i++){
		attrs.push_back(rAttrs[i]);
	}
}
*/

RC Iterator::joinTuple(const vector<Attribute> &leftAttrs, const vector<Attribute> & rightAttrs, const void* leftData, const void* rightData, void* joinedData)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	int leftLength=rbfm->calculateFieldsSize(leftAttrs,leftData);
	int rightLength=rbfm->calculateFieldsSize(rightAttrs,rightData);

	memset(joinedData,0,leftLength+rightLength);
	memcpy((char*)joinedData,leftData,leftLength);
	memcpy((char*)joinedData+leftLength,(char*)rightData,rightLength);

	return 0;
}


//////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////
////////////////////////////  Index nest loop join////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////

INLJoin::INLJoin(Iterator *leftIn,IndexScan *rightIn,const Condition &condition)
{
	    empty=false;
		left=leftIn;
		right=rightIn;
		cond=condition;
		leftIn->getAttributes(rdl);
		rightIn->getAttributes(rdr);
		ldata=malloc(PAGE_SIZE);
		if(leftIn->getNextTuple(ldata)==-1)  empty=true;
}

void INLJoin::setCondition()
{
	void *lhs=malloc(PAGE_SIZE);
	void *rhs=malloc(PAGE_SIZE);
	AttrFromRecord(rdl,cond.lhsAttr,lhs,ldata);
	if(cond.op==LT_OP||cond.op==LE_OP)
		right->setIterator(lhs,NULL,true,true);
	if(cond.op==GT_OP||cond.op==GE_OP)
		right->setIterator(NULL,lhs,true,true);
	if(cond.op==EQ_OP)
		right->setIterator(lhs,lhs,true,true);
}

RC INLJoin::getNextTuple(void *data)
{
	if(empty==true)
				return -1;
			void *data1=malloc(PAGE_SIZE);
			while(true)
			{
			while(right->getNextTuple(data1)!=-1)
			{
				if(satisfiesCondition(ldata,data1))
				{
					int offset=recordLen(ldata,0,rdl,rdl.size());
					memcpy(data,ldata,offset);
					int l=recordLen(data1,0,rdr,rdr.size());
					memcpy((char *)data+offset,data1,l);
					free(data1);
					return 0;
				}
			}
			//(*right).setIterator(NULL,NULL,true,true);
			if(left->getNextTuple(ldata)!=-1)
				setCondition();
			else
				break;
			}
			free(data1);
			return -1;
}



bool INLJoin::satisfiesCondition(void *ldata,void *rdata)
{
	//return true;
	int indL=0,indR=0;
		for(indL=0;indL<rdl.size();indL++)
		{
			if(rdl[indL].name==cond.lhsAttr)
				break;
		}
		for(indR=0;indR<rdr.size();indR++)
		{
			if(rdr[indR].name==cond.rhsAttr)
				break;
		}
		void *lhs=malloc(PAGE_SIZE);
		void *rhs=malloc(PAGE_SIZE);
			//cout<<indL<<"\n";
		AttrFromRecord(rdl,cond.lhsAttr,lhs,ldata);
		if(cond.bRhsIsAttr)
			AttrFromRecord(rdr,cond.rhsAttr,rhs,rdata);
		else
		{
			int l=0;
			if(rdl[indL].type==0)
				l=sizeof(int);
			if(rdl[indL].type==1)
				l=sizeof(float);
			if(rdl[indL].type==2)
				l=*(int *)cond.rhsValue.data+sizeof(int);
			rhs=memcpy(rhs,cond.rhsValue.data,l);
		}
		//return true;
		bool res= satisfiesCompOp(cond.op,rhs,lhs,rdl[indL].type);
		free(lhs);
		free(rhs);
		return res;
}






