#include "dt_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include "dt_lib.h"
#include "zlib.h"
#include "dtio.h"
#include <sys/wait.h>
#define COMPRESSLEVEL 5

#define MAX_DUMPIDXBYTES	1024*1024
#define	SRCBUFLEN 2500000
#define DSTBUFLEN 2500000
#define PREPPARE_ONLINE_DBNAME "preponl"
#define FORDELETE_DBNAME "fordelete"
// 2005/08/27修改，partid等效于sub	

//固定为1MB
#define FIX_MAXINDEXRN 1*1024*1024

	extern char errfile[];
	extern char lgfile[];
// 为保持wdbi库的连续运行(dtadmin单独升级),TestColumn在此实现。理想位置是到wdbi库实现
bool TestColumn(int mt,const char *colname)
{
	int colct=wociGetColumnNumber(mt);
	char tmp[300];
	for(int i=0;i<colct;i++) {
		wociGetColumnName(mt,i,tmp);
		if(STRICMP(colname,tmp)==0) return true;
	}
	return false;
}

extern bool wdbi_kill_in_progress;
int CopyMySQLTable(const char *path,const char *sdn,const char *stn,
				   const char *ddn,const char *dtn)
{
	char srcf[300],dstf[300];
	// check destination directory
	sprintf(dstf,"%s%s",path,ddn);
	xmkdir(dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".frm");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".frm");
	mCopyFile(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYD");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYD");
	mCopyFile(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYI");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYI");
	mCopyFile(srcf,dstf);
	
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".DTP");
	{
		FILE *fsrc=fopen(srcf,"rb");
		if(fsrc!=NULL) {
			fclose(fsrc) ;
			sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".DTP");
			mCopyFile(srcf,dstf);
		}	
	}
	return 1;
}

int MoveMySQLTable(const char *path,const char *sdn,const char *stn,
				   const char *ddn,const char *dtn)
{
	char srcf[300],dstf[300];
	// check destination directory
	sprintf(dstf,"%s%s",path,ddn);
	xmkdir(dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".frm");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".frm");
	rename(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYD");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYD");
	rename(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYI");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYI");
	rename(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".DTP");
	{
		FILE *fsrc=fopen(srcf,"rb");
		if(fsrc!=NULL) {
			fclose(fsrc) ;
			sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".DTP");
			rename(srcf,dstf);
		}
	}
	return 1;
}


long StatMySQLTable(const char *path,const char *fulltbn)
{
	char srcf[300],fn[300];
	strcpy(fn,fulltbn);
	char *psep=strstr(fn,".");
	if(psep==NULL) 
		  ThrowWith("Invalid table name format'%s',should be dbname.tbname.",fn);
	  *psep='/';
	struct stat st;
	long rt=0;
	// check destination directory
	sprintf(srcf,"%s%s%s",path,fn,".frm");
	stat(srcf,&st);
	rt+=st.st_size;
	sprintf(srcf,"%s%s%s",path,fn,".MYD");
	stat(srcf,&st);
	rt+=st.st_size;
	sprintf(srcf,"%s%s%s",path,fn,".MYI");
	stat(srcf,&st);
	rt+=st.st_size;
	return rt;
}

/*
int CreateMtFromFile(int maxrows,char *filename)
{
FILE *fp=fopen(filename,"rb");
if(!fp)
ThrowWith("CreateMt from file '%s' which could not open.",filename);
int cdlen=0,cdnum=0;
fread(&cdlen,sizeof(int),1,fp);
fread(&cdnum,sizeof(int),1,fp);
revInt(&cdnum);
revInt(&cdlen);
if(cdlen==0 || cdnum==0)
ThrowWith("Could not read columns info from file 's' !",filename);
char *pbf=new char[cdlen];
if(fread(pbf,1,cdlen,fp)!=cdlen) {
delete [] pbf;
ThrowWith("Could not read columns info from file 's' !",filename);
}
int mt=wociCreateMemTable();
wociImport(mt,NULL,0,pbf,cdnum,cdlen,maxrows,0);
delete []pbf;
return mt;
}
*/



void SysAdmin::CreateDT(int tabid) 
{
	wociSetTraceFile("cdt建立目标表结构/");
	lgprintf("解析源表,重构表结构,目标表编号%d.",tabid);
	int tabp=wociSearchIK(dt_table,tabid);
	
	int srcid=dt_table.GetInt("sysid",tabp);
	int srcidp=wociSearchIK(dt_srcsys,srcid);
	if(srcidp<0)
	  ThrowWith( "找不到源系统连接配置,表%d,源系统号%d",tabid,srcid) ;
	//建立到数据源的连接
	AutoHandle srcsys;
	srcsys.SetHandle(BuildDBC(srcidp));
	//构造源数据的内存表结构
	AutoMt srcmt(srcsys,0);
	srcmt.SetHandle(GetSrcTableStructMt(tabp,srcsys));
	if(wociGetRowLen(srcmt)<1) 
		ThrowWith( "源表解析错误,记录长度为%d",wociGetRowLen(srcmt)) ;
	char tbname[150],idxname[150];
	GetTableName(tabid,-1,tbname,idxname,TBNAME_DEST);
	int colct=srcmt.GetColumnNum();
	#define WDBI_DEFAULT_NUMLEN 16
	#define WDBI_DEFAULT_SCALE  2
	for(int ci=0;ci<colct;ci++) {
	  if(wociGetColumnType(srcmt,ci)==COLUMN_TYPE_NUM) {
	   if(wociGetColumnDataLenByPos(srcmt,ci)==WDBI_DEFAULT_NUMLEN && wociGetColumnScale(srcmt,ci)==WDBI_DEFAULT_SCALE) {
	    char sql_st[4000];
	    wociGetCreateTableSQL(srcmt,sql_st,tbname,false);
	    char *psub;
	    while(psub=strstr(sql_st,"16,2")) memcpy(psub,"????",4);
	    ThrowWith( "源表解析错误,一些数值字段缺乏明确的长度定义,请使用格式表重新配置.参考建表语句: \n %s .",sql_st) ;
	   }
	  }
	}
		
	lgprintf("重建目标表结构（CreateDP)以前需要禁止对表的访问.由于数据或索引结构可能有变化,因此在结构重建后不做数据恢复.");
	lgprintf("记录数清零...");
	CloseTable(tabid,tbname,true);
	CreateTableOnMysql(srcmt,tbname,true);
	CreateAllIndexTable(tabid,srcmt,TBNAME_DEST,true);
	char sqlbf[300];
	sprintf(sqlbf,"update dp.dp_table set cdfileid=1 , recordlen=%d where tabid=%d",
		wociGetRowLen(srcmt), tabid);
	DoQuery(sqlbf);
	log(tabid,0,100,"结构重建:字段数%d,记录长度%d字节.",colct,wociGetRowLen(srcmt));
}


DataDump::DataDump(int dtdbc,int maxmem,int _blocksize):fnmt(dtdbc,300)
{
	this->dtdbc=dtdbc;
	//Create fnmt and build column structrues.
	//fnmt.FetchAll("select * from dt_middledatafile where rownum<1");
	fnmt.FetchAll("select * from dp.dp_middledatafile limit 3");
	fnmt.Wait();
	fnmt.Reset();
	indexid=0;
	memlimit=maxmem*1024*1000;
	maxblockrn=0;
	blocksize=_blocksize*1024;
}

int DataDump::BuildMiddleFilePath(int _fid) {
	int fid=_fid;
	sprintf(tmpfn,"%smddt_%d.dat",dp.tmppath[0]
		,fid);
	sprintf(tmpidxfn,"%smdidx_%d.dat",dp.tmppath[0],
		fid);
	while(true) {
		int freem=GetFreeM(tmpfn);
		if(freem<1024) {
			lgprintf("Available space on hard disk('%s') less then 1G : %dM,waitting 5 minutes for free...",tmpfn,freem);
			mSleep(300000);
		}
		else break;
	}
	return fid;
}

void DataDump::ProcBlock(int partid,AutoMt *pCur,int idx,AutoMt &blockmt,int _fid)
{
	freeinfo1("Start ProcBlock");
	int fid=BuildMiddleFilePath(_fid);
	blockmt.Reset();
	int cur=*pCur;
	char *idxcolsname=dp.idxp[idx].idxcolsname;
	int *ikptr=NULL;
	int strow=-1;
	int subrn=0;
	int blockrn=0;
	if(maxblockrn<MIN_BLOCKRN) ThrowWith("目标表的数据块大小(%d)不合适，至少为%d.",maxblockrn,MIN_BLOCKRN);
	if(maxblockrn>MAX_BLOCKRN) ThrowWith("目标表的数据块大小(%d)不合适，不能超过%d.",maxblockrn,MAX_BLOCKRN);
	AutoMt idxdt(0,10);
	wociCopyColumnDefine(idxdt,cur,idxcolsname);
	wociAddColumn(idxdt,"idx_blockoffset","",COLUMN_TYPE_INT,0,0);
	//	wociAddColumn(idxdt,"idx_storesize","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_startrow","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_rownum","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_fid","",COLUMN_TYPE_INT,0,0);
	int idxrnlmt=max(FIX_MAXINDEXRN/wociGetRowLen(idxdt),2);
	idxdt.SetMaxRows(idxrnlmt);
	idxdt.Build();
	freeinfo1("After Build indxdt mt");
	void *idxptr[20];
	int pidxc1[10];
	bool pkmode=false;
	sorttm.Start();
	int cn1=wociConvertColStrToInt(cur,idxcolsname,pidxc1);
	//屏蔽PK模式，全部按普通模式处理
	//if(cn1==1 && wociGetColumnType(cur,pidxc1[0])==COLUMN_TYPE_INT)
	//	pkmode=true;
	if(!pkmode) {
		wociSetSortColumn(cur,idxcolsname);
		wociSortHeap(cur);
	}
	else {
		wociSetIKByName(cur,idxcolsname);
		wociOrderByIK(cur);
		wociGetIntAddrByName(cur,idxcolsname,0,&ikptr);
	}
	sorttm.Stop();
	int idx_blockoffset=0,idx_store_size=0,idx_startrow=0,idx_rownum=0;
	int idxc=cn1;
	idxptr[idxc++]=&idx_blockoffset;
	//	idxptr[idxc++]=&idx_store_size;
	idxptr[idxc++]=&idx_startrow;
	idxptr[idxc++]=&idx_rownum;
	idxptr[idxc++]=&fid;
	idxptr[idxc]=NULL;
	dt_file df;
	df.Open(tmpfn,1,fid);
	idx_blockoffset=df.WriteHeader(cur);
	dt_file di;
	di.Open(tmpidxfn,1);
	di.WriteHeader(idxdt,wociGetMemtableRows(idxdt));
	idxdt.Reset();
	int totidxrn=0;
	int rn=wociGetMemtableRows(cur);
	adjtm.Start();
	for(int i=0;i<rn;i++) {
		int thisrow=pkmode?wociGetRawrnByIK(cur,i):wociGetRawrnBySort(cur,i);
		//int thisrow=wociGetRawrnByIK(cur,i);
		if(strow==-1) {
			strow=thisrow;
			idx_startrow=blockrn;
		}
		//子块分割
		else 
			if(pkmode?(ikptr[strow]!=ikptr[thisrow]):
			(wociCompareSortRow(cur,strow,thisrow)!=0) ){
				//if(ikptr[strow]!=ikptr[thisrow]) {
				for(int c=0;c<cn1;c++) {
					idxptr[c]=pCur->PtrVoid(pidxc1[c],strow);
				}
				idx_rownum=blockrn-idx_startrow;
				wociInsertRows(idxdt,idxptr,NULL,1);
				totidxrn++;
				int irn=wociGetMemtableRows(idxdt);
				if(irn>idxrnlmt-2) {
					int *pbo=idxdt.PtrInt("idx_blockoffset",0);
					int pos=irn-1;
					while(pos>=0 && pbo[pos]==idx_blockoffset) pos--;
					if(pos>0) {
						di.WriteMt(idxdt,COMPRESSLEVEL,pos+1,false);
						if(pos+1<irn) 
							wociCopyRowsTo(idxdt,idxdt,0,pos+1,irn-pos-1);
						else wociReset(idxdt);
					}
					else ThrowWith("数据预处理错误,分区号%d,索引字段'%s',超过允许索引块长度%d.",partid,idxcolsname,MAX_BLOCKRN);
				}
				strow=thisrow;
				idx_startrow=blockrn;
			}
			//blockmt.QuickCopyFrom(pcur,blockrn,thisrow);
			wociCopyRowsTo(cur,blockmt,-1,thisrow,1);
			blockrn++;//=wociGetMemtableRows(blockmt);
			//块和子块的分割
			if(blockrn>=maxblockrn) {
				adjtm.Stop();
				fiotm.Start();
				for(int c=0;c<cn1;c++) {
					idxptr[c]=pCur->PtrVoid(pidxc1[c],strow);
				}
				idx_rownum=blockrn-idx_startrow;
				wociInsertRows(idxdt,idxptr,NULL,1);
				totidxrn++;
				idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL,0,false);
				idx_startrow=0;
				strow=-1;blockrn=0;
				blockmt.Reset();
				fiotm.Stop();
				adjtm.Start();
			}
	}
	adjtm.Stop();
	//保存最后的块数据
	if(blockrn) {
		for(int c=0;c<cn1;c++) {
			idxptr[c]=pCur->PtrVoid(pidxc1[c],strow);
		}
		idx_rownum=blockrn-idx_startrow;
		wociInsertRows(idxdt,idxptr,NULL,1);
		totidxrn++;
		idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL,0,false);
		idx_startrow=0;
		strow=-1;blockrn=0;
		blockmt.Reset();
	}
	//保存索引数据
	{
		di.WriteMt(idxdt,COMPRESSLEVEL,0,false);
		di.SetFileHeader(totidxrn,NULL);
	}
	//保存文件索引
	{
		void *ptr[20];
		ptr[0]=&fid;
		ptr[1]=&partid;
		ptr[2]=&dp.tabid;
		int rn=df.GetRowNum();
		int fl=df.GetLastOffset();
		ptr[3]=&rn;ptr[4]=&fl;
		char now[10];
		wociGetCurDateTime(now);
		ptr[5]=tmpfn;ptr[6]=tmpidxfn;ptr[7]=now;
		int state=1;
		ptr[8]=&state;
		char nuldt[10];
		memset(nuldt,0,10);
		ptr[9]=now;//nuldt;
		ptr[10]=&dp.idxp[idx].idxid;
		ptr[11]=dp.idxp[idx].idxcolsname;
		ptr[12]=dp.idxp[idx].idxreusecols;
		ptr[13]=NULL;
		wociInsertRows(fnmt,ptr,NULL,1);
	}
	freeinfo1("End ProcBlock");
}


int DataDump::DoDump(SysAdmin &sp) {
	int tabid=0;
	sp.GetFirstTaskID(NEWTASK,tabid,datapartid);
	if(tabid<1) return 0;
	wociSetTraceFile("dump数据导出/");
	sorttm.Clear();
	fiotm.Clear();
	adjtm.Clear();
	sp.GetSoledIndexParam(datapartid,&dp,tabid);
	sp.OutTaskDesc("执行数据导出任务: ",tabid,datapartid);
	if(xmkdir(dp.tmppath[0])) 
		ThrowWith("临时主路径无法建立,表:%d,数据组:%d,路径:%s.",
		tabid,datapartid,dp.tmppath[0]);
	AutoHandle srcdbc;
	srcdbc.SetHandle(sp.BuildSrcDBC(tabid,datapartid));
	AutoMt srctstmt(0,10);
	int partoff=0;
	{	
		//如果格式表与目标表的结构不一致
		sp.BuildMtFromSrcTable(srcdbc,tabid,&srctstmt);
		srctstmt.AddrFresh();
		int srl=wociGetRowLen(srctstmt);
		char tabname[150];
		sp.GetTableName(tabid,-1,tabname,NULL,TBNAME_DEST);
		AutoMt dsttstmt(dtdbc,10);
		dsttstmt.FetchFirst("select * from dp.dp_datapart where tabid=%d and status=5 ",tabid);
		int ndmprn=dsttstmt.Wait();
		dsttstmt.FetchFirst("select * from %s",tabname);
		int tstrn=dsttstmt.Wait();
		if(srctstmt.CompareMt(dsttstmt)!=0 ) {
			if(tstrn>0 && ndmprn>0) 
				ThrowWith("表%s中已经有数据，但源表(格式表)格式被修改，不能导入数据，请导入新的(空的)目标表中。",tabname);
			lgprintf("源表数据格式发生变化，重新解析源表... ");
			if(tstrn==0) {
				sp.CreateDT(tabid);
				sp.Reload();
				sp.GetSoledIndexParam(datapartid,&dp,tabid);
			}
			else {
				//全部数据分组重新导入数据,可以允许结构暂时不一致
				//由于目标表有数据，暂时不修改dt_table.recordlen
				dp.rowlen=srl;
			}
		}
		else if(srl!=dp.rowlen) {
			lgprintf("目标表中的记录长度错误，%d修改为%d",dp.rowlen,srl);
			wociClear(dsttstmt);
			AutoStmt &st=dsttstmt.GetStmt();
			st.Prepare("update dp.dp_table set recordlen=%d where tabid=%d",srl,tabid);
			st.Execute(1);
			st.Wait();
		}
		if(ndmprn==0 && tstrn==0) {
			//复位文件编号,如果目标表非空,则文件编号不置位
			wociClear(dsttstmt);
			AutoStmt &st=dsttstmt.GetStmt();
			st.Prepare("update dp.dp_table set lstfid=0 where tabid=%d",srl,tabid);
			st.Execute(1);
			st.Wait();
		}			
	}
	
	int realrn=memlimit/dp.rowlen;
	lgprintf("开始导出数据,数据抽取内存%d字节(折合记录数:%d)",realrn*dp.rowlen,realrn);
	sp.log(tabid,datapartid,101,"开始数据导出:数据块%d字节(记录数%d),日志文件 '%s'.",realrn*dp.rowlen,realrn,lgfile);
	//if(realrn>dp.maxrecnum) realrn=dp.maxrecnum;
	{
		lgprintf("清除上次导出的数据...");
		
		AutoMt clsmt(dtdbc,100);
		AutoStmt st(dtdbc);
		int clsrn=0;
		do {
			clsmt.FetchAll("select * from dp.dp_middledatafile where datapartid=%d and tabid=%d limit 100",datapartid,tabid);
			clsrn=clsmt.Wait();
			for(int i=0;i<clsrn;i++) {
				unlink(clsmt.PtrStr("datafilename",i));
				unlink(clsmt.PtrStr("indexfilename",i));
			}
			st.Prepare("delete from dp.dp_middledatafile where datapartid=%d and tabid=%d limit 100",datapartid,tabid);
			st.Execute(1);
			st.Wait();
		} while(clsrn>0);
	}
	//realrn=50000;
	//indexparam *ip=&dp.idxp[dp.psoledindex];
	maxblockrn=blocksize/dp.rowlen;
	{
		//在作数据导出时设置块记录数,以后的处理和查询以此为依据
		//字段maxrecinblock的使用方法变更为:程序根据后台设置的参数自动计算,管理控制台只读
		lgprintf("设置目标数据块%d字节(记录数:%d)",maxblockrn*dp.rowlen,maxblockrn);
		AutoStmt st(dtdbc);
		st.Prepare("update dp.dp_table set maxrecinblock=%d where tabid=%d",maxblockrn,dp.tabid);
		st.Execute(1);
		st.Wait();
	}
	sp.Reload();
	maxblockrn=sp.GetMaxBlockRn(tabid);
	AutoMt blockmt(0,maxblockrn);
	fnmt.Reset();
	//int partid=0;
	fnorder=0;
	try {
		sp.UpdateTaskStatus(DUMPING,tabid,datapartid);
	}
	catch(char *str) {
		lgprintf(str);
		return 0;
	}
	bool dumpcomplete=false;
	
	LONG64 srn=0;
	try {
		bool ec=wociIsEcho();
		wociSetEcho(TRUE);
		
		if(sp.GetDumpSQL(tabid,datapartid,dumpsql)!=-1) {
			//idxdt.Reset();
			TradeOffMt dtmt(0,realrn);
			blockmt.Clear();
			sp.BuildMtFromSrcTable(srcdbc,tabid,&blockmt);
			//blockmt.Build(stmt);
			sp.log(tabid,datapartid,131,"数据导出语句:%s.",dumpsql);
			blockmt.AddrFresh();
			sp.BuildMtFromSrcTable(srcdbc,tabid,dtmt.Cur());
			sp.BuildMtFromSrcTable(srcdbc,tabid,dtmt.Next());
			AutoStmt stmt(srcdbc);
			stmt.Prepare(dumpsql);
			{
			 AutoMt tstmt(0,1);
			 tstmt.Build(stmt);
			 if(blockmt.CompatibleMt(tstmt)!=0 ) 
			 	ThrowWith("以下数据抽取语句得到的格式与源表定义的格式不一致:\n%s.",dumpsql);
			}
			wociReplaceStmt(*dtmt.Cur(),stmt);
			wociReplaceStmt(*dtmt.Next(),stmt);
			dtmt.Cur()->AddrFresh();
			dtmt.Next()->AddrFresh();
			//			dtmt.Cur()->Build(stmt);
			//			dtmt.Next()->Build(stmt);
			//准备数据索引表插入变量数组
			dtmt.FetchFirst();
			int rn=dtmt.Wait();
			srn=rn;
			while(rn>0) {
				dtmt.FetchNext();
				lgprintf("开始数据处理");
				freeinfo1("before call prcblk");
				for(int i=0;i<dp.soledindexnum;i++) {
					ProcBlock(datapartid,dtmt.Cur(),i/*dp.psoledindex*/,blockmt,sp.NextTmpFileID());
				}
				lgprintf("数据处理结束");
				freeinfo1("after call prcblk");
				rn=dtmt.Wait();
				srn+=rn;
			}
			//delete dtmt;
			//delete stmt;
		}
		wociSetEcho(ec);
		dumpcomplete=true;
		wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
		sp.UpdateTaskStatus(DUMPED,tabid,datapartid);
		
	}
	catch(...) {
		int frn=wociGetMemtableRows(fnmt);
		errprintf("数据导出异常终止，表%d(%d),中间文件数:%d.",tabid,datapartid,frn);
		sp.log(tabid,datapartid,102,"数据导出异常终止，中间文件数:%d.",frn);
		bool restored=false;
		if(dumpcomplete) {
			//当前任务的导出以完成，但修改DP参数失败.重试10次,如果仍然失败,则放弃.
			int retrytimes=0;
			while(retrytimes<10 &&!restored) {
				restored=true;
				try {
					wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
					sp.UpdateTaskStatus(DUMPED,tabid,datapartid);
				}
				catch(...) {
					lgprintf("表%d(%d)导出已完成,但写入dp参数表(dp_middledatafile)失败,一分钟后重试(%d)...",tabid,datapartid,++retrytimes);
					restored=false;
					mSleep(60000);
				}
			}
		}
		if(!restored) {
			int i;
			wociMTPrint(fnmt,0,NULL);
			//先做恢复任务状态的操作,因为任务状态的人工调整最为容易.如果数据库连接一直没有恢复,
			//则任务状态恢复会引起异常,后续的删除文件和记录的操作不会被执行,可以由人工来确定是否可恢复,如何恢复
			errprintf("恢复任务状态.");
		        sp.log(tabid,datapartid,103,"恢复任务状态.");
			sp.UpdateTaskStatus(NEWTASK,tabid,datapartid);
			errprintf("删除中间文件...");
			for(i=0;i<frn;i++) {
				errprintf("\t %s \t %s",fnmt.PtrStr("datafilename",i),
					fnmt.PtrStr("indexfilename",i));
			}
			for(i=0;i<frn;i++) {
				unlink(fnmt.PtrStr("datafilename",i));
				unlink(fnmt.PtrStr("indexfilename",i));
			}
			errprintf("删除中间文件记录...");
			AutoStmt st(dtdbc);
			st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
			st.Execute(1);
			st.Wait();
			throw;
		}
	}
	
	lgprintf("数据抽取结束,任务状态1-->2,tabid %d(%d)",tabid,datapartid);
	lgprintf("sort time :%11.6f file io time :%11.6f adjust data time:%11.6f",
		sorttm.GetTime(),fiotm.GetTime(),adjtm.GetTime());
	
	lgprintf("结束");
	sp.log(tabid,datapartid,104,"数据抽取结束 ,记录数%lld.",srn);
	//lgprintf("按任意键继续...");
	//getchar();
	//MYSQL中的MY_ISAM不支持事务处理，对MYSQL表的修改不需要提交.
	return 0;
}

MiddleDataLoader::MiddleDataLoader(SysAdmin *_sp):
indexmt(0,0),mdindexmt(0,0),blockmt(0,0),mdf(_sp->GetDTS(),MAX_MIDDLE_FILE_NUM)
{
		  sp=_sp;
		  tmpfilenum=0;
		  pdtf=NULL;
		  pdtfid=NULL;
		  //dtfidlen=0;
}

  void StrToLower(char *str) {
  	    while(*str) *str=tolower(*str++);
	}
	
  int MiddleDataLoader::CreateLike(const char *dbn,const char *tbn,const char *nsrctbn,const char *ndsttbn,const char *taskdate)
  {
	  int tabid=0,srctabid=0;
	  AutoMt mt(sp->GetDTS(),10);
	  char tdt[21];
	  if(strcmp(taskdate,"now")==0) 
		  wociGetCurDateTime(tdt);
	  else {
		  mt.FetchAll("select adddate(cast('%s' AS DATETIME),interval 0 day) as tskdate",taskdate);
		  if(mt.Wait()!=1) 
			  ThrowWith("日期格式错误:'%s'",taskdate);
		  memcpy(tdt,mt.PtrDate("tskdate",0),7);
		  if(wociGetYear(tdt)==0)
			  ThrowWith("日期格式错误:'%s'",taskdate);
	  }
	  mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbn,ndsttbn);
	  if(mt.Wait()>0) 
		  ThrowWith("表 %s.%s 已经存在。",dbn,ndsttbn);
	  
	  AutoMt tabmt(sp->GetDTS(),10);
	  tabmt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and lower(tabname)='%s'",dbn,tbn);
	  if(tabmt.Wait()!=1) 
		  ThrowWith("参考表 %s.%s 不存在。",dbn,tbn);
	  int reftabid=tabmt.GetInt("tabid",0);
	  //填充目标表信息
	  strcpy(tabmt.PtrStr("tabdesc",0),ndsttbn);
	  strcpy(tabmt.PtrStr("tabname",0),ndsttbn);
	  *tabmt.PtrInt("cdfileid",0)=0;
	  *tabmt.PtrDouble("recordnum",0)=0;
	  *tabmt.PtrInt("firstdatafileid",0)=0;
	  *tabmt.PtrInt("datafilenum",0)=0;
	  *tabmt.PtrDouble("totalbytes",0)=0;
	  const char *prefsrctbn=tabmt.PtrStr("srctabname",0);
	  StrToLower((char*)prefsrctbn);
	  //参考源表后面还要引用,暂时不替换
	  //strcpy(tabmt.PtrStr("srctabname",0),nsrctbn);
	  tabid=sp->NextTableID();
	  *tabmt.PtrInt("tabid",0)=tabid;
	  // 对应的源表稍后修改
	  mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	  if(mt.Wait()>0)
		ThrowWith("编号重复: 编号%d的目标表'%s.%s'已经存在!",mt.GetInt("tabid",0),mt.PtrStr("databasename",0),mt.PtrStr("tabname",0));
	  mt.FetchAll("select * from dp.dp_index where tabid=%d",tabid);
	  if(mt.Wait()>0)
		ThrowWith("发现不正确的索引参数(表%d)！",mt.GetInt("tabid",0));
	  mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",tabid);
	  if(mt.Wait()>0) 
		ThrowWith("发现不正确的数据文件记录(表%d)!",mt.GetInt("tabid",0));
	  mt.FetchAll("select * from dp.dp_datapart where tabid=%d",tabid);
	  if(mt.Wait()>0) 
		ThrowWith("发现不正确的数据分组参数(表%d)!",mt.GetInt("tabid",0));

	  AutoMt indexmt(sp->GetDTS(),200);
	  indexmt.FetchAll("select * from dp.dp_index where tabid=%d",reftabid);
	  int irn=indexmt.Wait();
	  if(irn<1)
		  ThrowWith("参考表 %s.%s 没有建立索引。",dbn,tbn);
	  int soledct=1;
	  //填充索引信息，重建复用关系
	  for(int ip=0;ip<irn;ip++) {
	  	*indexmt.PtrInt("tabid",ip)=tabid;
	  }
	  
	  AutoMt taskmt(sp->GetDTS(),500);
	  taskmt.FetchAll("select * from dp.dp_datapart where tabid=%d",reftabid);
	  int trn=taskmt.Wait();
	  if(trn<1) 
		  ThrowWith("参考表 %s.%s 没有数据分组信息。",dbn,tbn);
	  
	  //对数据抽取语句作大小写敏感的源表名称替换,可能有以下问题:
	  // 1. 如果字段名或常量名中与源表名称相同,会造成替换失败
	  // 2. 如果源表名称大小写不一致,会造成替换失败
	  for(int tp=0;tp<trn;tp++) {
	   char sqlbk[5000];
	   char *psql=taskmt.PtrStr("extsql",tp);
	   strcpy(sqlbk,psql);
	   if(strcmp(prefsrctbn,nsrctbn)!=0) {
		    char tmp[5000];
	      strcpy(tmp,psql);
  	    char extsql[5000];
			 	StrToLower(tmp);
		    char *sch=strstr(tmp," from ");
		    if(sch) {
		    	sch+=6;
		    	strncpy(extsql,psql,sch-tmp);
		    	extsql[sch-tmp]=0;
		    	char *sch2=strstr(sch,prefsrctbn);
		    	if(sch2) {
		        strncpy(extsql+strlen(extsql),psql+(sch-tmp),sch2-sch);
		        strcat(extsql,nsrctbn);
		        strcat(extsql,psql+(sch2-tmp)+strlen(prefsrctbn));
		        strcpy(psql,extsql);
		      }
		    }
		  }
		  if(strcmp(sqlbk,psql)==0) 
		    lgprintf("数据分组%d的数据抽取语句未作修改，请根据需要手工调整.",taskmt.GetInt("datapartid",tp));
		  else
		    lgprintf("数据分组%d的数据抽取语句已经修改：\n%s\n--->\n%s\n请根据需要手工调整.",taskmt.GetInt("datapartid",tp),sqlbk,psql);
		    
		  *taskmt.PtrInt("tabid",tp)=tabid; 
		  memcpy(taskmt.PtrDate("begintime",tp),tdt,7);
		  *taskmt.PtrInt("status",tp)=0;
		  //sprintf(taskmt.PtrStr("partdesc",tp),"%s.%s",dbn,ndsttbn);

	  }
	  strcpy(tabmt.PtrStr("srctabname",0),nsrctbn);
	  try {
		  wociAppendToDbTable(tabmt,"dp.dp_table",sp->GetDTS(),true);
		  wociAppendToDbTable(indexmt,"dp.dp_index",sp->GetDTS(),true);
		  wociAppendToDbTable(taskmt,"dp.dp_datapart",sp->GetDTS(),true);
	  }
	  catch(...) {
		  //恢复数据，回退所有操作
		  AutoStmt st(sp->GetDTS());
		  st.DirectExecute("delete from dp.dp_table where tabid=%d",tabid);
		  st.DirectExecute("delete from dp.dp_index where tabid=%d",tabid);
		  st.DirectExecute("delete from dp.dp_datapart where tabid=%d",tabid);
		  errprintf("作类似创建时数据提交失败，已删除数据。");
		  throw;
	  }
	  {
		  char dtstr[100];
		  wociDateTimeToStr(tdt,dtstr);
		  lgprintf("创建成功,源表'%s',目标表'%s',开始时间'%s'.",nsrctbn,ndsttbn,dtstr);
	  }
	  return 0;
}

//DT data&index File Check
int MiddleDataLoader::dtfile_chk(const char *dbname,const char *tname) {
	//Check deserved temporary(middle) fileset
	//检查状态为1的任务
	mdf.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbname,tname);
	int rn=mdf.Wait();
	if(rn<1) ThrowWith("DP文件检查:在dp_table中'%s.%s'目标表无记录。",dbname,tname);
	int firstfid=mdf.GetInt("firstdatafileid",0);
	int tabid=*mdf.PtrInt("tabid",0);
	int blockmaxrn=*mdf.PtrInt("maxrecinblock",0);
	double totrc=mdf.GetDouble("recordnum",0);
	sp->OutTaskDesc("目标表检查 :",0,tabid);
	char *srcbf=new char[SRCBUFLEN];//每一次处理的最大数据块（解压缩后）。
	int errct=0;
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 order by indexgid,fileid",tabid);
	int irn=mdf.Wait();
	if(irn<1) {
		ThrowWith("DP文件检查:在dp_datafilemap中%d目标表无数据文件的记录。",tabid);
	}
	
	{
		AutoMt datmt(sp->GetDTS(),100);
		datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileid=%d",tabid,firstfid);
		if(datmt.Wait()!=1) 
			ThrowWith("开始数据文件(编号%d)在系统中没有记录.",firstfid);
		char linkfn[300];
		strcpy(linkfn,datmt.PtrStr("filename",0));
		datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 and isfirstindex=1 order by datapartid,fileid",tabid);
		int rn1=datmt.Wait();
		if(firstfid!=datmt.GetInt("fileid",0)) 
			ThrowWith("开始数据文件(编号%d)在设置错误，应该是%d..",firstfid,datmt.GetInt("fileid",0));
		int lfn=0;
		while(true) {
			file_mt dtf;
			lfn++;
			dtf.Open(linkfn,0);
			const char *fn=dtf.GetNextFileName();
			if(fn==NULL) {
				printf("%s==>结束.\n",linkfn);
				break;
			}
			printf("%s==>%s\n",linkfn,fn);
			strcpy(linkfn,fn);
		}
		if(lfn!=rn1) 
			ThrowWith("文件链接错误，缺失%d个文件.",rn1-lfn);
	}
	
	mytimer chktm;
	chktm.Start();
	try {
		int oldidxid=-1;
		for(int iid=0;iid<irn;iid++) {
			//取基本参数
			int indexid=mdf.GetInt("indexgid",iid);
			
			AutoMt idxsubmt(sp->GetDTS(),100);
			idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d ",tabid,indexid);
			rn=idxsubmt.Wait();
			if(rn<1) {
				ThrowWith("DP文件检查:在dp.dp_index中%d目标表无%d索引的记录。",tabid,indexid);
			}
			
			printf("检查文件%s--\n",mdf.PtrStr("filename",iid));
			fflush(stdout);
			
			//遍历全部索引数据
			char dtfn[300];
			dt_file idxf;
			idxf.Open(mdf.PtrStr("idxfname",iid),0);
			AutoMt idxmt(0);
			idxmt.SetHandle(idxf.CreateMt(1));
			idxmt.SetHandle(idxf.CreateMt(FIX_MAXINDEXRN/wociGetRowLen(idxmt)));
			int brn=0;//idxf.ReadMt(-1,0,idxmt,false);
			int sbrn=0;
			while( (sbrn=idxf.ReadMt(-1,0,idxmt,true))>0) brn+=sbrn;
			int thiserrct=errct;
			//把索引数据文件的后缀由idx替换为dat就是数据文件.
			AutoMt destmt(0);
			strcpy(dtfn,mdf.PtrStr("idxfname",iid));
			strcpy(dtfn+strlen(dtfn)-3,"dat");
			//从数据文件取字段结构，内存表大小为目标表的每数据块最大行数。
			//destmt.SetHandle(dtf.CreateMt(blockmaxrn));
			FILE *fp=fopen(dtfn,"rb");
			if(fp==NULL)
				ThrowWith("DP文件检查:文件'%s'错误.",dtfn);
			fseek(fp,0,SEEK_END);
			unsigned int flen=ftell(fp);
			fseek(fp,0,SEEK_SET);
			block_hdr *pbhdr=(block_hdr *)srcbf;
			
			int oldblockstart=-1;
			int dspct=0;
			int totct=0;
			int blockstart,blocksize;
			int rownum;
			int bcn=wociGetColumnPosByName(idxmt,"dtfid");
			int bkf=0;
			sbrn=idxf.ReadMt(0,0,idxmt,true);
			int ist=0;
			for(int i=0;i<brn;i++) {
				//直接使用字段名称会造成idx_rownum字段的名称不匹配，早期的idx数据文件中的字段名为rownum.
				//dtfid=*idxmt.PtrInt(bcn,i);
				if(i>=sbrn) {
					ist=sbrn;
					sbrn+=idxf.ReadMt(-1,0,idxmt,true);
				}
				blockstart=*idxmt.PtrInt(bcn+1,i-ist);
				blocksize=*idxmt.PtrInt(bcn+2,i-ist);
				//blockrn=*idxmt.PtrInt(bcn+3,i);
				//startrow=*idxmt.PtrInt(bcn+4,i);
				rownum=*idxmt.PtrInt(bcn+5,i-ist);
				if(oldblockstart!=blockstart) {
					try {
						//dtf.ReadMt(blockstart,blocksize,mdf,1,1,srcbf);
						fseek(fp,blockstart,SEEK_SET);
						if(fread(srcbf,1,blocksize,fp)!=blocksize) {
							errprintf("读数据错误，位置:%d,长度:%d,索引序号:%d.\n",blockstart,blocksize,i);
							throw -1;
						}
						pbhdr->ReverseByteOrder();
						if(!dt_file::CheckBlockFlag(pbhdr->blockflag))
						{
							errprintf("错误的块标识，位置:%d,长度:%d,索引序号:%d.\n",blockstart,blocksize,i);
							throw -1;
						}
						if(pbhdr->blockflag!=bkf) {
							bkf=pbhdr->blockflag;
							//if(bkf==BLOCKFLAG) printf("数据块类型:WOCI.\n");
							//else if(bkf==MYSQLBLOCKFLAG)
							//  printf("数据块类型:MYISAM.\n");
							printf("数据块类型: %s .\n",dt_file::GetBlockTypeName(pbhdr->blockflag));
							printf("压缩类型:%d.\n",pbhdr->compressflag);
						}
						if(pbhdr->storelen!=blocksize-sizeof(block_hdr))
						{
							errprintf("错误的块长度，位置:%d,长度:%d,索引序号:%d.\n",blockstart,blocksize,i);
							throw -1;
						}
					}
					catch (...) {
						if(errct++>100) {
							errprintf("太多的错误，已放弃检查！");
							throw;
						}	
					}
					//int mt=dtf.ReadBlock(blockstart,0,1,true);
					//destmt.SetHandle(mt,true);
					oldblockstart=blockstart;
				}
				totct+=rownum;
				if(totct-dspct>200000) {
					printf("%d/%d    --- %d%%\r",i,brn,i*100/brn);
					fflush(stdout);
					dspct=totct;
				}
			} // end of for(...)
			if(ftell(fp)!=flen) {
				errprintf("文件长度不匹配，数据文件长度:%d,索引文件指示的结束位置:%d\n",flen,ftell(fp));
				errct++;
			}
			printf("文件检查完毕，错误数 ：%d.    \n",errct-thiserrct);
			fclose(fp);
		}// end of for
	} // end of try
	catch (...) {
		errprintf("DP文件检查出现异常，tabid:%d.",tabid);
	}
	delete []srcbf;
	if(errct>0)
		errprintf("DP文件检查出现错误，可能需要使用异构重建方式建立索引.");
	printf("\n");
	chktm.Stop();
	lgprintf("DP文件检查结束,共处理%d个文件,发现%d个错误(%.2fs).",irn,errct,chktm.GetTime());
	return 1;
  }
  
  
  void MiddleDataLoader::CheckEmptyDump() {
	  mdf.FetchAll("select * from dp.dp_datapart where status=2 limit 100");
	  int rn=mdf.Wait();
	  if(rn>0) {
		  while(--rn>=0) {
			  AutoMt tmt(sp->GetDTS(),10);
			  tmt.FetchAll("select * from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate!=3 limit 10",
			      mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
			  if(tmt.Wait()<1) {
				  AutoStmt st(sp->GetDTS());
				  st.Prepare(" update dp.dp_datapart set status=3 where tabid=%d and datapartid=%d",
			      		mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
				  st.Execute(1);
				  st.Wait();
				  lgprintf("表%d(%d)的数据为空，修改为已整理。",mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
			  }
		  }
	  }
  }
  // MAXINDEXRN 缺省值500MB,LOADIDXSIZE缺省值1600MB
  // MAXINDEXRN 为最终索引文件,记录长度对应索引表.
  // LOADIDXSIZE 存储临时索引,记录长度从数据导出时的索引文件中内存表计算.
  
  //2005/08/24修改： MAXINDEXRN不再使用
  //2005/11/01修改： 装入状态分为1(待装入），2（正装入),3（已装入);取消10状态(原用于区分第一次和第二次装入)
  //2005/11/01修改： 索引数据装入内存时的策略改为按索引分段。
  //2006/02/15修改： 一次装入所有索引数据，改为分段装入
  //			一次只装入每个文件中的一个内存表(解压缩后1M,索引记录设50字节长,则有约2万条记录,1G内存可以容纳1000/(1*1.2)=800个数据文件)
  int MiddleDataLoader::Load(int MAXINDEXRN,int LOADTIDXSIZE,bool useOldBlock) {
	  //Check deserved temporary(middle) fileset
	  //检查状态为1的任务，1为抽取结束等待装入.
	  CheckEmptyDump();
	  mdf.FetchAll("select * from dp.dp_middledatafile where procstate<=1 order by tabid,datapartid,indexgid limit 100");
	  int rn=mdf.Wait();
	  //如果索引超过LOADIDXSIZE,需要分批处理,则只在处理第一批数据时清空DT_DATAFILEMAP/DT_INDEXFILEMAP表
	  //区分第一次和第二次装入的意义：如果一份数据子集（指定数据集-〉分区(datapartid)->索引)被导出后，在开始装入以前，要
	  //  先删除上次正在装入的数据（注意：已上线的数据不在这里删除，而在装入结束后删除).
	  bool firstbatch=true;
	  if(rn<1) return 0;
	  
	  //检查该数据子集是否第一次装入
	  int tabid=mdf.GetInt("tabid",0);
	  int indexid=mdf.GetInt("indexgid",0);
	  int datapartid=mdf.GetInt("datapartid",0);
	  mdf.FetchAll("select procstate from dp.dp_middledatafile where tabid=%d and datapartid=%d and indexgid=%d and procstate>1 limit 10",
		  tabid,datapartid,indexid);
	  firstbatch=mdf.Wait()<1;//数据子集没有正整理或已整理的记录。
	  
	  //取出中间文件记录
	  mdf.FetchAll("select * from dp.dp_middledatafile where  tabid=%d and datapartid=%d and indexgid=%d and procstate<=1 order by mdfid limit %d",
		  tabid,datapartid,indexid,MAX_MIDDLE_FILE_NUM);
	  rn=mdf.Wait();
	  if(rn<1) 
		  ThrowWith("MiddleDataLoader::Load : 确定数据子集后找不到中间数据记录(未处理)。");
	  
	  
	  //取基本参数
	  int idxtlimit=0,idxdlimit=0;//临时区(多个抽取数据文件对应)和目标区(单个目标数据文件对应)的索引最大记录数.
	  wociSetTraceFile("dr数据整理/");
	  tabid=mdf.GetInt("tabid",0);
	  indexid=mdf.GetInt("indexgid",0);
	  datapartid=mdf.GetInt("datapartid",0);
	  int compflag=5;
	  bool col_object=false; // useOldBlock=true时，忽略col_object
	  //取压缩类型和数据块类型
	  {
		  AutoMt tmt(sp->GetDTS(),10);
		  tmt.FetchAll("select compflag from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
		  if(tmt.Wait()>0)
			compflag=tmt.GetInt("compflag",0);
		  tmt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
		  if(tmt.Wait()>0) {
			  if(TestColumn(tmt,"blocktype")) 
				  col_object=tmt.GetInt("blocktype",0)&1; // bit 1 means column object block type.
		  }
	  }
	  
	  //从dt_datafilemap(存blockmt文件表)和dt_indexfilemap(存indexmt文件表)
	  //建立内存表结构
	  char fn[300];
	  AutoMt fnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);
	  
	  fnmt.FetchAll("select * from dp.dp_datafilemap limit 2");
	  fnmt.Wait();
	  wociReset(fnmt);
	  LONG64 dispct=0,lstdispct=0,llt=0;
	  sp->OutTaskDesc("数据重组 :",tabid,datapartid,indexid);
	  sp->log(tabid,datapartid,105,"数据重组,索引组%d,日志文件 '%s' .",indexid,lgfile);
	  int start_mdfid=0,end_mdfid=0;
	  char sqlbf[200];
	  LONG64 extrn=0,lmtextrn=-1;
	  LONG64 adjrn=0;
	  try {	
		  tmpfilenum=rn;
		  //索引数据文件遍历，累加索引总行数
		  LONG64 idxrn=0;
		  int i;
		  int mdrowlen=0;
		  //取索引记录的长度(临时索引数据记录)
		  {
			  dt_file df;
			  df.Open(mdf.PtrStr("indexfilename",0),0);
			  mdrowlen=df.GetRowLen();
		  }
		  
		  lgprintf("临时索引数据记录长度:%d",mdrowlen);  
		  int lmtrn=-1,lmtfn=-1;
		  //检查临时索引的内存用量,判断当前的参数设置是否可以一次装入全部索引记录
		  for( i=0;i<rn;i++) {
			  dt_file df;
			  df.Open(mdf.PtrStr("indexfilename",i),0);
			  if(mdrowlen==0) 
				  mdrowlen=df.GetRowLen();
			  extrn+=mdf.GetInt("recordnum",i);
			  idxrn+=df.GetRowNum();
			  llt=idxrn;
			  llt*=mdrowlen;

			  llt/=(1024*1024); //-->(MB)
			  if(llt>LOADTIDXSIZE && lmtrn==-1) { //使用的临时索引超过内存允许参数的上限，需要拆分
				  if(i==0) 
					  ThrowWith("MLoader:内存参数DP_LOADTIDXSIZE设置太低:%dMB，\n"
					  "不足以装载至少一个临时抽取块:%dMB。\n",LOADTIDXSIZE,(int)llt);
				  lmtrn=idxrn-df.GetRowNum();
				  lmtfn=i;
				  lmtextrn=extrn-mdf.GetInt("recordnum",i);
			  }
		  }
		  if(lmtrn!=-1) { //使用的临时索引超过内存允许参数的上限，需要拆分
			  lgprintf("MLoader:数据整理超过内存限制%dMB,需要处理文件数%d,数据组%d,起始点:%d,截至点:%d,文件数:%d .",LOADTIDXSIZE,rn,datapartid,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn);
			  lgprintf("索引需要内存%dM ",idxrn*mdrowlen/(1024*1024));
			  sp->log(tabid,datapartid,106,"MLoader:数据整理超过内存限制%dMB,需要处理文件数%d,起始点:%d,截至点:%d,文件数:%d ,需要内存%dMB.",LOADTIDXSIZE,rn,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn,idxrn*mdrowlen/(1024*1024));
			  idxrn=lmtrn;
			  //fix a bug
			  rn=lmtfn;
		  }
		  if(lmtextrn==-1) lmtextrn=extrn;
		  lgprintf("数据整理实际使用内存%dM,索引列宽%d.",idxrn*mdrowlen/(1024*1024),mdrowlen);
		  start_mdfid=mdf.GetInt("mdfid",0);
		  end_mdfid=mdf.GetInt("mdfid",rn-1);
		  lgprintf("索引记录数:%d",idxrn);
		  //为防止功能重入,中间文件状态修改.
		  lgprintf("修改中间文件的处理状态(tabid:%d,datapartid:%d,indexid:%d,%d个文件)：1-->2",tabid,datapartid,indexid,rn);
		  sprintf(sqlbf,"update dp.dp_middledatafile set procstate=2 where tabid=%d  and datapartid=%d and indexgid=%d and procstate<=1 and mdfid>=%d and mdfid<=%d ",
			  tabid,datapartid,indexid,start_mdfid,end_mdfid);
		  int ern=sp->DoQuery(sqlbf);
		  if(ern!=rn) {
			  if(ern>0) {  //上面的UPdate语句修改了一部分记录状态,且不可恢复,需要对数据子集作重新装入.
				  ThrowWith("MLoader修改中间文件的处理状态异常，可能是与其它进程冲突。\n"
					  " 部分文件的处理状态不一致，请立即停止所有的数据整理任务，重新作数据整理操作。\n"
					  "  tabid:%d,datapartid:%d,indexid:%d.\n",
					  tabid,datapartid,indexid);
			  }
			  else //上面的update语句未产生实际修改操作,其它进程可以继续处理.
				  ThrowWith("MLoader修改中间文件的处理状态异常，可能是与其它进程冲突。\n"
					  "  tabid:%d,datapartid:%d,indexid:%d.\n",
					  tabid,datapartid,indexid);
		  }
		  
		  	//ThrowWith("调试终止---%d组数据.",dispct);
	if(firstbatch) {
		lgprintf("删除数据分组%d,索引%d 的数据和索引记录(表:%d)...",datapartid,indexid,tabid);
		sp->log(tabid,datapartid,107,"删除索引组%d过去生成的数据和索引记录...",indexid);
		AutoMt dfnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);
		dfnmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1",tabid,datapartid,indexid);
		int dfn=dfnmt.Wait();
		if(dfn>0) {
			AutoStmt st(sp->GetDTS());
			for(int di=0;di<dfn;di++)
			{
				lgprintf("删除'%s'/'%s'和附加的depcp,dep5文件",dfnmt.PtrStr("filename",di),dfnmt.PtrStr("idxfname",di));
				unlink(dfnmt.PtrStr("filename",di));
				unlink(dfnmt.PtrStr("idxfname",di));
				char tmp[300];
				sprintf(tmp,"%s.depcp",dfnmt.PtrStr("filename",di));
				unlink(tmp);
				sprintf(tmp,"%s.dep5",dfnmt.PtrStr("filename",di));
				unlink(tmp);
				sprintf(tmp,"%s.depcp",dfnmt.PtrStr("idxfname",di));
				unlink(tmp);
				sprintf(tmp,"%s.dep5",dfnmt.PtrStr("idxfname",di));
				unlink(tmp);
			}
			st.Prepare(" delete from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1",tabid,datapartid,indexid);
			st.Execute(1);
			st.Wait();
		}
	}

		  //建立中间索引(中间文件数据块索引)内存表mdindexmt和目标数据块内存表blockmt
		  int maxblockrn=sp->GetMaxBlockRn(tabid);
		  {
			  dt_file idf;
			  idf.Open(mdf.PtrStr("indexfilename",0),0);
			  mdindexmt.SetHandle(idf.CreateMt(idxrn));
			  //wociAddColumn(idxmt,"dtfileid",NULL,COLUMN_TYPE_INT,4,0);
			  //idxmt.SetMaxRows(idxrn);
			  //mdindexmt.Build();
			  idf.Open(mdf.PtrStr("datafilename",0),0);
			  blockmt.SetHandle(idf.CreateMt(maxblockrn));
			  //mdblockmt.SetHandle(idf.CreateMt(maxblockrn));
		  }
		  LONG64 crn=0;
		  //	wociGetIntAddrByName(idxmt,"dtfileid",0,&pdtfid);
		  // pdtfid为一个字符数组，偏移为x的值表示中间索引内存表第x行的文件序号(Base0);
		  if(pdtfid)
			  delete [] pdtfid;
		  pdtfid=new unsigned short [idxrn];
		  //dtfidlen=idxrn;
		  //pdtf为file_mt对象的数组。存放数据文件对象。
		  if(pdtf) delete [] pdtf;
		  pdtf=new file_mt[rn];
		  //mdindexmt.SetMaxRows(idxrn);
		  //读入全部索引数据到mdindexmt(中间索引内存表),并打开全部数据文件
		  //pdtfid指向对应的文件序号。
		  lgprintf("读索引数据...");
		  for(i=0;i<rn;i++) {
			  dt_file df; 
			  df.Open(mdf.PtrStr("indexfilename",i),0);
			  int brn=0;
			  int sbrn=0;
			  while( (sbrn=df.ReadMt(-1,0,mdindexmt,false))>0) brn+=sbrn;
			  
			  for(int j=crn;j<crn+brn;j++)
				  pdtfid[j]=(unsigned short )i;
			  crn+=brn;
			  
			  //pdtf[i].SetParalMode(true);
			  pdtf[i].Open(mdf.PtrStr("datafilename",i),0);
			  //		if(crn>10000000) break; ///DEBUG
		  }
		  lgprintf("索引数据:%d.",crn);
		  if(crn!=idxrn) {
		  	sp->log(tabid,0,108,"索引数据文件的总记录数%lld,与指示信息不一致:%lld",crn,idxrn);
		  	ThrowWith("索引数据文件的总记录数%lld,与指示信息不一致:%lld",crn,idxrn);
		  }
		  //对mdindexmt(中间索引内存表)做排序。
		  //由于排序不涉及内存表的数据排列，而是新建记录顺序表，因此，
		  // pdtfid作为内存表外的等效内存表字段，不需做处理。
		  lgprintf("排序('%s')...",mdf.PtrStr("soledindexcols",0));
		  {
			  char sort[300];
			  sprintf(sort,"%s,idx_fid,idx_blockoffset",mdf.PtrStr("soledindexcols",0));
			  wociSetSortColumn(mdindexmt,sort);
			  wociSortHeap(mdindexmt);
		  }
		  lgprintf("排序完成.");
		  //取得全部独立索引结构
		  sp->GetSoledIndexParam(datapartid,&dp,tabid);
		  //检查需要处理的中间数据是否使用主独立索引，如果是，isfirstidx=1.
		  int isfirstidx=0;
		  indexparam *ip;
		  {
			  int idxp=dp.GetOffset(indexid);
			  ip=&dp.idxp[idxp];
			  if(idxp==dp.psoledindex) isfirstidx=1;
		  }
		  //从结构描述文件建立indexmt,indexmt是目标索引内存表。是建立目标索引表的数据源。
		  //indexmt.SetHandle(CreateMtFromFile(MAXINDEXRN,ip->cdsfilename));
		  int pblockc[20];
		  char colsname[500];
		  void *indexptr[40];
		  indexmt.SetMaxRows(1);
		  int stcn=sp->CreateIndexMT(indexmt,blockmt,tabid,indexid,pblockc,colsname,true),bcn=stcn;
		  llt=FIX_MAXINDEXRN;
		  llt/=wociGetRowLen(indexmt); //==> to rownum;
		  idxdlimit=(int)llt;
		  indexmt.SetMaxRows(idxdlimit);
		  sp->CreateIndexMT(indexmt,blockmt,tabid,indexid,pblockc,colsname,true);
		  bool pkmode=false;
		  //取独立索引在mdindexmt(中间索引存表)结构中的位置。
		  //设置对indexmt插入记录需要的结构和变量。
		  int pidxc1[20];
		  int cn1=wociConvertColStrToInt(mdindexmt,ip->idxcolsname,pidxc1);
		  int dtfid,blockstart,blocksize,blockrn=0,startrow,rownum;
		  indexptr[stcn++]=&dtfid;
		  indexptr[stcn++]=&blockstart;
		  indexptr[stcn++]=&blocksize;
		  indexptr[stcn++]=&blockrn;
		  indexptr[stcn++]=&startrow;
		  indexptr[stcn++]=&rownum;
		  indexptr[stcn]=NULL;
		  //indexmt中的blocksize,blockstart?,blockrownum需要滞后写入，
		  //因而需要取出这些字段的首地址。
		  int *pblocksize;
		  int *pblockstart;
		  int *pblockrn;
		  wociGetIntAddrByName(indexmt,"blocksize",0,&pblocksize);
		  wociGetIntAddrByName(indexmt,"blockstart",0,&pblockstart);
		  wociGetIntAddrByName(indexmt,"blockrownum",0,&pblockrn);
		  //mdindexmt中下列字段是读中间数据文件的关键项。
		  int *poffset,*pstartrow,*prownum;
		  wociGetIntAddrByName(mdindexmt,"idx_blockoffset",0,&poffset);
		  wociGetIntAddrByName(mdindexmt,"idx_startrow",0,&pstartrow);
		  wociGetIntAddrByName(mdindexmt,"idx_rownum",0,&prownum);
		  
		  //indexmt 记录行数计数复位
		  int indexmtrn=0;
		  
		  //建立目标数据文件和目标索引文件对象(dt_file).
		  // 目标数据文件和目标索引文件一一对应，目标数据文件中按子块方式存储内存表
		  //  目标索引文件中为一个单一的内存表，文件头结构在写入内存表时建立
		  dtfid=sp->NextDstFileID(tabid);
		  char tbname[150];
		  sp->GetTableName(tabid,-1,tbname,NULL,TBNAME_DEST);
		  dp.usingpathid=0;
		  sprintf(fn,"%s%s_%d_%d_%d.dat",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
		  {
		  	FILE *fp;
		  	fp=fopen(fn,"rb");
		  	if(fp!=NULL) {
		  		fclose(fp);
		  		ThrowWith("文件'%s'已经存在，不能继续整理数据。",fn);
		  	}
		  }
		  dt_file dstfile;
		  dstfile.Open(fn,1);
		  blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
		  char idxfn[300];
		  sprintf(idxfn,"%s%s_%d_%d_%d.idx",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
		  dt_file idxf;
		  idxf.Open(idxfn,1);
		  idxf.WriteHeader(indexmt,0,dtfid);
		  
		  startrow=0;
		  rownum=0;
		  blockrn=0;
		  int subtotrn=0;
		  int idxtotrn=0;
		  lgprintf("开始数据处理(MiddleDataLoading)....");
		  
		  /*******按照Sort顺序遍历mdindexmt(中间索引内存表)***********/
		  //
		  //
		  lgprintf("创建文件,编号:%d...",dtfid);
		  int firstrn=wociGetRawrnBySort(mdindexmt,0);
		  mytimer arrtm;
		  arrtm.Start();
		  for(i=0;i<idxrn;i++) {
			  int thisrn=wociGetRawrnBySort(mdindexmt,i);
			  int rfid=pdtfid[thisrn];
			  int sbrn=prownum[thisrn];
			  int sbstart=pstartrow[thisrn];
			  int sameval=mdindexmt.CompareRows(firstrn,thisrn,pidxc1,cn1);
			  if(sameval!=0) {
				  //要处理的数据块和上次的数据块不是一个关键字，
				  // 所以，先保存上次的关键字索引，关键字的值从blockmt中提取。
				  // startrow变量始终保存第一个未存储关键字索引的数据块开始行号。
				  int c;
				  for(c=0;c<bcn;c++) {
					  indexptr[c]=blockmt.PtrVoid(pblockc[c],startrow);
				  }
				  if(rownum>0) {
					  wociInsertRows(indexmt,indexptr,NULL,1);
					  idxtotrn++;
				  }
				  firstrn=thisrn;
				  //这里，出现startrow暂时指向无效行的情况(数据未填充).
				  startrow=blockrn;
				  rownum=0;
			  }
			  //从数据文件中读入数据块
			  int mt=pdtf[rfid].ReadBlock(poffset[thisrn],0,true);
			  //2005/08/24 修改： 索引数据文件为多块数据顺序存储
			  
			  //由于结束当前文件有可能还要增加一条索引,因此判断内存表满的条件为(idxdlimit-1)
			  int irn=wociGetMemtableRows(indexmt);
			  if(irn>=(idxdlimit-2))
			  {
				  int pos=irn-1;
				  //如果blocksize，blockrn等字段还未设置，则不清除
				  while(pos>=0 && pblockstart[pos]==blockstart) pos--;
				  if(pos>0) {
					  //保存已经设置完整的索引数据,false参数表示不需要删除位图区.
					  idxf.WriteMt(indexmt,COMPRESSLEVEL,pos+1,false);
					  if(pos+1<irn) 
						  wociCopyRowsTo(indexmt,indexmt,0,pos+1,irn-pos-1);
					  else wociReset(indexmt);
				  }
				  else 
				  ThrowWith("目标表%d,索引号%d,装入时索引超过最大单一块允许记录数%d",tabid,indexid,idxdlimit);
			  }
			  //数据块拆分 
			  //检查数据块是否需要拆分
			  // 增加一个循环，用于处理mt中的sbrn本身大于maxblockrn的异常情况：
			  //	由于异常原因导致临时导出块大于最终数据块，在索引聚合良好时，会出现不能一次容纳所有临时记录
			  while(true) {
				  if(blockrn+sbrn>maxblockrn ) {
					  //每个数据块至少需要达到最大值的80%。
					  if(blockrn<maxblockrn*.8 ) {
						  //如果不足80%，把当前处理的数据块拆分
						  int rmrn=maxblockrn-blockrn;
						  wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
						  rownum+=rmrn;
						  sbrn-=rmrn;
						  sbstart+=rmrn;
						  blockrn+=rmrn;
					  }
					  
					  //保存块数据
					  if(useOldBlock) 
						  blocksize=dstfile.WriteMt(blockmt,compflag,0,false)-blockstart;
					  else if(col_object)
						  blocksize=dstfile.WriteMySQLMt(blockmt,compflag)-blockstart;
					  else 
						  blocksize=dstfile.WriteMySQLMt(blockmt,compflag,false)-blockstart;
					  adjrn+=wociGetMemtableRows(blockmt);
					  //保存子快索引
					  if(startrow<blockrn) {
						  int c;
						  for(c=0;c<bcn;c++) {
							  indexptr[c]=blockmt.PtrVoid(pblockc[c],startrow);
						  }
						  if(rownum>0) {
							  wociInsertRows(indexmt,indexptr,NULL,1);
							  dispct++;
							  idxtotrn++;
						  }
					  }
					  int irn=wociGetMemtableRows(indexmt);
					  int irn1=irn;
					  while(--irn>=0) {
						  if(pblockstart[irn]==blockstart) {
							  pblocksize[irn]=blocksize;
							  pblockrn[irn]=blockrn;
						  }
						  else break;
					  }
					  
					  blockstart+=blocksize;
					  subtotrn+=blockrn;
					  //数据文件长度超过2G时拆分
					  if(blockstart>2000000000 ) {
						  //增加文件对照表记录(dt_datafilemap)
						  {
							  void *fnmtptr[20];
  							  int idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL,0,false);
							  fnmtptr[0]=&dtfid;
							  fnmtptr[1]=fn;
							  fnmtptr[2]=&datapartid;
							  fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
							  fnmtptr[4]=&dp.tabid;
							  fnmtptr[5]=&ip->idxid;
							  fnmtptr[6]=&isfirstidx;
							  fnmtptr[7]=&blockstart;
							  fnmtptr[8]=&subtotrn;
							  int procstatus=0;
							  fnmtptr[9]=&procstatus;
							  //int compflag=COMPRESSLEVEL;
							  fnmtptr[10]=&compflag;
							  int fileflag=1;
							  fnmtptr[11]=&fileflag;
							  fnmtptr[12]=idxfn;
							  fnmtptr[13]=&idxfsize;
							  fnmtptr[14]=&idxtotrn;
							  fnmtptr[15]=NULL;
							  wociInsertRows(fnmt,fnmtptr,NULL,1);
						  }
						  //
						  dtfid=sp->NextDstFileID(tabid);
		  				  sprintf(fn,"%s%s_%d_%d_%d.dat",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
		  				  {
		  					FILE *fp;
		  					fp=fopen(fn,"rb");
		  					if(fp!=NULL) {
		  						fclose(fp);
		  						ThrowWith("文件'%s'已经存在，不能继续整理数据。",fn);
		  					}
		  				  }
						  dstfile.SetFileHeader(subtotrn,fn);
						  dstfile.Open(fn,1);
						  blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
						  printf("\r                                                                            \r");
						  lgprintf("创建文件,编号:%d...",dtfid);
		  				  sprintf(idxfn,"%s%s_%d_%d_%d.idx",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
						  
						  idxf.SetFileHeader(idxtotrn,idxfn);
						  // create another file
						  idxf.Open(idxfn,1);
						  idxf.WriteHeader(indexmt,0,dtfid);
						  indexmt.Reset();
						  subtotrn=0;
						  blockrn=0;
						  idxtotrn=0;
						  //lgprintf("创建文件,编号:%d...",dtfid);
						  
					  } // end of IF blockstart>2000000000)
					  blockmt.Reset();
					  blockrn=0;
					  firstrn=thisrn;
					  startrow=blockrn;
					  rownum=0;
					  dispct++;
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("用户取消操作!");
					  }
					  if(dispct-lstdispct>=200) {
						  lstdispct=dispct;
						  arrtm.Stop();
						  double tm1=arrtm.GetTime();
						  arrtm.Start();
						  printf("  已生成%lld数据块,用时%.0f秒,预计还需要%.0f秒          .\r",dispct,tm1,(tm1*(idxrn-i))/i);
						  fflush(stdout);
					  }
			} //end of blockrn+sbrn>maxblockrn
			if(blockrn+sbrn>maxblockrn) {
				int rmrn=maxblockrn-blockrn;
				wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
				rownum+=rmrn;
				sbrn-=rmrn;
				sbstart+=rmrn;
				blockrn+=rmrn;
			}
			else {
				wociCopyRowsTo(mt,blockmt,-1,sbstart,sbrn);
				rownum+=sbrn;
				blockrn+=sbrn;
				break;
			}
		} // end of while(true)
	} // end of for(...)
	if(blockrn>0) {
		
		//保存子快索引
		int c;
		for( c=0;c<bcn;c++) {
			indexptr[c]=blockmt.PtrVoid(pblockc[c],startrow);
		}
		//for(c=0;c<bcn2;c++) {
		//	indexptr[bcn1+c]=blockmt.PtrVoid(pblockc2[c],startrow);
		//}
		if(rownum>0) {
			wociInsertRows(indexmt,indexptr,NULL,1);
			dispct++;
		}
		//保存块数据
		//保存块数据
		if(useOldBlock)
			blocksize=dstfile.WriteMt(blockmt,compflag,0,false)-blockstart;
		else if(col_object)
			blocksize=dstfile.WriteMySQLMt(blockmt,compflag)-blockstart;
		else 
			blocksize=dstfile.WriteMySQLMt(blockmt,compflag,false)-blockstart;
		int irn=wociGetMemtableRows(indexmt);
		adjrn+=wociGetMemtableRows(blockmt);
		while(--irn>=0) {
			if(pblockstart[irn]==blockstart) {
				pblocksize[irn]=blocksize;
				pblockrn[irn]=blockrn;
			}
			else break;
		}
		blockstart+=blocksize;
		subtotrn+=blockrn;
		//增加文件对照表记录(dt_datafilemap)
		{
			void *fnmtptr[20];
			int idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL,0,false);
			fnmtptr[0]=&dtfid;
			fnmtptr[1]=fn;
			fnmtptr[2]=&datapartid;
			fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
			fnmtptr[4]=&dp.tabid;
			fnmtptr[5]=&ip->idxid;
			fnmtptr[6]=&isfirstidx;
			fnmtptr[7]=&blockstart;
			fnmtptr[8]=&subtotrn;
			int procstatus=0;
			fnmtptr[9]=&procstatus;
			//int compflag=COMPRESSLEVEL;
			fnmtptr[10]=&compflag;
			int fileflag=1;
			fnmtptr[11]=&fileflag;
		        fnmtptr[12]=idxfn;
			fnmtptr[13]=&idxfsize;
			fnmtptr[14]=&idxtotrn;
			fnmtptr[15]=NULL;
			wociInsertRows(fnmt,fnmtptr,NULL,1);
		}
		
		//
		dstfile.SetFileHeader(subtotrn,NULL);
		idxf.SetFileHeader(idxtotrn,NULL);
		indexmt.Reset();
		blockmt.Reset();
		blockrn=0;
		startrow=blockrn;
		rownum=0;
	}
	
	//记录数校验。这里的校验仅是本次数据重组，有可能是一个数据组的某个索引组的一部分。后面的校验是挑一个索引组，校验整个数据组
	if(adjrn!=lmtextrn) {
		sp->log(tabid,datapartid,109,"数据重组要求处理导出数据%lld行，但实际生成%lld行! 索引组%d.",lmtextrn,adjrn,indexid);
		ThrowWith("数据重组要求处理导出数据%lld行，但实际生成%lld行! 表%d(%d),索引组%d.",lmtextrn,adjrn,tabid,datapartid,indexid);
	}
	wociAppendToDbTable(fnmt,"dp.dp_datafilemap",sp->GetDTS(),true);
	lgprintf("修改中间文件的处理状态(表%d,索引%d,数据组:%d,%d个文件)：2-->3",tabid,indexid,datapartid,rn);
	sprintf(sqlbf,"update dp.dp_middledatafile set procstate=3 where tabid=%d and datapartid=%d and indexgid=%d and procstate=%d",tabid,datapartid,indexid,2);
	sp->DoQuery(sqlbf);
	sp->log(tabid,datapartid,110,"数据重组,索引组:%d,记录数%lld.",indexid,lmtextrn);
	}
	catch (...) {
		int frn=wociGetMemtableRows(fnmt);
		errprintf("数据整理出现异常，表:%d,数据组:%d.",tabid,datapartid);
		sp->log(tabid,datapartid,111,"数据整理出现异常");
		errprintf("恢复中间文件的处理状态(数据组:%d,%d个文件)：2-->1",datapartid,rn);
		sprintf(sqlbf,"update dp.dp_middledatafile set procstate=1 where tabid=%d and datapartid=%d and indexgid=%d and mdfid>=%d and mdfid<=%d",tabid,datapartid,indexid,start_mdfid,end_mdfid);
		sp->DoQuery(sqlbf);
		errprintf("删除已整理的数据和索引文件.");
		errprintf("删除数据文件...");
		int i;
		for(i=0;i<frn;i++) {
			errprintf("\t %s ",fnmt.PtrStr("filename",i));
			errprintf("\t %s ",fnmt.PtrStr("idxfname",i));
		}
		for(i=0;i<frn;i++) {
			unlink(fnmt.PtrStr("filename",i));
			unlink(fnmt.PtrStr("idxfname",i));
		}
		errprintf("删除已处理数据文件和索引文件记录...");
		AutoStmt st(sp->GetDTS());
		for(i=0;i<frn;i++) {
			st.Prepare("delete from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1 and fileid=%d",tabid,datapartid,indexid,fnmt.PtrInt("fileid",i));
			st.Execute(1);
			st.Wait();
		}
		wociCommit(sp->GetDTS());
		
		sp->log(tabid,datapartid,112,"状态已恢复");
		throw ;
	}
	
	lgprintf("数据处理(MiddleDataLoading)结束,共处理数据包%d个.",dispct);
	lgprintf("生成%d个数据文件,已插入dp.dp_datafilemap表.",wociGetMemtableRows(fnmt));
	sp->log(tabid,datapartid,113,"数据重组结束,共处理数据包%d个,生成文件%d个.",dispct,wociGetMemtableRows(fnmt));
	//wociMTPrint(fnmt,0,NULL);
	//检查是否该数据分组的最后一批数据
	// 如果是最后一批数据，则说明：
	//  1. 拆分处理的最后一个批次数据已处理完。
	//  2. 所有该分组对应的一个或多个独立索引都已整理完成。
	//在这个情况下才删除导出的临时数据。
	try
	{
		//检查其他独立索引或同一个分组中其它批次的数据（其他数据子集）是否整理完毕。
		mdf.FetchAll("select * from dp.dp_middledatafile where procstate!=3 and tabid=%d and datapartid=%d ",
			tabid,datapartid);
		int rn=mdf.Wait();
		if(rn==0) {
			mdf.FetchAll("select sum(recordnum) rn from dp.dp_middledatafile where tabid=%d and datapartid and indexgid=%d",
			  tabid,datapartid,indexid);
			mdf.Wait();
			LONG64 trn=mdf.GetLong("rn",0);
			mdf.FetchAll("select sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and datapartid and indexgid=%d and fileflag=1 and procstatus=0",
			  tabid,datapartid,indexid);
			mdf.Wait();
			//数据校验
			if(trn!=mdf.GetLong("rn",0)) {
				//未知原因引起的错误，不能直接恢复状态. 暂停任务的执行
				sp->log(tabid,datapartid,114,"数据校验错误,导出%lld行，整理生成%lld行(验证索引组%d),数据迁移过程已被暂停。",trn,mdf.GetLong("rn",0),indexid);
				sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=70 where tabid=%d",tabid);
				sp->DoQuery(sqlbf);
				ThrowWith("数据校验错误,表%d(%d),导出%lld行，整理生成%lld行(验证索引组%d),数据迁移过程已被暂停",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
			}
			if(mdf.GetInt("rn",0))
			lgprintf("最后一批数据已处理完,任务状态2-->3,表%d(%d)",tabid,datapartid);
			//如果是单分区处理任务，必须是所有相同数据集的任务状态为3，才能启动下一步的操作（数据装入）。
			sprintf(sqlbf,"update dp.dp_datapart set status=3 where tabid=%d and datapartid=%d",
				tabid,datapartid);
			sp->DoQuery(sqlbf);
			//重新创建表结构。
			//sp->CreateDT(tabid);
			//}
			lgprintf("删除中间临时文件...");
			mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
			int dfn=mdf.Wait();
			{
				for(int di=0;di<dfn;di++) {
					lgprintf("删除文件'%s'",mdf.PtrStr("datafilename",di));
					unlink(mdf.PtrStr("datafilename",di));
					lgprintf("删除文件'%s'",mdf.PtrStr("indexfilename",di));
					unlink(mdf.PtrStr("indexfilename",di));
				} 
				lgprintf("删除记录...");
				AutoStmt st(sp->GetDTS());
				st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
				st.Execute(1);
				st.Wait();
			}
		}
		
	}
	catch(...) {
		errprintf("数据整理任务已完成，但任务状态调整或临时中间文件删除时出现错误，需要人工调整。\n表%d(%d)。",
			tabid,datapartid);
		throw;
	}
	return 1;
	//Load index data into memory table (indexmt)
  }
  
  //索引数据表清空,但保留表结构和索引结构
  bool SysAdmin::EmptyIndex(int tabid)
  {
	  AutoMt indexmt(dts,MAX_DST_INDEX_NUM);
	  indexmt.FetchAll("select databasename from dp.dp_table where tabid=%d",tabid);
	  int rn=indexmt.Wait();
	  char dbn[150];
	  char tbname[150],idxtbn[150];
	  strcpy(dbn,indexmt.PtrStr("databasename",0));
	  if(rn<1) ThrowWith("清空索引表时找不到%d目标表.",tabid);
	  indexmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1",tabid);
	  rn=indexmt.Wait();
	  if(rn<1) ThrowWith("清空索引表时找不到独立索引记录(%d目标表).",tabid);
	  char sqlbf[300];
	  for(int i=0;i<rn;i++) {
	  	  GetTableName(tabid,indexmt.GetInt("indexgid",i),tbname,idxtbn,TBNAME_DEST);
		  lgprintf("清空索引表%s...",idxtbn);
		  sprintf(sqlbf,"truncate table %s",idxtbn);
		  DoQuery(sqlbf);
	  }
	  return true;
  }
  
  //返回的mt只能操作内存表,不能操作数据库(fetch),因为语句对象在函数退出时已经释放
  int SysAdmin::BuildMtFromSrcTable(int srcsys,int tabid,AutoMt *mt) {
	  int tabp=wociSearchIK(dt_table,tabid);
	  const char *srctbn=dt_table.PtrStr("srctabname",tabp);
  	  AutoStmt srcst(srcsys);
	  srcst.Prepare("select * from %s.%s",dt_table.PtrStr("srcowner",tabp),
		  srctbn);
	  wociBuildStmt(*mt,srcst,mt->GetMaxRows());
	  return 0;
  }
  
  int SysAdmin::GetSrcTableStructMt(int tabp, int srcsys)
  {
	  AutoStmt srcst(srcsys);
	  srcst.Prepare("select * from %s.%s",dt_table.PtrStr("srcowner",tabp),dt_table.PtrStr("srctabname",tabp));
	  int mt=wociCreateMemTable();
	  wociBuildStmt(mt,srcst,10);
	  return mt;
  }
  
  //如果去掉字段描述文件的支持,则下面的函数会大大简化
  bool SysAdmin::CreateTableOnMysql(int srcmt,const char *tabname,bool forcecreate)
  {
	  //如果目标表已存在，先删除
	    	char sqlbf[3000];
			bool exist=conn.TouchTable(tabname);
			if(exist && !forcecreate) 
				ThrowWith("Create MySQL Table '%s' failed,table already exists.",tabname);
			if(exist) {
				printf("table %s has exist,dropped.\n",tabname);
				sprintf(sqlbf,"drop table %s",tabname);
				conn.DoQuery(sqlbf);
			}
			//建立目标标及其表结构的描述文件
			wociGetCreateTableSQL(srcmt,sqlbf,tabname,true);
			strcat(sqlbf," PACK_KEYS = 1");
			//printf("%s.\n",sqlbf);
			conn.DoQuery(sqlbf);
			mSleep(300);			
			return true;
  }
  
  void SysAdmin::CloseTable(int tabid,char *tbname,bool cleandt) {
	  char tabname[150];
	  //AutoStmt st(dts);
	  //if(cleandt)
	  //	  st.Prepare("update dp.dp_table set recordnum=0,firstdatafileid=0,totalbytes=0 where tabid=%d",tabid);
	  //else
	  //	  st.Prepare("update dp.dp_table set recordnum=0 where tabid=%d",tabid);
	  //st.Execute(1);
	  //st.Wait();
	  //wociCommit(dts);
	  if(tbname==NULL) {
		  GetTableName(tabid,-1,tabname,NULL,TBNAME_DEST);
	  }
	  else strcpy(tabname,tbname);
	  lgprintf("关闭'%s'表...",tabname);
	  {
		  lgprintf("删除DP参数文件.");
		  char basedir[300];
		  char streamPath[300];
		  char tbntmp[200];
		  strcpy(basedir,GetMySQLPathName(0,"msys"));
		  strcpy(tbntmp,tabname);
		  char *psep=strstr(tbntmp,".");
		  if(psep==NULL) 
			  ThrowWith("Invalid table name format'%s',should be dbname.tbname.",tbname);
		  *psep='/';
		  sprintf(streamPath,"%s%s.DTP",basedir,tbntmp);
		  unlink(streamPath);
	  }
    	  conn.FlushTables(tabname);
		 lgprintf("表'%s'已清空...",tabname);
  }	
  
  //建立表并更新复用字段值(dt_index.reusecols)
  //数据上线有以下几种情况
  //1. 有完整的两组数据文件,需要备份原有的表和数据记录.
  //2. 上线前表中无数据,不需要备份原有的表和数据记录.
  //3. 上线前后使用同一组数据文件,但要使用新的表和索引结构替换原数据,这时也不需要备份原有的表和数据记录.
  
  void SysAdmin::DataOnLine(int tabid) {
	  char tbname[150],idxname[150];
	  char tbname_p[150],idxname_p[150];
	  bool ec=wociIsEcho();
	  wociSetEcho(FALSE);
	  lgprintf("%d表上线...",tabid);
	  AutoMt mt(dts,100);
	  mt.FetchAll("select sum(recordnum) rn,sum(filesize) tsize,sum(idxfsize) itsize ,count(*) fnum from dp.dp_datafilemap where tabid=%d and fileflag=1 and isfirstindex=1",tabid);
	  mt.Wait();
	  int sumrn=mt.GetInt("rn",0);
	  double tsize=mt.GetDouble("tsize",0);
	  double itsize=mt.GetDouble("itsize",0);
	  double idxtbsize=0;
	  int fnum=mt.GetInt("fnum",0);
	  mt.FetchAll("select sum(recordnum) rn,sum(filesize) tsize,sum(idxfsize) itsize,count(*) fnum from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1",tabid);
	  mt.Wait();
	  bool needbackup=mt.GetInt("rn",0)>0;
	  bool sharemode=false;
	  if(sumrn==0 && !needbackup) 
		  ThrowWith("上线的数据为空，上线失败(tabid:%d)。",tabid);
	  if(sumrn==0) {
		  needbackup=false; //不需要备份
		  sharemode=true;
		  sumrn=mt.GetInt("rn",0);
		  tsize=mt.GetDouble("tsize",0);
		  itsize=mt.GetDouble("itsize",0);
		  fnum=mt.GetInt("fnum",0);
	  }
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("找不到对应于%d表的任何索引。",tabid);
	  //后续操作涉及dt_table,dt_datafilemap等核心参数表的修改，需要关闭表。
	  AutoStmt st(dts);
	  try {
	  CloseTable(tabid,NULL,false);
	  if(needbackup) {
		  st.Prepare("update dp.dp_datafilemap set fileflag=2 where tabid=%d and fileflag=0",tabid);
		  st.Execute(1);
		  st.Wait();
	  }
	  if(!sharemode) {
		  st.Prepare("update dp.dp_datafilemap set fileflag=0 where tabid=%d and fileflag=1",tabid);
		  st.Execute(1);
		  st.Wait();
	  }
	  
	  //if(!needbackup) DropDTTable(tabid,TBNAME_DEST);
	  for(int i=0;i<rn;i++) {
		  int indexid=mt.GetInt("indexgid",i);
		  GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST);
		  if(needbackup) {
			  GetTableName(tabid,indexid,tbname_p,idxname_p,TBNAME_FORDELETE);
			  conn.RenameTable(idxname,idxname_p,true);
		  }
		  GetTableName(tabid,indexid,tbname_p,idxname_p,TBNAME_PREPONL);
		  conn.RenameTable(idxname_p,idxname,true);
		  idxtbsize+=StatMySQLTable(GetMySQLPathName(0,"msys"),idxname);
	  }
	  if(needbackup) {
		  GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_FORDELETE);
		  conn.RenameTable(tbname,tbname_p,true);
	  }
	  GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_PREPONL);
	  conn.RenameTable(tbname_p,tbname,true);
	  
	  mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1 order by datapartid,indexgid,fileid",
		  tabid);
	  rn=mt.Wait();
	  
	  st.Prepare("update dp.dp_table set recordnum=%d,firstdatafileid=%d,totalbytes=%15.0f,"
		  "datafilenum=%d where tabid=%d",
		  sumrn,mt.GetInt("fileid",0),tsize,fnum,tabid);
	  st.Execute(1);
	  st.Wait();
	  wociCommit(dts);
	  BuildDTP(tbname);
	  lgprintf("表'%s'成功上线,记录数%d,数据%.0f,索引%.0f. MySQL刷新...",tbname,sumrn,tsize+itsize,idxtbsize);
	  conn.FlushTables(tbname);
	  wociSetEcho(ec);
	  
	  log(tabid,0,115,"表已成功上线,记录数%d,数据%.0f,索引%.0f. ",sumrn,tsize+itsize,idxtbsize);
	}
	catch(...) {
	  if(!sharemode) {
		  lgprintf("恢复文件类型,正常->待装入");
		  st.Prepare("update dp.dp_datafilemap set fileflag=1 where tabid=%d and fileflag=0",tabid);
		  st.Execute(1);
		  st.Wait();
	  }
	  if(needbackup) {
		  lgprintf("恢复文件类型,待删除->正常");
		  st.Prepare("update dp.dp_datafilemap set fileflag=0 where tabid=%d and fileflag=2",tabid);
		  st.Execute(1);
		  st.Wait();
	  }
	  throw;
	}		
		
  }
  
  void SysAdmin::BuildDTP(const char *tbname)
  {
	  lgprintf("建立DP参数文件.");
	  char basedir[300];
	  char streamPath[300];
	  char tbntmp[200];
	  strcpy(basedir,GetMySQLPathName(0,"msys"));
	  
	  dtioStream *pdtio=new dtioStreamFile(basedir);
	  strcpy(tbntmp,tbname);
	  char *psep=strstr(tbntmp,".");
	  if(psep==NULL) 
		  ThrowWith("Invalid table name format'%s',should be dbname.tbname.",tbname);
	  *psep=0;
	  psep++;
#ifdef WIN32
	  sprintf(streamPath,"%s%s\\%s.DTP",basedir,tbntmp,psep);
#else
	  sprintf(streamPath,"%s%s/%s.DTP",basedir,tbntmp,psep);
#endif
	  pdtio->SetStreamName(streamPath);
	  pdtio->SetWrite(false);
	  pdtio->StreamWriteInit(DTP_BIND);
	  
	  pdtio->SetOutDir(basedir);
	  {
		  dtioDTTable dtt(tbntmp,psep,pdtio,false);
		  dtt.FetchParam(dts);
		  dtt.SerializeParam();
	  }
	  pdtio->SetWrite(true);
	  pdtio->StreamWriteInit(DTP_BIND);
	  pdtio->SetOutDir(basedir); 
	  {
		  dtioDTTable dtt(tbntmp,psep,pdtio,false);
		  dtt.FetchParam(dts);
		  dtt.SerializeParam();
	  }
	  delete pdtio;
  }
  
  //表名必须是全名(含数据库名)
  void SysAdmin::GetPathName(char *path,const char *tbname,const char *surf) {
	  char dbname[150];
	  strcpy(dbname,tbname);
	  char *dot=strstr(dbname,".");
	  if(dot==NULL) 
		  ThrowWith("表名必须是全名('%s')",tbname);
	  char *mtbname=dot+1;
	  *dot=0;
	  const char *pathval=GetMySQLPathName(0,"msys");
#ifdef WIN32
	  sprintf(path,"%s\\%s\\%s.%s",pathval,dbname,mtbname,surf);
#else
	  sprintf(path,"%s/%s/%s.%s",pathval,dbname,mtbname,surf);
#endif
  }
  
  //type TBNAME_DEST: destination name
  //type TBNAME_PREPONL: prepare for online
  //type TBNAME_FORDELETE: fordelete 
  
  void SysAdmin::GetTableName(int tabid,int indexid,char *tbname,char *idxname,int type) {
	  char tbname1[150],idxname1[150],dbname[130];
	  idxname1[0]=0;
	  if(tabid==-1) 
		  ThrowWith("Invalid tabid parameter on call SysAdmin::GetTableName.");
	  AutoMt mt(dts,10);
	  mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("Tabid is invalid  :%d",tabid);
	  strcpy(tbname1,mt.PtrStr("tabname",0));
	  strcpy(dbname,mt.PtrStr("databasename",0));
	  if(indexid!=-1) {
		  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>=1",tabid,indexid);
		  int rn=mt.Wait();
		  if(rn<1)
		  	 ThrowWith("tabid:%d,indexid:%d is invalid or not a soled.",tabid,indexid);
		  if(strlen(mt.PtrStr("indextabname",0))>0)
		    strcpy(idxname1,mt.PtrStr("indextabname",0));
		  else sprintf(idxname1,"%sidx%d",tbname1,indexid);
	  }
//	  if(STRICMP(idxname1,tbname1)==0) 
//	   ThrowWith("索引表名'%s'和目标名重复:indexid:%d,tabid:%d!",indexid,tabid);
	  if(type!=TBNAME_DEST) {
		  if(indexid!=-1) {
			  strcat(idxname1,"_");
			  strcat(idxname1,dbname);
		  }
		  strcat(tbname1,"_");
		  strcat(tbname1,dbname);
		  if(type==TBNAME_PREPONL)
			  strcpy(dbname,PREPPARE_ONLINE_DBNAME);
		  else if(type==TBNAME_FORDELETE)
			  strcpy(dbname,FORDELETE_DBNAME);
		  else ThrowWith("Invalid table name type :%d.",type);
	  }
	  if(tbname) sprintf(tbname,"%s.%s",dbname,tbname1);
	  if(indexid!=-1 && idxname)
		  sprintf(idxname,"%s.%s",dbname,idxname1);
  }
  
  void SysAdmin::CreateAllIndex(int tabid,int nametype,bool forcecreate,int ci_type)
  {
	  AutoMt mt(dts,MAX_DST_INDEX_NUM);
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1 order by seqindattab",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("找不到%d表的独立索引。",tabid);
	  for(int i=0;i<rn;i++)
		  CreateIndex(tabid,mt.GetInt("indexgid",i),nametype,forcecreate,ci_type);
  }
  
  void SysAdmin::RepairAllIndex(int tabid,int nametype)
  {
	  AutoMt mt(dts,MAX_DST_INDEX_NUM);
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1 order by seqindattab",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("找不到%d表的独立索引。",tabid);
	  for(int i=0;i<rn;i++) {
		  int indexid=mt.GetInt("indexgid",i);
		  char tbname[100],idxname[100];
		  GetTableName(tabid,indexid,tbname,idxname,nametype);
		  lgprintf("索引表刷新...");
		  FlushTables(idxname);
		  lgprintf("索引表重建:%s...",idxname);
		  
		  char fn[500];
		  GetPathName(fn,idxname,"MYI");
		  char cmdline[500];
		  // -n 选项用于强制使用排序方式修复
		  sprintf(cmdline,"myisamchk -rqnv --tmpdir=\"%s\" %s ",GetMySQLPathName(0,"msys"),fn);
		  int rt=system(cmdline);
		  wait(&rt);
		  if(rt)
			  ThrowWith("索引重建失败!");
		  //char sqlbf[3000];
		  //sprintf(sqlbf,"repair table %s quick",idxname);
		  //conn.DoQuery(sqlbf);
	  }
  }
  
  void SysAdmin::CreateIndex(int tabid,int indexid,int nametype,bool forcecreate,int ci_type)
  {
	  AutoMt mt(dts,10);
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>=1",tabid,indexid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("Indexgid is invalid or not a soled :%d",indexid);
	  char colsname[300];
	  strcpy(colsname,mt.PtrStr("columnsname",0));
	  char tbname[100],idxname[100];
	  GetTableName(tabid,indexid,tbname,idxname,nametype);
	  //建立独立索引：
	  if(ci_type==CI_ALL || ci_type==CI_IDX_ONLY)
		  CreateIndex(idxname,mt.GetInt("seqinidxtab",0),colsname,forcecreate);
	  if(ci_type==CI_ALL || ci_type==CI_DAT_ONLY)
		  CreateIndex(tbname,mt.GetInt("seqindattab",0),colsname,forcecreate);
	  
	  //建立依赖索引：
	  //查找该独立索引附带的非独立索引，并以此为依据建立索引表
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex=0 order by seqinidxtab",
		  tabid,indexid);
	  int srn=mt.Wait();
	  for(int j=0;j<srn;j++) {
		  strcpy(colsname,mt.PtrStr("columnsname",j));
		  if(ci_type==CI_ALL || ci_type==CI_IDX_ONLY)
			  CreateIndex(idxname,mt.GetInt("seqinidxtab",j),colsname,forcecreate);
		  if(ci_type==CI_ALL || ci_type==CI_DAT_ONLY)
			  CreateIndex(tbname,mt.GetInt("seqindattab",j),colsname,forcecreate);
	  }
  }
  
  void SysAdmin::CreateIndex(const char *dtname, int id, const char *colsname, bool forcecreate)
  {
	  //create index语句中的索引名称不允许带数据库名。
	  const char *tbname=strstr(dtname,".");
	  if(tbname==NULL) tbname=dtname;
	  else tbname++;
	  char sqlbf[300];
	  if(forcecreate) {
		  sprintf(sqlbf,"drop index %s_%d on %s",tbname,
			  id,dtname);
		  conn.DoQuery(sqlbf);
	  }
	  sprintf(sqlbf,"create index %s_%d on %s(%s)",
		  tbname,id,
		  dtname,colsname);
	  lgprintf("建立索引:%s.",sqlbf);
	  conn.DoQuery(sqlbf);
  }
#ifdef WORDS_BIGENDIAN
#define revlint(v) v
#else
#define revlint(V)   { char def_temp[8];\
	((mbyte*) &def_temp)[0]=((mbyte*)(V))[7];\
	((mbyte*) &def_temp)[1]=((mbyte*)(V))[6];\
	((mbyte*) &def_temp)[2]=((mbyte*)(V))[5];\
	((mbyte*) &def_temp)[3]=((mbyte*)(V))[4];\
	((mbyte*) &def_temp)[4]=((mbyte*)(V))[3];\
	((mbyte*) &def_temp)[5]=((mbyte*)(V))[2];\
	((mbyte*) &def_temp)[6]=((mbyte*)(V))[1];\
	((mbyte*) &def_temp)[7]=((mbyte*)(V))[0];\
  memcpy(V,def_temp,sizeof(LLONG)); }
#endif                          
  
  int DestLoader::Load(bool directIOSkip) {
	  //Check deserved temporary(middle) fileset
	  AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	  mdf.FetchAll("select * from dp.dp_datapart where (status=3 or status=30) limit 2");
	  int rn=mdf.Wait();
	  if(rn<1) {
		  printf("没有发现处理完成等待装入的数据(任务状态=3).\n");
		  return 0;
	  }
	  char sqlbf[1000];
	  tabid=mdf.GetInt("tabid",0);
	  datapartid=mdf.GetInt("datapartid",0);
	  bool preponl=mdf.GetInt("status",0)==3;
	  lgprintf("装入类型:%s.",preponl?"新装入":"重装入");
	  mdf.FetchAll("select * from dp.dp_datapart where status<3 and tabid=%d limit 2",tabid);
	  rn=mdf.Wait();
	  if(rn>0) {
		  printf("数据装入时，表%d中一些数据分组还未整理,例如分组%d(状态%d).\n",tabid,
		          mdf.GetInt("datapartid",0),mdf.GetInt("status",0));
		  return 0;
	  }
	  
	  mdf.FetchAll("select * from dp.dp_datapart where status!=%d and tabid=%d limit 2",tabid,preponl?3:30);
	  rn=mdf.Wait();
	  if(rn>0) {
		  if(!preponl) {
			  lgprintf("数据装入时，数据组状态为重新装入(30)，但并非所有分区都设置为30，例如表%d,分组号%d,(状态%d).\n",tabid,
				  mdf.GetInt("datapartid",0),mdf.GetInt("status",0));
			  return 0;
		  }
		  mdf.FetchAll("select * from dp.dp_datapart where tabid=%d and status=3",tabid);
		  rn=mdf.Wait();
		  AutoStmt st(psa->GetDTS());
		  for(int i=0;i<rn;i++) {
	  		datapartid=mdf.GetInt("datapartid",i);
	  		lgprintf("替换表%d的分区%d数据.",tabid,datapartid);
	  		AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);
		  	mt.FetchAll("select count(*) fn,sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and datapartid=%d and fileflag=0 and isfirstindex=1",
		  	 tabid,datapartid);
		  	mt.Wait();
		  	if(mt.GetInt("fn",0)>0) {
	  		 lgprintf("表%d的分区%d主索引原有的数据为%d个文件，%.0f条记录.",tabid,datapartid,mt.GetInt("fn",0),mt.GetDouble("rn",0));
		         st.DirectExecute(" update dp.dp_datafilemap set fileflag=2 where tabid=%d and datapartid=%d and fileflag=0 ",tabid,datapartid);
			}
			else lgprintf("表%d的分区%d替换前没有数据.",tabid,datapartid);
		  	mt.FetchAll("select count(*) fn,sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and datapartid=%d and fileflag=1 and isfirstindex=1",
		  	 tabid,datapartid);
		  	mt.Wait();
		  	if(mt.GetInt("fn",0)>0) {
	  		 lgprintf("表%d的分区%d主索引新的数据为%d个文件，%.0f条记录.",tabid,datapartid,mt.GetInt("fn",0),mt.GetDouble("rn",0));
		         st.DirectExecute(" update dp.dp_datafilemap set fileflag=0 where tabid=%d and datapartid=%d and fileflag=1 ",tabid,datapartid);
		  	}
		  }
		  st.DirectExecute(" update dp.dp_datapart set status=30 where tabid=%d",tabid);
		  lgprintf("表的分组已变换为重新装入(30).",tabid);
		  preponl=false;
	  }
	  
	  wociSetTraceFile("dl数据装载/");
	  psa->OutTaskDesc("数据装载 :",tabid);
	  
	  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus=0 and fileflag=%d limit 2",
		  tabid,preponl?1:0);
	  if(mdf.Wait()<1) {
		  errprintf("数据组%d(%d)指示已完成数据整理，但找不到对应的数据记录。\n可能是数据文件记录不存在或状态非空闲(0).\n",
		    tabid,datapartid);
		  return 0;
	  }
	  tabid=mdf.GetInt("tabid",0);
	  datapartid=mdf.GetInt("datapartid",0);
	  
	  mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	  if(mdf.Wait()<1) {
		  errprintf("找不到数据文件对应的目标表(%d).\n",tabid);
		  return 0;
	  }
	  //char dbname[300];
	  //strcpy(dbname,mdf.PtrStr("databasename",0));
	  //conn.SelectDB(dbname);
	  //partid=mdf.GetInt("partid",0);
	  //dumpparam dpsrc;
	  //psa->GetSoledIndexParam(srctabid,&dpsrc);
	  
	  psa->GetSoledIndexParam(-1,&dp,tabid);
	  AutoMt idxmt(psa->GetDTS(),10);
	  idxmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1",tabid);
	  int idxrn=idxmt.Wait();
	  int totdatrn=0;
	  psa->log(tabid,0,116,"数据装入,独立索引数%d,日志文件 '%s' .",idxrn,lgfile);
	  try {
		  //为防止功能重入,数据文件状态修改.
		  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag=%d order by indexgid,fileid",
			  tabid,preponl?1:0);
		  totdatrn=rn=mdf.Wait();
		  lgprintf("修改数据文件的处理状态(tabid:%d,%d个文件)：0-->1",tabid,rn);
		  sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=1 where tabid=%d and "
			  "procstatus=0 and fileflag=%d",tabid,preponl?1:0);
		  if(psa->DoQuery(sqlbf)!=rn) 
			  ThrowWith("修改数据文件的处理状态异常，可能是与其它进程冲突。\n"
			  "   tabid:%d.\n",tabid);
	  for(int i=0;i<idxrn;i++) {
		  indexid=idxmt.GetInt("indexgid",i);
		  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d  and indexgid=%d and fileflag=%d order by fileid",
			  tabid,indexid,preponl?1:0);
		  rn=mdf.Wait();
		  if(rn<1) ThrowWith("找不到数据文件(indexgid:%d,tabid:%d,datapartid:%d,fileflag:%d).",indexid,tabid,datapartid,preponl?1:0);
		  //索引数据表的结构从文件提取，比从template中提取更准确。
		  int off=dp.GetOffset(indexid);
		  //{
		  dt_file idxf;
		  idxf.Open(mdf.PtrStr("idxfname",0),0);
		  AutoMt indexmt(0);
		  indexmt.SetHandle(idxf.CreateMt(10));
		  /*
		  if(conn.TouchTable(dp.idxp[off].idxtbname)) {
		  printf("table %s has exist,dropped.\n",dp.idxp[off].idxtbname);
		  sprintf(sqlbf,"drop table %s",dp.idxp[off].idxtbname);
		  conn.DoQuery(sqlbf);
		  }
		  //建立目标标及其表结构的描述文件
		  wociGetCreateTableSQL(idxmt,sqlbf,dp.idxp[off].idxtbname,true);
		  conn.DoQuery(sqlbf);
		  */
		  //}
		 
		  AutoMt datmt(psa->GetDTS(),MAX_DST_DATAFILENUM);
		  datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d "
			  " and indexgid=%d and procstatus=1 and fileflag=%d order by fileid",
			  tabid,indexid,preponl?1:0);
		  int datrn=datmt.Wait();
		  //防止功能重入或并行运行时的重复执行
		  if(datrn<1) continue;
		  char fn[300];
		  bool isfixed=false;
		  int k=0;
		  const char *pathval=psa->GetMySQLPathName(0,"msys");		
			  for(k=0;k<datrn;k++) {
				  //Build table data file link information.
				  if(k+1==datrn) {
					  dt_file df;
					  df.Open(datmt.PtrStr("filename",k),2,datmt.GetInt("fileid",k));
					  df.SetFileHeader(0,NULL);
				  }
				  else {
					  dt_file df;
					  df.Open(datmt.PtrStr("filename",k),2,datmt.GetInt("fileid",k));
					  df.SetFileHeader(0,datmt.PtrStr("filename",k+1));
				  }
			  }
			  lgprintf("开始数据装入(DestLoading),文件数:%d,tabid:%d,indexid:%d ...",
				  rn,tabid,indexid);
			  //原来的方式为不建立索引(FALSE参数)
			  //2005/12/01修改为建立索引（在空表之上),在索引重建过程中(::RecreateIndex,taskstaus 4->5),
			  //  使用repair table ... quick来快速建立索引结构。
			  // BUG FIXING
			  //**
			  psa->CreateIndexTable(tabid,indexid,indexmt,-1,TBNAME_PREPONL,TRUE,CI_IDX_ONLY);
			  //psa->CreateIndexTable(indexid,indexmt,-1,TBNAME_PREPONL,FALSE);
			  char tbname[150],idxname[150];
			  psa->GetTableName(tabid,indexid,tbname,idxname,TBNAME_PREPONL);
			  psa->GetPathName(fn,idxname,"MYD");
			  //struct _finddata_t ft;
			  FILE *fp =NULL;
			  if(!directIOSkip) {
				  fp=fopen(fn,"wb");
				  if(fp==NULL) 
					  ThrowWith("Open file %s for writing failed!",fn);
			  }
			  LLONG totidxrn=0;
			  //lgprintf("索引文件：%s",mdf.PtrStr("filename",0));
			  for(k=0;k<rn;k++) {
				  //Build index data file link information.
				  if(k+1==rn) {
					  dt_file df;
					  df.Open(datmt.PtrStr("idxfname",k),2,datmt.GetInt("fileid",k));
					  df.SetFileHeader(0,NULL);
					  //lgprintf(
				  }
				  else {
					  dt_file df;
					  df.Open(datmt.PtrStr("idxfname",k),2,datmt.GetInt("fileid",k));
					  df.SetFileHeader(0,datmt.PtrStr("idxfname",k+1));
				  }
				  file_mt idxf;
				  idxf.Open(datmt.PtrStr("idxfname",k),0);
				  int rn_fromtab=datmt.GetInt("idxrn",k);
				  int mt=idxf.ReadBlock(0,0);
				  isfixed=wociIsFixedMySQLBlock(mt);
				  LLONG startat=totidxrn;
				  try {
					  AutoStmt st(psa->GetDTS());
					  st.DirectExecute("use preponl");
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("用户取消操作!");
					  }
					  lgprintf("生成索引数据...");
					  while(mt) {
						  if(!directIOSkip)
							  wociCopyToMySQL(mt,0,0,fp);
						  else wociAppendToDbTable(mt,idxname,psa->GetDTS(),true);
						  totidxrn+=wociGetMemtableRows(mt);
						  if(totidxrn-startat<rn_fromtab)
							  mt=idxf.ReadBlock(-1,0);
						  else break;
					  }
				  }
				  catch(...) {
					  AutoStmt st(psa->GetDTS());
					  st.DirectExecute("use dp");
					  throw;
				  }
				  {
					  AutoStmt st(psa->GetDTS());
					  st.DirectExecute("use dp");
					  lgprintf("索引数据:%lld行.",totidxrn);
				  }
			  }
			  if(!directIOSkip) {
				  fclose(fp);
				  fp=fopen(fn,"rb");
				  fseeko(fp,0,SEEK_END);
				  LONG64 fsz=ftello(fp);//_filelength(_fileno(fp));
				  fclose(fp);
				  //索引数据表的结构从文件提取，比从template中提取更准确。
				  psa->GetPathName(fn,idxname,"MYI");
				  char tmp[20];
				  memset(tmp,0,20);
				  
				  revlint(&totidxrn);
				  revlint(&fsz);
				  fp=fopen(fn,"r+b");
				  if(fp==NULL) 
					  ThrowWith("无法打开文件'%s'，请检查目录参数设置(dt_path)是否正确。",fn);
				  fseek(fp,28,SEEK_SET);
				  dp_fwrite(&totidxrn,1,8,fp);
				  // reset deleted records count.
				  dp_fwrite(tmp,1,8,fp);
				  fseek(fp,68,SEEK_SET);
				  dp_fwrite(&fsz,1,8,fp);
				  fseek(fp,0,SEEK_END);
				  fclose(fp); 
				  lgprintf("索引表刷新...");
				  psa->FlushTables(idxname);
				  revlint(&fsz);
				  // BUG FIXING
				  //**ThrowWith("调试中断");
				  
				  if(fsz>1024*1024) {
					  lgprintf("压缩索引表:%s....",idxname);
					  char cmdline[300];
					  strcpy(fn+strlen(fn)-3,"TMD");
					  unlink(fn);
					  strcpy(fn+strlen(fn)-3,"MYI");
					  printf("pack:%s\n",fn);
					  sprintf(cmdline,"myisampack -v %s",fn);
					  //sprintf(cmdline,"gdb myisampack",fn);
					  int rt=system(cmdline) ;
					  wait (&rt);
					  if(rt)
						  ThrowWith("索引表%s压缩失败.",idxname);
					  lgprintf("压缩成功。");
				  }
			  }
		  }
	}
	catch(...) {
		  lgprintf("恢复数据文件的处理状态(tabid %d(%d),indexid:%d,%d个文件)：1-->0",tabid,datapartid,indexid,totdatrn);
		  sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d  and fileflag=%d",
			  tabid,preponl?1:0);
		  psa->DoQuery(sqlbf);
		  char tbname[150],idxname[150];
	  	  psa->log(tabid,0,123,"数据装入出现异常错误,已恢复处理状态");
		  // BUG FIXING
		  /*
		  psa->GetTableName(tabid,indexid,tbname,idxname,TBNAME_PREPONL);
		  char fn[300];
		  psa->GetPathName(fn,idxname,"MYD");
		  unlink(fn);
		  psa->GetPathName(fn,idxname,"MYI");
		  unlink(fn);
		  psa->GetPathName(fn,idxname,"frm");
		  unlink(fn);
		  */
		  throw;
	}
	lgprintf("数据装入(DestLoading)结束 ...");
	AutoStmt updst(psa->GetDTS());
	updst.Prepare("update dp.dp_datapart set status=%d where tabid=%d ",
		preponl?4:40,tabid);
	updst.Execute(1);
	updst.Wait();
	lgprintf("任务状态更新,3(MLoaded)--->4(DLoaded),表:%d",tabid);
	psa->log(tabid,0,117,"数据装入结束，等待建立索引.");
	//ThrowWith("DEBUG_BREAK_HEAR.");
	return 1;
}

// 源表为DBPLUS管理的目标表，且记录数非空。
int DestLoader::MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname)
{
	char dtpath[300];
	lgprintf("目标表改名(转移) '%s.%s -> '%s.%s'.",srcdbn,srctabname,dstdbn,dsttabname);
	sprintf(dtpath,"%s.%s",srcdbn,srctabname);
	if(!psa->TouchTable(dtpath))
	  ThrowWith("源表没找到");
	sprintf(dtpath,"%s.%s",dstdbn,dsttabname);
	if(psa->TouchTable(dtpath)) {
		sprintf(dtpath,"表%s.%s已存在，要覆盖吗?(Y/N)",dstdbn,dsttabname);
		if(!GetYesNo(dtpath,false)) {
			lgprintf("取消操作。 ");
			return 0;
		}			
	}
	AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	int rn;
	//mt.FetchAll("select pathval from dp.dp_path where pathtype='msys'");
	//int rn=mt.Wait();
	//i/f(rn<1) 
	//	ThrowWith("找不到MySQL数据目录(dt_path.pathtype='msys'),数据转移异常中止.");
	strcpy(dtpath,psa->GetMySQLPathName(0,"msys"));
	if(STRICMP(srcdbn,dstdbn)==0 && STRICMP(srctabname,dsttabname)==0) 
		ThrowWith("源表和目标表名称不能相同.");
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",dsttabname,dstdbn);
	rn=mt.Wait();
	if(rn>0) {
		ThrowWith("表'%s.%s'已存在(记录数:%d)，操作失败!",dstdbn,dsttabname,mt.GetInt("recordnum",0));
	}
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
	rn=mt.Wait();
	if(rn<1) {
		ThrowWith("源表'%s.%s'不存在.",srcdbn,srctabname);
	}
	int dsttabid=psa->NextTableID();
	tabid=mt.GetInt("tabid",0);
	int recordnum=mt.GetInt("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	double totalbytes=mt.GetDouble("totalbytes",0);
	int datafilenum=mt.GetInt("datafilenum",0);
	//if(recordnum<1) {
	//	lgprintf("源表'%s'数据为空，数据转移失败。",srctabname);
	//	return 0;
	//}
	lgprintf("源表'%s.%s' id:%d,记录数:%d,起始数据文件号 :%d",
		srcdbn,srctabname,tabid,recordnum,firstdatafileid);
	
	//新表不存在，在dt_table中新建一条记录
	*mt.PtrInt("tabid",0)=dsttabid;
	strcat(mt.PtrStr("tabdesc",0),"_r");
	strcpy(mt.PtrStr("tabname",0),dsttabname);
	strcpy(mt.PtrStr("databasename",0),dstdbn);
	wociAppendToDbTable(mt,"dp.dp_table",psa->GetDTS(),true);
	psa->CloseTable(tabid,NULL,false);
	CopyMySQLTable(dtpath,srcdbn,srctabname,dstdbn,dsttabname);
	//暂时关闭源表的数据访问，记录数已存在本地变量recordnum。
	//目标表的.DTP文件已经存在,暂时屏蔽访问
	psa->CloseTable(dsttabid,NULL,false);
	char sqlbuf[1000];
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d order by datapartid ",tabid);
	rn=mt.Wait();
	//创建索引记录和索引表，修改索引文件和数据文件的tabid 指向
	int i=0;
	for(i=0;i<rn;i++) 
		*mt.PtrInt("tabid",i)=dsttabid;
	wociAppendToDbTable(mt,"dp.dp_datapart",psa->GetDTS(),true);

	mt.FetchAll("select * from dp.dp_index where tabid=%d order by seqindattab ",tabid);
	rn=mt.Wait();
	//创建索引记录和索引表，修改索引文件和数据文件的tabid 指向
	for(i=0;i<rn;i++) {
		*mt.PtrInt("tabid",i)=dsttabid;
		strcpy(mt.PtrStr("indextabname",i),"");
	}
	wociAppendToDbTable(mt,"dp.dp_index",psa->GetDTS(),true);
	
	for(i=0;i<rn;i++) {
		if(mt.GetInt("issoledindex",i)>=1) {
			char tbn1[300],tbn2[300];
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST);
			psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST);
			lgprintf("索引表 '%s'-->'%s...'",tbn1,tbn2);
			MoveMySQLTable(dtpath,srcdbn,strstr(tbn1,".")+1,dstdbn,strstr(tbn2,".")+1);
		}
	}
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d order by fileid",tabid);
	rn=mt.Wait();
	AutoMt idxmt(psa->GetDTS(),MAX_DST_DATAFILENUM);

	for(i=0;i<rn;i++) {
		  char fn[300];
		  psa->GetMySQLPathName(mt.GetInt("pathid",i));
		  sprintf(fn,"%s%s.%s_%d_%d_%d.dat",psa->GetMySQLPathName(mt.GetInt("pathid",i)),
		    dstdbn,dsttabname,mt.GetInt("datapartid",i),mt.GetInt("indexgid",i),mt.GetInt("fileid",i));
		  
		  FILE *fp;
		  fp=fopen(mt.PtrStr("filename",i),"rb");
		  if(fp==NULL) ThrowWith("找不到文件'%s'.",mt.PtrStr("filename",i));
		  fclose(fp);
		  fp=fopen(fn,"rb");
		  if(fp!=NULL) ThrowWith("文件'%s'已经存在.",fn);
      		  rename(mt.PtrStr("filename",i),fn);
		  strcpy(mt.PtrStr("filename",i),fn);
		  *mt.PtrInt("tabid",i)=dsttabid;

		  //psa->GetMySQLPathName(idxmt.GetInt("pathid",i));
		  sprintf(fn,"%s%s.%s_%d_%d_%d.idx",psa->GetMySQLPathName(mt.GetInt("pathid",i)),
		    dstdbn,dsttabname,mt.GetInt("datapartid",i),mt.GetInt("indexgid",i),mt.GetInt("fileid",i));
		  rename(mt.PtrStr("idxfname",i),fn);
		  strcpy(mt.PtrStr("idxfname",i),fn);
	}
	wociAppendToDbTable(mt,"dp.dp_datafilemap",psa->GetDTS(),true);
	
	sprintf(sqlbuf,"delete from dp.dp_datafilemap where tabid=%d",tabid);
	psa->DoQuery(sqlbuf);
	sprintf(sqlbuf,"update dp.dp_log set tabid=%d where tabid=%d",dsttabid,tabid);
	psa->DoQuery(sqlbuf);
	sprintf(sqlbuf,"update dp.dp_table set recordnum=0,cdfileid=0  where tabid=%d ",tabid);
	psa->DoQuery(sqlbuf);
	psa->log(dsttabid,0,118,"数据从%s.%s表转移而来.",srcdbn,srctabname);
	//Move操作结束,打开目标表
	lgprintf("MySQL刷新...");
	char tbn[300];
	sprintf(tbn,"%s.%s",dstdbn,dsttabname);
	psa->BuildDTP(tbn);
	psa->FlushTables(tbn);
	lgprintf("删除源表..");
	RemoveTable(srcdbn,srctabname,false);
	lgprintf("数据已从表'%s'转移到'%s'。",srctabname,dsttabname);
	return 1;
}


// 7,10的任务状态处理:
//   1. 从文件系统获取二次压缩后的文件大小.
//   2. 任务状态修改为(8,11) (?? 可以省略)
//   3. 关闭目标表(unlink DTP file,flush table).
//   4. 修改数据/索引映射表中的文件大小和压缩类型.
//   5. 数据/索引 文件替换.
//   6. 任务状态修改为30(等待重新装入).
int DestLoader::ReLoad() {
	
	AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	mdf.FetchAll("select * from dp.dp_datapart where status in (7,10) limit 2");
	int rn=mdf.Wait();
	if(rn<1) {
		printf("没有发现重新压缩完成等待装入的数据.\n");
		return 0;
	}
	wociSetTraceFile("drcl重新压缩后装入/");
	bool dpcp=mdf.GetInt("status",0)==7;
	int compflag=mdf.GetInt("compflag",0);
	tabid=mdf.GetInt("tabid",0); 
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) order by indexgid",tabid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("装入二次压缩数据时找不到数据文件记录。");
		return 1;
	}
	mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("装入二次压缩数据时找不到dp.dp_table记录(tabid:%d).",tabid);
		return 1;
	}
	char dbname[100],tbname[100];
	strcpy(dbname,mdf.PtrStr("databasename",0));
	strcpy(tbname,mdf.PtrStr("tabname",0));
	AutoMt datmt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",
	               tabid);
	int datrn=datmt.Wait();
	rn=datrn;
	AutoStmt updst(psa->GetDTS());
	char tmpfn[300];
	int k;
	unsigned long dtflen[MAX_DST_DATAFILENUM];
	unsigned long idxflen[MAX_DST_DATAFILENUM];
	//先检查
	for(k=0;k<datrn;k++) {
		sprintf(tmpfn,"%s.%s",datmt.PtrStr("filename",k),dpcp?"depcp":"dep5");
		dt_file df;
		df.Open(tmpfn,0);
		dtflen[k]=df.GetFileSize();
		if(dtflen[k]<1) 
			ThrowWith("file '%s' is empty!",tmpfn);
	}
	for(k=0;k<rn;k++) {
		sprintf(tmpfn,"%s.%s",datmt.PtrStr("idxfname",k),dpcp?"depcp":"dep5");
		dt_file df;
		df.Open(tmpfn,0);
		idxflen[k]=df.GetFileSize();
		if(idxflen[k]<1) 
			ThrowWith("file '%s' is empty!",tmpfn);
	}
	char sqlbf[200];
	sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d", dpcp?8:11,tabid);
	if(psa->DoQuery(sqlbf)<1) 
		ThrowWith("二次压缩数据重新装入过程修改任务状态异常，可能是与其它进程冲突(tabid:%d)。\n",tabid);
	
	//防止功能重入，修改任务状态
	//后续修改将涉及数据文件的替换,操作数据前先关闭表
	psa->CloseTable(tabid,NULL,false);
	lgprintf("数据已关闭.");
	//用新的数据文件替换原来的文件：先删除原文件，新文件名称更改为原文件并修改文件记录中的文件大小字段。
	lgprintf("开始数据和索引文件替换...");
	for(k=0;k<datrn;k++) {
		updst.Prepare("update dp.dp_datafilemap set filesize=%d,compflag=%d,idxfsize=%d where tabid=%d and fileid=%d and fileflag=0",
			dtflen[k],compflag,idxflen[k],tabid,datmt.GetInt("fileid",k));
		updst.Execute(1);
		updst.Wait();
		const char *filename=datmt.PtrStr("filename",k);
		unlink(filename);
		sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
		rename(tmpfn,filename);
		lgprintf("rename file '%s' as '%s'",tmpfn,filename);
		filename=datmt.PtrStr("idxfname",k);
		unlink(filename);
		sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
		rename(tmpfn,filename);
		lgprintf("rename file '%s' as '%s'",tmpfn,filename);
	}
	lgprintf("数据和索引文件已成功替换...");
	sprintf(sqlbf,"update dp.dp_datapart set status=30 where tabid=%d", tabid);
	psa->DoQuery(sqlbf);
	sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0",tabid);
	lgprintf("任务状态修改为数据整理结束(3),数据文件处理状态改为未处理(0).");
	Load();
	return 1;
}


int DestLoader::RecreateIndex(SysAdmin *_Psa) 
{
	AutoMt mdf(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
	mdf.FetchAll("select * from dp.dp_datapart where (status =4 or status=40 ) limit 2");
	int rn=mdf.Wait();
	if(rn<1) {
		return 0;
	}
	datapartid=mdf.GetInt("datapartid",0);
	tabid=mdf.GetInt("tabid",0);
	bool preponl=mdf.GetInt("status",0)==4;
	//if(tabid<1) ThrowWith("找不到任务号:%d中Tabid",taskid);
	
	//check intergrity.
	mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1",tabid);
	rn=mdf.Wait();
	if(rn<1) 
		ThrowWith("目标表%d缺少主索引记录.",tabid);
	
	mdf.FetchAll("select distinct indexgid from dp.dp_datafilemap where tabid=%d and fileflag=%d",tabid,preponl?1:0);
	rn=mdf.Wait();
	mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1",tabid);
	int rni=mdf.Wait();
	if(rni!=rn) 
	{
		lgprintf("出现错误: 装入独立索引数(%d)和索引参数表中的值(%d)不符.",
			rn,rni);
		return 0; //dump && destload(create temporary index table) have not complete.
	}
	char sqlbf[300];
	try {
		sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d", 20,tabid);
		if(psa->DoQuery(sqlbf)<1) 
			ThrowWith("数据装入重建索引过程修改任务状态异常，可能是与其它进程冲突。\n"
			"  tabid:%d.\n",
			tabid);
		
		lgprintf("开始索引重建,tabid:%d,总索引数 :%d",
			tabid,rn);
		psa->log(tabid,0,119,"开始建立索引.");
		char tbname[150],idxname[150];
		psa->GetTableName(tabid,-1,tbname,idxname,TBNAME_PREPONL);
		AutoMt destmt(0,10);
		psa->CreateDataMtFromFile(destmt,0,tabid,preponl?1:0);
		psa->CreateTableOnMysql(destmt,tbname,true);
		//2005/12/01 索引改为数据新增后重建(修复)。
		psa->CreateAllIndex(tabid,TBNAME_PREPONL,true,CI_DAT_ONLY);
		lgprintf("建立索引的过程可能需要较长的时间，请耐心等待...");
		psa->RepairAllIndex(tabid,TBNAME_PREPONL);
		psa->DataOnLine(tabid);
		lgprintf("索引建立完成.");
		mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and isfirstindex=1 and fileflag!=2 order by datapartid,indexgid,fileid",
			tabid);
		rn=mdf.Wait();
	}
	catch (...) {
		errprintf("建立索引结构时出现异常错误,恢复处理状态...");
		sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d", preponl?3:30,tabid);
		psa->DoQuery(sqlbf);
		sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and "
			"procstatus=1 and fileflag=%d",tabid,preponl?1:0);
		psa->DoQuery(sqlbf);
  	        psa->log(tabid,0,124,"建立索引结构时出现异常错误,已恢复处理状态.");
		throw;
	}
	
	AutoStmt st(psa->GetDTS());
	st.Prepare("update dp.dp_datapart set status=5 where tabid=%d",
		tabid);
	st.Execute(1);
	st.Wait();
	st.Prepare("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0",
		tabid);
	st.Execute(1);
	st.Wait();
	lgprintf("状态4(DestLoaded)-->5(Complete),tabid:%d.",tabid);
	
	lgprintf("删除中间临时文件...");
	mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d",tabid);
	{
		int dfn=mdf.Wait();
		for(int di=0;di<dfn;di++) {
			lgprintf("删除文件'%s'",mdf.PtrStr("datafilename",di));
			unlink(mdf.PtrStr("datafilename",di));
			lgprintf("删除文件'%s'",mdf.PtrStr("indexfilename",di));
			unlink(mdf.PtrStr("indexfilename",di));
		}
		lgprintf("删除记录...");
		st.Prepare("delete from dp.dp_middledatafile where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	} 
	psa->CleanData(false);
	psa->log(tabid,0,120,"索引建立完成.");
	return 1;
}

thread_rt LaunchWork(void *ptr) 
{
	((worker *) ptr)->work();
	thread_end;
}

//以下代码有错误
//up to 2005/04/13, the bugs of this routine continuous produce error occursionnaly .
//   ReCompress sometimes give up last block of data file,but remain original index record in idx file.
int DestLoader::ReCompress(int threadnum)
{
	AutoMt mdt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	mdt.FetchAll("select distinct tabid,datapartid,status,compflag from dp.dp_datapart where (status=6 or status=9)");
	int rn1=mdt.Wait();
	int rn;
	int i=0;
	bool deepcmp;
	if(rn1<1) {
		return 0;
	}
	wociSetTraceFile("ddc重新压缩/");
	for(i=0;i<rn1;i++) {

		tabid=mdt.GetInt("tabid",i);
		datapartid=mdt.GetInt("datapartid",i);
		deepcmp=mdt.GetInt("status",i)==6;
		mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus =0 and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",tabid);
		rn=mdf.Wait();
		if(rn<1) {
			mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus <>2 and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",tabid);
			rn=mdf.Wait();
			if(rn<1) {
				AutoStmt st1(psa->GetDTS());
				st1.Prepare("update dp.dp_datapart set status=%d where tabid=%d",
					deepcmp?7:10,tabid);
				st1.Execute(1);
				st1.Wait();
				st1.Prepare("update dp.dp_datafilemap set procstatus =0 where tabid=%d and fileflag=0",tabid);
				st1.Execute(1);
				st1.Wait();
				lgprintf("二次压缩任务已完成，任务状态已修改为%d,数据文件处理状态修改为空闲(0)",deepcmp?7:10);
			}
			else lgprintf("表%d(%d)---二次压缩任务未完成,但已没有等待压缩的数据",tabid,datapartid);
		}
		else break;
	}
	if(i==rn1) return 0;
	
	//防止重入，修改数据文件状态。
	psa->OutTaskDesc("数据重新压缩任务",mdt.GetInt("tabid",0),mdt.GetInt("datapartid",0));
	int compflag=mdt.GetInt("compflag",0);
	lgprintf("原压缩类型:%d, 新的压缩类型:%d .",mdf.GetInt("compflag",0),compflag);
	int fid=mdf.GetInt("fileid",0);
	psa->log(tabid,0,121,"二次压缩，类型： %d-->%d ,文件号%d,日志文件 '%s' .",mdf.GetInt("compflag",0),compflag,fid,lgfile);

	char srcfn[300];
	strcpy(srcfn,mdf.PtrStr("filename",0));
	int origsize=mdf.GetInt("filesize",0);
	char dstfn[300];
	sprintf(dstfn,"%s.%s",srcfn,deepcmp?"depcp":"dep5");
	tabid=mdf.GetInt("tabid",0);
	//dstf.SetFileHeader(0,srcf.GetNextFileName());
	//mdf.SetMaxRows(10);
	mdf.FetchAll("select filename,idxfname from dp.dp_datafilemap where tabid=%d and fileid=%d and fileflag!=2",
		tabid,fid);
	if(mdf.Wait()<1)
		ThrowWith(" 找不到数据文件记录,dp_datafilemap中的记录已损坏,请检查.\n"
		" 对应的数据文件为:'%s',文件编号: '%d'",srcfn,fid);
	char idxdstfn[300];
	sprintf(idxdstfn,"%s.%s",mdf.PtrStr("idxfname",0),deepcmp?"depcp":"dep5");
	double dstfilelen=0;
	try {
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=1 where tabid=%d and fileid=%d and procstatus=0 and fileflag!=2",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		if(wociGetFetchedRows(st)!=1) {
			lgprintf("处理文件压缩时状态异常,tabid:%d,fid:%d,可能与其它进程冲突！"
				,tabid,fid);
			return 1;
		}
		file_mt idxf;
		lgprintf("数据处理，数据文件:'%s',字节数:%d,索引文件:'%s'.",srcfn,origsize,mdf.PtrStr("idxfname",0));
		idxf.Open(mdf.PtrStr("idxfname",0),0);
		
		dt_file srcf;
		srcf.Open(srcfn,0,fid);
		dt_file dstf;
		dstf.Open(dstfn,1,fid);
		mdf.SetHandle(srcf.CreateMt());
		int lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());
		
		dt_file idxdstf;
		idxdstf.Open(idxdstfn,1,fid);
		mdf.SetHandle(idxf.CreateMt());
		int idxrn=idxf.GetRowNum();
		idxdstf.WriteHeader(mdf,idxf.GetRowNum(),fid,idxf.GetNextFileName());
		if(idxf.ReadBlock(-1,0)<0)
			ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
		AutoMt *pidxmt=(AutoMt *)idxf;
		//lgprintf("从索引文件读入%d条记录.",wociGetMemtableRows(*pidxmt));
		int *pblockstart=pidxmt->PtrInt("blockstart",0);
		int *pblocksize=pidxmt->PtrInt("blocksize",0);
		blockcompress bc(compflag);
		for(i=1;i<threadnum;i++) {
			bc.AddWorker(new blockcompress(compflag));
		}
#define BFNUM 10
		char *srcbf=new char[SRCBUFLEN];//每一次处理的最大数据块（解压缩后）。
		char *dstbf=new char[DSTBUFLEN*BFNUM];//可累积的最多数据(压缩后).
		int dstseplen=DSTBUFLEN;
		bool isfilled[BFNUM];
		int filledlen[BFNUM];
		int filledworkid[BFNUM];
		char *outcache[BFNUM];
		for(i=0;i<BFNUM;i++) {
			isfilled[i]=false;
			filledworkid[i]=0;
			outcache[i]=dstbf+i*DSTBUFLEN;
			filledlen[i]=0;
		}
		int workid=0;
		int nextid=0;
		int oldblockstart=pblockstart[0];
		int lastrow=0;
		int slastrow=0;
		bool iseof=false;
		bool isalldone=false;
		int lastdsp=0;
		mytimer tmr;
		tmr.Start();
		while(!isalldone) {//文件处理完退出
			if(srcf.ReadMt(-1,0,mdf,1,1,srcbf,false,true)<0) {
				iseof=true;
			}
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("用户取消操作!");
					  }
			block_hdr *pbh=(block_hdr *)srcbf;
			int doff=srcf.GetDataOffset(pbh);
			if(pbh->origlen+doff>SRCBUFLEN) 
				ThrowWith("Decompress data exceed buffer length. dec:%d,bufl:%d",
				pbh->origlen+sizeof(block_hdr),SRCBUFLEN);
			bool deliverd=false;
			while(!deliverd) { //任务交付后退出
				worker *pbc=NULL;
				if(!iseof) {
					pbc=bc.GetIdleWorker();
					if(pbc) {
						
						//pbc->Do(workid++,srcbf,pbh->origlen+sizeof(block_hdr),
						//	pbh->origlen/2); //Unlock internal
						pbc->Do(workid++,srcbf,pbh->origlen+doff,doff,
							pbh->origlen/2); //Unlock internal
						deliverd=true;
					}
				}
				pbc=bc.GetDoneWorker();
				while(pbc) {
					char *pout;
					int dstlen=pbc->GetOutput(&pout);//Unlock internal;
					int doneid=pbc->GetWorkID();
					if(dstlen>dstseplen) 
						ThrowWith("要压缩的数据:%d,超过缓存上限:%d.",dstlen,dstseplen);
					//get empty buf:
					for(i=0;i<BFNUM;i++) if(!isfilled[i]) break;
					if(i==BFNUM) ThrowWith("Write cache buffer fulled!.");
					memcpy(outcache[i],pout,dstlen);
					filledworkid[i]=doneid;
					filledlen[i]=dstlen;
					isfilled[i]=true;
					pbc=bc.GetDoneWorker();
					//lgprintf("Fill to cache %d,doneid:%d,len:%d",i,doneid,dstlen);
				}
				bool idleall=bc.isidleall();
				for(i=0;i<BFNUM;i++) {
					if(isfilled[i] && filledworkid[i]==nextid) {
						int idxrn1=wociGetMemtableRows(*pidxmt);
						for(;pblockstart[lastrow]==oldblockstart;) {
							pblockstart[lastrow]=lastoffset;
							pblocksize[lastrow++]=filledlen[i];
							slastrow++;
							if(lastrow==idxrn1) {
								idxdstf.WriteMt(*pidxmt,compflag,0,false);
								pidxmt->Reset();
								lastrow=0;
								if(idxf.ReadBlock(-1,0)>0) {
									//ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
									pidxmt=(AutoMt *)idxf;
									//lgprintf("从索引文件读入%d条记录.",wociGetMemtableRows(*pidxmt));
									pblockstart=pidxmt->PtrInt("blockstart",0);
									pblocksize=pidxmt->PtrInt("blocksize",0);
								}
							}
							else if(lastrow>idxrn1) 
								ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
							
						}
						lastoffset=dstf.WriteBlock(outcache[i],filledlen[i],0,true);
						oldblockstart=pblockstart[lastrow];
						dstfilelen+=filledlen[i];
						filledworkid[i]=0;
						filledlen[i]=0;
						isfilled[i]=false;
						nextid++;
						tmr.Stop();
						double tm1=tmr.GetTime();
						if(nextid-lastdsp>=50) { 
							printf("已处理%d个数据块(%d%%),%.2f(MB/s) %.0f--%.0f.\r",nextid,slastrow*100/idxrn,lastoffset/tm1/1024/1024,tm1,tm1/slastrow*(idxrn-slastrow));
							fflush(stdout);
							lastdsp=nextid;
						}
						i=-1; //Loop from begining.
					}
				}
				if(idleall && iseof) {
					//					if(bc.isidleall()) {
					isalldone=true;
					break;
					//					}
				}
				if(!pbc) 
					mSleep(20);
			}
		}
		if(lastrow!=wociGetMemtableRows(*pidxmt)) 
			ThrowWith("异常错误：并非所有数据都被处理，已处理%d,应处理%d.",lastrow,wociGetMemtableRows(*pidxmt));
		if(wociGetMemtableRows(*pidxmt)>0)
		  idxdstf.WriteMt(*pidxmt,compflag,0,false);
		dstf.Close();
		idxdstf.Close();
		st.Prepare("update dp.dp_datafilemap set procstatus=2 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		delete []srcbf;
		delete []dstbf;
	}
	catch(...) {
		errprintf("数据二次压缩出现异常，文件处理状态恢复...");
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		errprintf("删除数据文件和索引文件");
		unlink(dstfn);
		unlink(idxdstfn);
		throw;
	}
	
	psa->log(tabid,0,122,"二次压缩结束,文件%d，大小%d->%d",fid,origsize,dstfilelen);
	lgprintf("文件转换结束,目标文件:'%s',文件长度(字节):%f.",dstfn,dstfilelen);
	return 1;
}


int DestLoader::ToMySQLBlock(const char *dbn, const char *tabname)
{
	lgprintf("格式转换 '%s.%s' ...",dbn,tabname);
	AutoMt mt(psa->GetDTS(),100);
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
	int rn=mt.Wait();
	if(rn<1) {
		printf("表'%s'不存在!",tabname);
		return 0;
	}
	tabid=mt.GetInt("tabid",0);
	int recordnum=mt.GetInt("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	if(recordnum<1) {
		lgprintf("源表'%s'数据为空.",tabname);
		return 0;
	}
	AutoMt mdt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	wociSetTraceFile("rawBlock格式转换/");
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus =0 and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",tabid);
	rn=mdf.Wait();
	//防止重入，修改数据文件状态。
	int fid=mdf.GetInt("fileid",0);
	char srcfn[300];
	strcpy(srcfn,mdf.PtrStr("filename",0));
	int origsize=mdf.GetInt("filesize",0);
	char dstfn[300];
	sprintf(dstfn,"%s.%s",srcfn,"dep5");
	tabid=mdf.GetInt("tabid",0);
	
	//dstf.SetFileHeader(0,srcf.GetNextFileName());
	//mdf.SetMaxRows(10);
	mdf.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d and fileid=%d and fileflag!=2",
		tabid,fid);
	rn=mdf.Wait();
	char idxdstfn[300];
	sprintf(idxdstfn,"%s.%s",mdf.PtrStr("filename",0),"dep5");
	double dstfilelen=0;
	try {
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=1 where tabid=%d and fileid=%d and procstatus=0 and fileflag!=2",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		if(wociGetFetchedRows(st)!=1) {
			lgprintf("处理文件转换时状态异常,tabid:%d,fid:%d,可能与其它进程冲突！"
				,tabid,fid);
			return 1;
		}
		file_mt idxf;
		lgprintf("数据处理，数据文件:'%s',字节数:%d,索引文件:'%s'.",srcfn,origsize,mdf.PtrStr("filename",0));
		idxf.Open(mdf.PtrStr("filename",0),0);
		if(idxf.ReadBlock(-1,0)<0)
			ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
		
		file_mt srcf;
		srcf.Open(srcfn,0,fid);
		dt_file dstf;
		dstf.Open(dstfn,1,fid);
		mdf.SetHandle(srcf.CreateMt());
		int lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());
		
		AutoMt *pidxmt=(AutoMt *)idxf;
		int idxrn=wociGetMemtableRows(*pidxmt);
		lgprintf("从索引文件读入%d条记录.",idxrn);
		int *pblockstart=pidxmt->PtrInt("blockstart",0);
		int *pblocksize=pidxmt->PtrInt("blocksize",0);
		int lastrow=0;
		int oldblockstart=pblockstart[0];
		int dspct=0;
		while(true) {//文件处理完退出
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("用户取消操作!");
					  }
			int srcmt=srcf.ReadBlock(-1,0,1);
			if(srcmt==0) break;
			int tmpoffset=dstf.WriteMySQLMt(srcmt,COMPRESSLEVEL);
			int storesize=tmpoffset-lastoffset;
			for(;pblockstart[lastrow]==oldblockstart;) {
				pblockstart[lastrow]=lastoffset;
				pblocksize[lastrow++]=storesize;
			}
			if(++dspct>1000) {
				dspct=0;
				printf("\r...%d%% ",lastrow*100/idxrn);
				fflush(stdout);
				//			break;
			}
			lastoffset=tmpoffset;
			oldblockstart=pblockstart[lastrow];
		}
		dt_file idxdstf;
		idxdstf.Open(idxdstfn,1,fid);
		//mdf.SetHandle(idxf.CreateMt());
		idxdstf.WriteHeader(*pidxmt,idxrn,fid,idxf.GetNextFileName());
		dstfilelen=lastoffset;
		idxdstf.WriteMt(*pidxmt,COMPRESSLEVEL,0,false);
		dstf.Close();
		idxdstf.Close();
		st.Prepare("update dp.dp_datafilemap set procstatus=2 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.Execute(1);
		st.Wait();
	}
	catch(...) {
		errprintf("数据转换出现异常，文件处理状态恢复...");
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		errprintf("删除数据文件和索引文件");
		unlink(dstfn);
		unlink(idxdstfn);
		throw;
	}
	
	lgprintf("文件转换结束,目标文件:'%s',文件长度(字节):%f.",dstfn,dstfilelen);
	return 1;
}
int DestLoader::RemoveTable(const char *dbn, const char *tabname,bool prompt)
{
	char sqlbuf[1000];
	char choose[200];
	wociSetEcho(FALSE); 
	lgprintf("remove table '%s.%s ' ...",dbn,tabname);
	AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	AutoStmt st(psa->GetDTS());
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
	int rn=mt.Wait();
	if(rn<1) 
		ThrowWith("表%s.%s在dp_table中找不到!.",dbn,tabname);
	tabid=mt.GetInt("tabid",0);
	double recordnum=mt.GetDouble("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	{
		char fulltbname[300];
		sprintf(fulltbname,"%s.%s",dbn,tabname);

		sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
		lgprintf(sqlbuf);
		if(psa->TouchTable(fulltbname)) psa->DoQuery(sqlbuf);
		{
			lgprintf("删除DP参数文件.");
			char streamPath[300];
			sprintf(streamPath,"%s%s/%s.DTP",psa->GetMySQLPathName(0,"msys"),dbn,tabname);
			unlink(streamPath);
		}
	}
	if(rn<1) {
		lgprintf("表'%s.%s'已删除.",dbn,tabname);
		return 0;
	}
	
	if(prompt) {
		if(recordnum<1)
			sprintf(choose,"DP表'%s.%s'将被删除，数据为空，继续？(Y/N)",dbn,tabname);
		else
			sprintf(choose,"DP表'%s.%s'将被删除，记录数:%.0f？(Y/N)",dbn,tabname,recordnum);
		if(!GetYesNo(choose,false)) {
			lgprintf("取消删除。 ");
			return 0;
		}			
	}
	psa->CloseTable(tabid,NULL,true);
	
	
	//下面的查询不需要加fileflag!=2的限制条件，可以全部删除。
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d ",
		tabid);
	rn=mt.Wait();
	int i=0;
	for(i=0;i<rn;i++) {
		char tmp[300];
		lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("filename",i));
		unlink(mt.PtrStr("filename",i));
		sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
		unlink(tmp);
		lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("idxfname",i));
		unlink(mt.PtrStr("idxfname",i));
		sprintf(tmp,"%s.depcp",mt.PtrStr("idxfname",i));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",mt.PtrStr("idxfname",i));
		unlink(tmp);
	}
	//下面的语句不需要加fileflag!=2的限制条件，可以全部删除。
	st.Prepare(" delete from dp.dp_datafilemap where tabid=%d ",tabid);
	st.Execute(1);
	st.Wait();
	
	st.Prepare(" delete from dp.dp_middledatafile where tabid=%d",tabid);
	st.Execute(1);
	st.Wait();
	
	bool forcedel=false;
	//if(prompt) {
		sprintf(choose,"表'%s.%s'的配置参数也要删除吗?(Y/N)",dbn,tabname);
		forcedel=GetYesNo(choose,false);
	//}
	if(forcedel) {
		mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1",tabid);
		rn=mt.Wait();
		for(i=0;i<rn;i++) {
			char tmp[300];
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tmp,TBNAME_DEST);
			sprintf(sqlbuf,"drop table %s",tmp);
			lgprintf(sqlbuf);
			if(psa->TouchTable(tmp)) psa->DoQuery(sqlbuf);
		}
		st.Prepare(" delete from dp.dp_index where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
		st.Prepare(" delete from dp.dp_datapart where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
		st.Prepare(" delete from dp.dp_table where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	}
	else {
		st.Prepare(" update dp.dp_table set recordnum=0,lstfid=0,cdfileid=0,firstdatafileid=0 where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	}//psa->EmptyIndex(tabid);
	
	lgprintf("表'%s.%s'已删除%s.",dbn,tabname,forcedel?"":",但参数表保留");
	return 1;
}

// 数据文件状态为2：等待删除的数据全部清除，在新数据成功上线后执行。
int SysAdmin::CleanData(bool prompt)
{
	AutoMt mt(dts,100);
	AutoStmt st(dts);
	mt.FetchAll("select tabid,indexgid,sum(recordnum) recordnum from dp.dp_datafilemap where fileflag=2 group by tabid,indexgid");
	int rn=mt.Wait();
	if(rn<1) {
		printf("没有数据需要清空!");
		return 0;
	}
	lgprintf("永久删除数据...");
	//for(int i=0;i<rn;i++) {
	int tabid=mt.GetInt("tabid",0);
	//	int recordnum=mt.GetInt("recordnum",0);
	char tbname[150],idxname[150];
	GetTableName(tabid,-1,tbname,idxname,TBNAME_FORDELETE);
	AutoMt dtmt(dts,MAX_DST_DATAFILENUM);
	dtmt.FetchAll("select filename from dp.dp_datafilemap where tabid=%d  and fileflag=2",
		tabid);
	int frn=dtmt.Wait();
	AutoMt idxmt(dts,200);
	idxmt.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d  and fileflag=2",
		tabid);
	idxmt.Wait();
	//int firstdatafileid=mt.GetInt("firstdatafileid",0);
	//int srctabid=mt.GetInt("srctabid",0);
	if(prompt)
	{
		while(true) {
			printf("\n表 '%s': 将被删除，编号:%d,继续?(Y/N)?",
				tbname,tabid);
			char ans[100];
			fgets(ans,100,stdin);
			if(tolower(ans[0])=='n') {
				lgprintf("取消删除。 ");
				return 0;
			}
			if(tolower(ans[0])=='y') break;
		}
	}
	
	for(int j=0;j<frn;j++) {
		lgprintf("删除'%s'和附加的depcp,dep5文件",dtmt.PtrStr("filename",j));
		unlink(dtmt.PtrStr("filename",j));
		char tmp[300];
		sprintf(tmp,"%s.depcp",dtmt.PtrStr("filename",j));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",dtmt.PtrStr("filename",j));
		unlink(tmp);
		lgprintf("删除'%s'和附加的depcp,dep5文件",idxmt.PtrStr("filename",j));
		unlink(idxmt.PtrStr("filename",j));
		sprintf(tmp,"%s.depcp",idxmt.PtrStr("filename",j));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",idxmt.PtrStr("filename",j));
		unlink(tmp);
	}
	st.Prepare(" delete from dp.dp_datafilemap where tabid=%d  and fileflag=2",tabid);
	st.Execute(1);
	st.Wait();
	//千万不能删!!!!!!!!!!!
	//st.Prepare(" delete from dt_table where tabid=%d",tabid);
	//st.Execute(1);
	//st.Wait();
	DropDTTable(tabid,TBNAME_FORDELETE);
	lgprintf("表'%s'及数据、索引文件已删除.",tbname);
	return 1;
}


void SysAdmin::DropDTTable(int tabid,int nametype) {
	char sqlbf[300];
	AutoMt mt(dts,MAX_DST_INDEX_NUM);
	char tbname[150],idxname[150];
	mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1",tabid);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("找不到%d表的独立索引.",tabid);
	for(int i=0;i<rn;i++) {
		GetTableName(tabid,mt.GetInt("indexgid",i),tbname,idxname,nametype);
		sprintf(sqlbf,"drop table %s",idxname);
		if(conn.TouchTable(idxname))
			DoQuery(sqlbf);
	}
	sprintf(sqlbf,"drop table %s",tbname);
	if(conn.TouchTable(tbname))
		DoQuery(sqlbf);
}

//		
//返回不含公共字段的索引字段数
//destmt:目标表的内存表,含字段格式信息
//indexid:索引编号
int SysAdmin::CreateIndexMT(AutoMt &idxtarget,int destmt,int tabid,int indexid,int *colidx,char *colsname,bool update_idxtb) {
	bool ec=wociIsEcho();
	wociSetEcho(FALSE);
	AutoMt idxsubmt(dts,10);
	idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>=1",tabid,indexid);
	int rn=idxsubmt.Wait();
	if(rn<1) {
		ThrowWith("建立索引结构:在dp.dp_index中无%d独立索引的记录。",indexid);
	}
	wociClear(idxtarget);
	strcpy(colsname,idxsubmt.PtrStr("columnsname",0));
	wociCopyColumnDefine(idxtarget,destmt,colsname);
	//查找该独立索引附带的非独立索引，并以此为依据建立索引表
	idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex=0 order by seqinidxtab",
		tabid,indexid);
	int srn=idxsubmt.Wait();
	for(int j=0;j<srn;j++) {
		//重复的字段自动剔除
		wociCopyColumnDefine(idxtarget,destmt,idxsubmt.PtrStr("columnsname",j));
	}
	//重构索引复用段
	char reusedcols[300];
	int cn1;
	cn1=wociConvertColStrToInt(destmt,colsname,colidx);
	reusedcols[0]=0;
	int tcn=wociGetMtColumnsNum(idxtarget);
	if(tcn>cn1) {
		for(int i=cn1;i<tcn;i++) {
			if(i!=cn1) strcat(reusedcols,",");
			wociGetColumnName(idxtarget,i,reusedcols+strlen(reusedcols));
		}
		strcat(colsname,",");
		strcat(colsname,reusedcols);
	}
	if(update_idxtb) {
		lgprintf("修改索引%d的复用字段为'%s'.",indexid,reusedcols);
		AutoStmt st(dts);
		if(strlen(reusedcols)>0)
			st.Prepare("update dp.dp_index set reusecols='%s' where tabid=%d and indexgid=%d and issoledindex>=1",
			reusedcols,tabid,indexid);
		else
			st.Prepare("update dp.dp_index set reusecols=null where tabid=%d and indexgid=%d and issoledindex>=1",
			 tabid,indexid);
		st.Execute(1);
		st.Wait();
	}
	
	//索引表公共字段
	wociAddColumn(idxtarget,"dtfid",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"blockstart",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"blocksize",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"blockrownum",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"startrow",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"idx_rownum",NULL,COLUMN_TYPE_INT,10,0);
	idxtarget.Build();
	//取独立索引和复用索引在blockmt(目标数据块内存表)结构中的位置，
	// 检查结构描述文件建立的索引是否和系统参数表中指定的字段数相同。
	int bcn=cn1;//wociConvertColStrToInt(destmt,colsname,colidx);(colsname与reusedcols的内容相同）
	bcn+=wociConvertColStrToInt(destmt,reusedcols,colidx+bcn);
	if(wociGetColumnNumber(idxtarget)!=bcn+6) {
		ThrowWith("Column number error,colnum:%d,deserved:%d",
			wociGetColumnNumber(idxtarget),bcn+6);
	}
	//设置dt_index中的idxfieldnum
	if(update_idxtb) {
		lgprintf("修改%d索引及复用索引的字段总数为%d.",indexid,bcn);
		AutoStmt st(dts);
		st.Prepare("update dp.dp_index set idxfieldnum=%d where tabid=%d and indexgid=%d ",
			bcn,tabid,indexid);
		st.Execute(1);
		st.Wait();
	}
	wociSetEcho(ec);
	return bcn;
}

void SysAdmin::CreateAllIndexTable(int tabid,int destmt,int nametype,bool createidx,int ci_type) {
	AutoMt mt(dts,100);
	mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1 order by seqindattab",tabid);
	int rn=mt.Wait();
	for(int i=0;i<rn;i++) 
		CreateIndexTable(tabid,mt.GetInt("indexgid",i),-1,destmt,nametype,createidx);
}

//如果indexmt为-1，则destmt必须有效。
void SysAdmin::CreateIndexTable(int tabid,int indexid,int indexmt,int destmt,int nametype,bool createidx,int ci_type) {
	AutoMt targetidxmt(dts,10);
	if(indexmt==-1) {
		int colidx[50];
		char colsname[300];
		int cn=CreateIndexMT(targetidxmt,destmt,tabid,indexid,colidx,colsname,false);
		indexmt=targetidxmt;
	}
	char tbname[100],idxname[100];
	GetTableName(tabid,indexid,tbname,idxname,nametype);
	CreateTableOnMysql(indexmt,idxname,true);
	if(createidx) {
		//2005/12/01修改,增加创建索引表/目标表控制
		CreateIndex(tabid,indexid,nametype,true,ci_type);//tabname,idxtabname,0,conn,tabid,
		//indexid,true);
	}
}


int SysAdmin::CreateDataMtFromFile(AutoMt &destmt,int rownum,int tabid,int fileflag) {
	AutoMt mt(dts,10);
	mt.FetchAll("select filename from dp.dp_datafilemap where tabid=%d and fileflag=%d limit 2",tabid,fileflag);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("创建目标表结构时找不到数据文件。");
	dt_file idf;
	idf.Open(mt.PtrStr("filename",0),0);
	destmt.SetHandle(idf.CreateMt(rownum));
	return wociGetColumnNumber(destmt);
}

int SysAdmin::CreateIndexMtFromFile(AutoMt &indexmt,int rownum,int tabid,int indexid) {
	AutoMt mt(dts,10);
	mt.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d and indexgid=%d  and (fileflag=0 or fileflag is null) limit 2",tabid,indexid);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("创建索引表结构时找不到数据文件。");
	dt_file idf;
	idf.Open(mt.PtrStr("filename",0),0);
	indexmt.SetHandle(idf.CreateMt(rownum));
	return wociGetColumnNumber(indexmt);
}

void SysAdmin::OutTaskDesc(const char *prompt,int tabid,int datapartid,int indexid)
{
	AutoMt mt(dts,100);
	char tinfo[300];
	tinfo[0]=0;
	lgprintf(prompt);
	if(datapartid>0 && tabid>0) {
		mt.FetchAll("select * from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
		if(mt.Wait()<1) ThrowWith("错误的数据分组:%d->%d.",tabid,datapartid);
		lgprintf("任务描述 : %s.",mt.PtrStr("partdesc",0));
		lgprintf("数据分组 : %d.",datapartid);
		lgprintf("源系统 : %d.",mt.GetInt("srcsysid",0));
		lgprintf("数据抽取 : \n%s.",mt.PtrStr("extsql",0));
	}
	if(indexid>0 && tabid>0) {
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
	  if(mt.Wait()<1) ThrowWith("错误的索引组号:%d->%d.",tabid,indexid);
	  lgprintf("索引组号%d,索引字段: %s.",indexid,mt.PtrStr("columnsname",0));
	}		
	if(tabid>0) {
		mt.FetchAll("select concat(databasename,'.',tabname) as dstdesc,srcowner,srctabname from dp.dp_table where tabid=%d",tabid);
		if(mt.Wait()>0) 
			
			lgprintf("目标表 :%s,源表:%s.%s.",mt.PtrStr("dstdesc",0),mt.PtrStr("srcowner",0),mt.PtrStr("srctabname",0));
	}
}
