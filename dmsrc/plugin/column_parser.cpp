#include "column_parser.h"

#ifndef DEFINE_DOWNLOAD_EXTERN_NAME_LIST
#define DEFINE_DOWNLOAD_EXTERN_NAME_LIST
// ������չ���ƶ���
const char *DOWNLOAD_EXTERN_NAME_LIST[]={     
	".exe", // windows down load 
	".apk", // application/vnd.android 
	".ipa", // application/vnd.iphone
	".mp3", // audio/x-mpeg 
	".mp4", // video/mp4 
	".mmf", // audio/mmf 
	".mfm", // audio/mfm 
	".pmd", // audio/pmd 
	".adp", // audio/adp 
	".amr", // audio/amr 
	".3gp", // video/3gpp 
	".cab", // application/vnd.smartpohone 
	".gif", // image/gif  
	".pxl", // application/vnd.iphone 
	".ded", // application/vnd.iphone 
	".app", // application/vnd.iphone 
	".hme", // application/vnd.smartphone.thm 
	".jad", // text/vnd.sun.j2me.app-descriptor 
	".jar", // application/java-archive 
	".jpg", // image/jpeg 
	".mid", // audio/midi 
	".ogg", // application/ogg 
	".pdb", // application/ebook 
	".rm", // video/rm 
	".rng", // application/vnd.nokia.ringing-tone 
	".sdt", // application/vnd.sie.thm 
	".sis", // application/vnd.symbian.install 
	".thm", // application/vnd.eri.thm 
	".tsk", // application/vnd.ppc.thm 
	".umd", // application/umd 
	".utz", // application/vnd.uiq.thm 
	".wav", // audio/x-wav 
	".wbmp", // image/vnd.wap.wbmp 
	".wml", // text/vnd.wap.wml 
	".wmlc", // application/vnd.wap.wmlc 
	".wmls", // text/vnd.wap.wmlscript 
	".wmlsc", // application/vnd.wap.wmlscriptc 
	".wmv", // video/x-ms-wmv 
	".wsc", // application/vnd.wap/wmlscriptc 
	//TODO add ..
	"\0" // the last end
};

#define DOWNLOAD_SEPARATOR  '/' // ���طָ���

#endif

#ifndef min
 #define min(a,b) ((a)>(b)?(b):(a))
#endif
#ifndef max
 #define max(a,b) ((a)>(b)?(a):(b))
#endif

Column_parser::Column_parser(){
    init_adms_and_key_info();
    st_iconv_utf8.is_open = false;
    st_iconv_gbk.is_open = false;
}

Column_parser::~Column_parser(){
    if(st_iconv_gbk.is_open){
        iconv_close(st_iconv_gbk.cd);
        st_iconv_gbk.is_open = false;
    }
    if(st_iconv_utf8.is_open){
        iconv_close(st_iconv_utf8.cd);
        st_iconv_utf8.is_open = false;
    }
}

// pc xp/win7 �����(ie,chrome),�ؼ��ַ������ֵĶ���ԭ����,�������ĵ�λunicode����,��Ҫת����gbk�������utf8����
// unicode �� utf8��ת����:  http://www.falconhan.com/tools/utf8-conv.aspx
// unicode �� gbk��ת����:  http://www.falconhan.com/tools/gb2312-conv.aspx
enum SEARCH_ENGINES_TYPE{
    INVALID_KEYWORD=-1, // ��Ч�ؼ��� 
    www_bai_com=0,      // �ٶ�       ����[www.baidu.com]       ������Χ[wd=,&]            �ؼ���[unicode����--->UTF8����]
    www_google_cn,      // google     ����[www.google.com]      ������Χ[q=,&]             �ؼ���[unicode����--->UTF8����]
    www_google_cn_hk,   // google.hk  ����[www.google.com.hk]   ������Χ[q=,&]             �ؼ���[unicode����--->UTF8����] 
    www_so_com,         // 360        ����[www.so.com]          ������Χ[q=,&]             �ؼ���[unicode����--->UTF8����]
    www_sogou_com,      // �Ѻ�       ����[www.sogou.com]       ������Χ[query=,&]         �ؼ���[unicode����--->UTF8����]  
    cn_bing_com,        // ΢������   ����[cn.bing.com]         ������Χ[search?q=,&]      �ؼ���[unicode����--->UTF8����] 
    s_taobao_com,       // �Ա�����   ����[s.taobao.com]        ������Χ[search?q=,&]      �ؼ���[unicode����-->GBK/GB2312����]
    www_youdao_com,     // ��������   ����[www.youdao.com]      ������Χ[search?q=,&]      �ؼ���[unicode����--->UTF8����] 
    music_baidu_com,    // �ٶ�����   ����[music.baidu.com]     ������Χ[search?key=,]     �ؼ���[unicode����--->UTF8����
};

void Column_parser::init_adms_and_key_info(){
	for(int i=0;i<MAX_SEARCH_ENGINES;i++){
		adms_search_engines_info[i].lSearchId = i;

        switch(i){
            case www_bai_com:{
                // �ٶ���ҳ�ǶԵ�
                // �ؼ��ֱ�������: unicode����--->UTF8����
                // �ο�����   http://www.falconhan.com/tools/utf8-conv.aspx
    			adms_search_engines_info[i].cEncodingType = SET_UTF8;// ����Ӧ����UTF8
    			adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"wd=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
    			strcpy(adms_search_engines_info[i].cHostName,"www.baidu.com");
    			adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "oe=UTF-8");
    		}
            break;
            
    		case music_baidu_com:{
                // �ؼ��ֱ�������: unicode����--->UTF8����
                // �ο�����   http://www.falconhan.com/tools/utf8-conv.aspx
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 3;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"search?key=");					       
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"");
        		strcpy(adms_search_engines_info[i].cHostName,"music.baidu.com");
                adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
    		}
            break;

            case s_taobao_com:{
                // �ؼ�������Ϊ: unicode����-->GBK/GB2312
                // �ο�����   http://www.falconhan.com/tools/gb2312-conv.aspx
    			adms_search_engines_info[i].cEncodingType = SET_GBK;
                adms_search_engines_info[i].cKeyType = 2;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
    			strcpy(adms_search_engines_info[i].cHostName,"s.taobao.com");
                adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
    		}
            break;
            
            case www_so_com:{
                // �ؼ��ֱ�������: unicode����--->UTF8����
                // �ο�����   http://www.falconhan.com/tools/utf8-conv.aspx
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
                strcpy(adms_search_engines_info[i].cHostName,"www.so.com");
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "complete");
    		}
            break;
            
            case www_sogou_com:{
                // �ؼ��ֱ�������: unicode����--->UTF8����
                // �ο�����   http://www.falconhan.com/tools/utf8-conv.aspx
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"query=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
                strcpy(adms_search_engines_info[i].cHostName,"www.sogou.com");
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "complete");
    		}
            break;
            
    		case www_google_cn:{
                // goole
                // �ؼ��ֱ�������: unicode����--->UTF8����
                // �ο�����   http://www.falconhan.com/tools/utf8-conv.aspx
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
                strcpy(adms_search_engines_info[i].cHostName,"www.google.cn");
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "complete");
    		}
            break;
            
    		case www_google_cn_hk:{
                // �ؼ��ֱ�������: unicode����--->UTF8����
                // �ο�����   http://www.falconhan.com/tools/utf8-conv.aspx
                // ��ҳ/���� �����ǶԵ�          
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
                strcpy(adms_search_engines_info[i].cHostName,"www.google.com.hk");
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "complete");
    		}
            break;
            
            case cn_bing_com:{
                // �ؼ�������Ϊ: unicode����-->utf8
                // �ο�����   http://www.falconhan.com/tools/utf8-conv.aspx
    			adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 2;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"search?q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
    			strcpy(adms_search_engines_info[i].cHostName,"cn.bing.com");
                adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
    		}
            break;

            case www_youdao_com:{
                // �ؼ�������Ϊ: unicode����-->utf8
                // �ο�����   http://www.falconhan.com/tools/utf8-conv.aspx
    			adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 2;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"search?q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
    			strcpy(adms_search_engines_info[i].cHostName,"www.youdao.com");
                adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
            }
            break;

            default:
                adms_search_engines_info[i].lSearchId = INVALID_KEYWORD;
                return ;
        } // end switch
        
	} // end for

    download_extern_name_list.clear();
}

int Column_parser::is_text_utf8(const char* in_stream,const int stream_len){ 
    // ����ʵ�֣��ο�:http://blog.csdn.net/shanshu12/article/details/6716118
   short nBytes=0;              // UFT8 ����1-6���ֽڱ���,ASCII��һ���ֽ�  
   
   bool bAllAscii=true;         // ���ȫ������ASCII, ˵������UTF-8  
   
   for(int i=0;i<stream_len;i++){  
       if( ((*in_stream)&0x80) != 0 ){ // �ж��Ƿ�ASCII����,�������,˵���п�����UTF-8,ASCII��7λ����,����һ���ֽڴ�,���λ���Ϊ0,o0xxxxxxx  
           bAllAscii= false;  
       }
       
       if(nBytes==0){ //�������ASCII��,Ӧ���Ƕ��ֽڷ�,�����ֽ���  
           if( (*in_stream) >= 0x80 ){  
               if( (*in_stream) >= 0xFC && (*in_stream) <= 0xFD ){  
                   nBytes=6;  
               }else if( (*in_stream) >= 0xF8 ){  
                   nBytes=5;  
               }else if( (*in_stream) >= 0xF0 ){ 
                   nBytes=4;  
               }else if( (*in_stream) >= 0xE0 ){  
                   nBytes=3;  
               }else if( (*in_stream) >= 0xC0 ){
                   nBytes=2;  
               }else{  
                   return 0;  
               }  
               nBytes--;  
           }  
       }else{ //���ֽڷ��ķ����ֽ�,ӦΪ 10xxxxxx  
           if( ( (*in_stream)&0xC0 ) != 0x80 ){  
               return 0;  
           }  
           nBytes--;  
       }  

       in_stream++;
   }  
   
   if( nBytes > 0 ){ //Υ������  
       return 0;  
   }  
   if( bAllAscii ){ //���ȫ������ASCII, ˵������UTF-8  
       return 0;  
   }  
   
   return 1;  
}

// get keyword(just get keyword without decoding from unicode) from http
// success: return the adms_search_engines_info.lsearchid
// error: return -1;
// get process:
// 1. get hostname pos
// 2. get keyword start pos
// 3. make sure hostname pos < start pos
// 4. get keyword end pos
// 5. make sure start pos < end pos
// 6. get the keyword <----- http[startpos,endpos]
int Column_parser::_get_keyword_from_http(const stru_adms_search_engines_info* st,const std::string& http,std::string& keyword){
    std::string _cHostName = "";
    int _cHostName_pos = -1;

    std::string _cMatchStrStart = "";
    int _cMatchStrStart_pos = -1;
    
    std::string _cMatchStrEnd = "";
    int _cMatchStrEnd_pos = -1;

    int _lSearchId = -1;
    for(int i=0;i<MAX_SEARCH_ENGINES;i++){
        if(st[i].lSearchId == INVALID_KEYWORD){
            keyword="";
            break;
        }

        _cHostName = (std::string)st[i].cHostName;
        _cMatchStrStart = (std::string)st[i].cMatchStrStart;
        _cMatchStrEnd = (std::string)st[i].cMatchStrEnd;

        // 1. get hostname pos
        _cHostName_pos = http.find(_cHostName);
        if(std::string::npos == _cHostName_pos){ // do not find the hostname
            continue;
        }

        // 2. get keyword start pos
        // 3. make sure hostname pos < start pos
        _cMatchStrStart_pos = http.find(_cMatchStrStart,_cHostName_pos+1);
        if(std::string::npos == _cMatchStrStart_pos || _cHostName_pos >= _cMatchStrStart_pos ){ // do not find the MatchStrStart
            continue;
        }

        // 4. get keyword end pos
        // 5. make sure start pos < end pos
        if(music_baidu_com == st[i].lSearchId){ // skip the MatchStrEnd

            // 6. get the keyword <----- http[startpos,endpos]
            keyword = http.substr((_cMatchStrStart_pos+_cMatchStrStart.size()),(http.size()-_cMatchStrStart.size()-_cMatchStrStart_pos));     
            _lSearchId = st[i].lSearchId;
            
            break;
        }else{//music_baidu_com != st[i].lSearchId
            _cMatchStrEnd_pos = http.find(_cMatchStrEnd,_cMatchStrStart_pos+1);

            if(std::string::npos == _cMatchStrEnd_pos || _cMatchStrStart_pos >= _cMatchStrEnd_pos){
                continue;
            }

            // 6. get the keyword <----- http[startpos,endpos]
            keyword = http.substr((_cMatchStrStart_pos+_cMatchStrStart.size()),(_cMatchStrEnd_pos-_cMatchStrStart_pos-_cMatchStrStart.size()));      
            _lSearchId = st[i].lSearchId;

            break;
        }              
    }

    return _lSearchId;
}

int Column_parser::code_convert(char *from_charset,char *to_charset,const char *inbuf, size_t inlen,char *outbuf, size_t& outlen){
     char **pin = (char**)&inbuf;
     char **pout = &outbuf;

#if 0 
     // ����ÿ�ζ��������´�
     if(strcmp(from_charset,"UTF-8") == 0){ // utf8 ---> gbk
        if(!st_iconv_utf8.is_open){
            st_iconv_utf8.cd = iconv_open(to_charset,from_charset);
            if(st_iconv_utf8.cd == 0) return -1;
            st_iconv_utf8.is_open = true;
        }
        if (iconv(st_iconv_utf8.cd, pin,(size_t*)&inlen,pout, (size_t*)&outlen)==-1){
            return -1;      
        }         
     }else if(strcmp(from_charset,"GBK") == 0){ // gbk ---> gbk
        if(!st_iconv_gbk.is_open){
            st_iconv_gbk.cd = iconv_open(to_charset,from_charset);
            if(st_iconv_gbk.cd == 0) return -1;
            st_iconv_gbk.is_open = true;
        }
        if (iconv(st_iconv_gbk.cd, pin,(size_t*)&inlen,pout, (size_t*)&outlen)==-1){
            return -1;      
        }    
     }else{
        assert(0);
     }
#else     
     // �����������ȷ�ģ�ֻ�ǿ��ǵ�Ч�ʽ������δ���
     iconv_t cd; 
	 size_t origin_len=outlen;
     cd = iconv_open(to_charset,from_charset);
     if (cd==0) return -1;
     memset(outbuf,0,outlen);
     if (iconv(cd, pin,(size_t*)&inlen,pout, (size_t*)&outlen)==-1) return -1;
     iconv_close(cd);
     outlen=origin_len-outlen;
#endif   

     return 0;

}

// �������� unicode ---> utf8 ---> gbk
// �Ա����� unicode ---> gbk
int Column_parser::change_buff_info(char* oribuf)	
{
	int re1,re2,i,inlen=0;

    char inbuf[1024];
	char *in  = inbuf;

    size_t orilen = strlen(oribuf);
	
	for(i=0 ; i < orilen ; )
	{
		if( oribuf[i] == '%')
		{
			if(oribuf[i+1] >= 'A' && oribuf[i+1] <= 'F'){
				re1 = oribuf[i+1] - 'A' + 10;
			}else if(oribuf[i+1] >= 'a' && oribuf[i+1] <= 'f'){
				re1 = oribuf[i+1] - 'a' + 10;
			}else if(oribuf[i+1]>='0' && oribuf[i+1]<='9'){ 
				re1 = oribuf[i+1] - '0';
			}
            
            if(oribuf[i+2]>='A' && oribuf[i+2]<='F'){ 
				re2 = oribuf[i+2] - 'A' + 10;
            }else if(oribuf[i+2]>='a' && oribuf[i+2]<='f'){ 
				re2 = oribuf[i+2] - 'a' + 10;
			}else if(oribuf[i+2]>='0' && oribuf[i+2]<='9'){ 
				re2 = oribuf[i+2] - '0';
			}

			inbuf[inlen++] = re1 * 16 + re2;
			i += 3;
		}else{ //if(oribuf[i] >= 0 && oribuf[i] <= 127)
			inbuf[inlen++] = oribuf[i];
			i++;
		}
	}

	inbuf[inlen] = '\0';
    inlen ++;
	memcpy(oribuf,inbuf,inlen);
	
	return inlen;
}

// get keyword from http
int   Column_parser::get_keyword_from_http(const char* phttp,std::string& keyword){
    std::string _http(phttp);
    std::string _unicode_keyword = "";
    char out_buf[2048];
    char in_buf[2048];
    size_t out_len = 2048;
    memset(out_buf,0,2048);
    
    int valid_len = 0;

    int search_id = this->_get_keyword_from_http(this->adms_search_engines_info,_http,_unicode_keyword);
    if(search_id == INVALID_KEYWORD){
        // get invalid keyword
        keyword = "";
        return keyword.size();
    }else{
        // get valid keyword

        // get cEncodingType type
        int _cEncodingType = this->adms_search_engines_info[search_id].cEncodingType;
		
		strcpy(in_buf,_unicode_keyword.c_str());
        size_t in_len = change_buff_info(in_buf);
        
        // gbk �����ֱ�ӷ��ؾͿ�����
        if(_cEncodingType == (int)SET_GBK){
        	keyword = in_buf;
        	return keyword.size();
        }
        
        valid_len = min(in_len,strlen(in_buf));
        in_len = valid_len;
             
        // ���������utf8����
       	if(is_text_utf8(in_buf,in_len)){
			_cEncodingType=SET_UTF8;
		}else{
			_cEncodingType=SET_GBK;
		}
       	
       	int ret = 0;	
        if(_cEncodingType == (int)SET_UTF8){ // unicode to UTF-8
            ret = code_convert((char*)"UTF-8",(char*)"GBK",in_buf,in_len,out_buf,out_len);
        }else if(_cEncodingType == (int)SET_GBK){ // unicode to gbk
            ret = code_convert((char*)"GBK",(char*)"GBK",in_buf,in_len,out_buf,out_len);
        }else{
            ThrowWith("Get the error encoding type .");
        }
        
        if(-1 == ret){
            lgprintf("code_convert error, return -1.");            
            keyword="";
        }else{
            // �ú�������bug,out_len Ϊout_bufʣ��Ĵ�С
            /*            
            size_t iconv (iconv_t cd,
                     const char* * inbuf, size_t * inbytesleft,
                     char* * outbuf, size_t * outbytesleft);
            */
            int valid_len = min(strlen(out_buf),out_len);
            out_buf[valid_len] = 0;
            keyword = out_buf;
        }        
    }
    
    return keyword.size();
}

// p_download_extern_name : *.apk;*.exe
#ifndef SEPERATE_FLAG
#define SEPERATE_FLAG   ';'
#endif
void  Column_parser::set_download_extern_name(const char* p_download_extern_name){
    if(!download_extern_name_list.empty()){
        return;
    }
    char _tmp_lst[300];
    strcpy(_tmp_lst,p_download_extern_name);
    const char* p = _tmp_lst;
    char *q = _tmp_lst + (strlen(_tmp_lst)-1);

    while((*p) == SEPERATE_FLAG){p++;};
    while((*q) == SEPERATE_FLAG){*q = 0; q--;};

    char _path[300];
    int _path_len = 0;
    while(*p){
        if(*p == SEPERATE_FLAG){ 
            strncpy(_path,(p-_path_len),_path_len);
            _path[_path_len] = 0;
            std::string _tmp_item(_path+1);// skip *
            download_extern_name_list.push_back(_tmp_item);
            _path_len = 0;
        }else{
            _path_len++;            
        }    
        p++;
    }
    // ���һ����չ��
    {
        strncpy(_path,(p-_path_len),_path_len);
        _path[_path_len] = 0;    
        std::string _tmp_item(_path+1); //skip *
        download_extern_name_list.push_back(_tmp_item);
    }      

}

int   Column_parser::get_download_from_http(const char* phttp,std::string& download){
    for(int i=0;i<download_extern_name_list.size();i++){
        const char* pext_name = download_extern_name_list[i].c_str();
        int ext_len = strlen(pext_name);
        if(ext_len == 0){
            download = "";
            break;
        }else{
            int http_len = strlen(phttp);
            if(http_len<=ext_len){
                continue;
            }

            // find the download
            if(strcmp(phttp+(http_len - ext_len),pext_name) == 0){
                const char* q = phttp+(http_len-1);
                int _tmp_len = http_len;

                // ����:"/"����"%2F","#"����"%23","&"����"%26","="����"%3D","?"����"%3F"
                // %252F Ҳ�Ǵ��� /
                int sperator_len = 0;
                while( (_tmp_len-->1) && (*q)){
                    if(*q == DOWNLOAD_SEPARATOR){
                        sperator_len = 1;
                        break;
                    }else if((http_len-_tmp_len > 3) && strncasecmp(q,"%2F",3)==0){
                        sperator_len = 3;
                        break;
                    }else if((http_len-_tmp_len > 5) && strncasecmp(q,"%252F",5)==0){
                        sperator_len = 5;
                        break;
                    }else if((http_len-_tmp_len > 4) && strncasecmp(q,"%02F",4)==0){
                        sperator_len = 4;
                        break;
                    }

                    q--;
                }

                if(_tmp_len>1){ // find download 
                    download = q + sperator_len;
                    break;
                }                
            }
        }
    }
   
    return download.size();
}
