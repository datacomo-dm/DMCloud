/**************************************************************************************************
  Copyright (C), 2010-2020, DATACOMO COMMUNICATIONGS INC(http://www.datacomo.com/)
  File name   : IOPENSSLUTILITY.H      
  Author      : Liujs(liu_shun_shun@126.com)     
  Version     : V1.00        
  DateTime    : 2012/9/6 10:48
  Description : OpensslUtility ���⹤��,֧��windows/linux����Ҫʵ�ֹ�������
				DES ���ܽ���
				RSA ���ܽ���
				BASE64 ת��
				IToolUtility ���ü򵥺���
**************************************************************************************************/
#ifndef OPENSSL_UTILITY_DEF_H
#define	OPENSSL_UTILITY_DEF_H

#include <cstdio>
#include <iostream>
#include <cassert>						// assert
#include <cstring>
#include <cstdlib>
#include <cstdarg>						// va_start,va_end,va_list
#include "openssl/rsa.h"				// rsa
#include "openssl/x509.h"				// d2i,i2d,bn
#include "openssl/des.h"				// des
#include "openssl/md5.h"				// md5
#include "openssl/evp.h"                // base64

using	namespace	std;

namespace	IOpensslUtility
{
//#pragma pack(4)							// 4�ֽڶ��� 
#define			ERROR			-1			// ʧ��
#define			OK				0			// �ɹ�

#define		ASSERT(exp)		assert((exp))		// assert
#define		CON_EXP(exp)	#exp
#define		FREE_MEM(mem) do {if(NULL != (mem)){free((mem));(mem) = NULL;}} while (0); // �Ƿ�malloc���ڴ�
typedef		char					CHAR;
typedef		unsigned	char		UCHAR;
typedef		int						INT;
typedef		unsigned	int			UINT;
typedef		short					SSHORT;
typedef		unsigned	short		USHORT;
typedef		long					LONG;
typedef		unsigned	long		ULONG;	
typedef		unsigned	short		WORD;
typedef		unsigned	int			DWORD;
#ifdef _WIN32
#define		VOID		void
#else
typedef		void		VOID;			// activex conflict
#endif
#define		CONST		const

// RSA ���ܽ���,����λ��
CONST	USHORT		conCryptoBit_1024		=	1024;
CONST	USHORT		conCryptoBit_512		=	512;
CONST	USHORT		conCryptoBit_256		=	256;
CONST	USHORT		conCryptoBit_128		=	128;
CONST	USHORT		conCryptoBit_64			=	64;


CONST	USHORT		conMd5SumLen            =   16;     // MD5У���ַ�
CONST	USHORT      conDesKeysFileLen       =   8*1024;	// �洢1024��
enum EnumKeySourceType    // ��Կ���ͣ���Ӧdp.dp_keysource��keytype
{
	KeySourceType_Des_Random_key = 1,                   // Des�����Կ�ļ�����
};

CONST	USHORT		conKeyPosLen			=	2;		// ��Կ���ݳ���

#define	DATA_LEN_4_BYTES								// ���ݳ��ȶ��壬Ŀǰ��֧��4�ֽڵ����ݣ�ȥ���ú�󣬳���Ϊ2
#ifdef DATA_LEN_4_BYTES
CONST	USHORT		conDataLenLen			=	4;		
typedef UINT   DATA_LEN_LEN;							// 4�ֽ�
#else
typedef USHORT DATA_LEN_LEN;							// 2�ֽ� 0xFFFF,���64KB
CONST	USHORT		conDataLenLen			=	2;		
#endif 


const	INT		conEcbKeyLen = 8;			// ecbģʽkey���� 8 
const	INT		conEcb3KeyLen = 16;			// ecb3ģʽkey���� 16
const	INT		conEcbEncryDataLen = 8;		// ecbģʽ�£�ÿ��ֻ�ܼ���8�ֽ�

// ����������DES,RSA��״̬�Ƿ��
typedef	enum	CryptoStatus
{
	CRYPTO_UNREADY = 0,				// ��ʼ״̬��0
	CRYPTO_READY = 7,			    // Ready״̬�����Խ��м���,DES/RSA���ã�����/����111--->7
	CRYPTO_RSA_ENCRYPT = 3,			// RSA�ã�ֻ�ܼ���011--->3
	CRYPTO_RSA_DECRYPT = 5,			// RSA�ã�ֻ�ܽ���101--->5
}CryptoStatus;


	/**************************************************************************************************
	  Object      : IToolUtility ���ߺ���
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 10:50
	  Description : ��Ҫ�ṩһЩ���õĲ��Ժ���
					LogMsg�ú��� ��Ҫע��
					1���������(����̨����,win/linux)����Ҫ����д����Ϸ���˵������������
					2�����ͻ��˵��ã�����Activex����־�����������Ҫ����MessageBox�Ǿ���Ҫ�ض���
	**************************************************************************************************/
	namespace	IToolUtility
	{
		#if 0
		// ������Ϣ�������Ļ���ڲ��Ѿ������˾�����ļ�����������
		VOID	LogMsg(CONST CHAR*	pszFormat,...); 
		#else
		// ������ͳһ�ĺ�������,ע�⣺activex��ʹ�õ�Ӧ����MessageBox���������̨�������ʹ��printf���
		#define		LogMsg	printf
		#endif
		// �� �������Ӵ��ļ���ָ��λ�ö�ȡ

		// ���ļ��ж�ȡָ���ֽڵ�����,�����ƶ�ȡ�ļ�,���ض�ȡ�ɹ����ֽ���
		LONG	LoadDataFromFile(CONST CHAR* pfilename,UCHAR *pbuff,CONST LONG len,LONG *retLen);		
		// ������д���ļ�,����д���ֽ�����������д���ļ�������д��ɹ����ֽ���
		LONG	SaveDataToFile(CONST CHAR* pfilename,CONST UCHAR *pdata,CONST LONG saveLen);

		// ����ַ���
		VOID	PrintData(CONST UCHAR *pdata,CONST UINT len,CONST CHAR *pcaption);
		// ��byteת����16�����ַ���
		INT		ChangeBytesToHexstr(CONST UCHAR *pSource,CONST INT sourceLen,CHAR *pDest,CONST INT destLen,INT *retDestLen);

		/************************************************************************/
		/* ����ĺ����ú����                                                                     */
		/************************************************************************/
		// 16�ֽڵ�MD5У��ֵ
		UCHAR*	MD5Sum(CONST UCHAR *pSource,CONST UINT sourceLen,UCHAR *md);
		// ����keyIndex
		USHORT	GetKeyIndexFromPlainText(CONST UCHAR *pSource,CONST UINT sourceLen);
		// ��[KeyIndex+DataLen+Cipher]�л�ȡKeyIndex ,DataLen
		void	GetKeyIndexAndDataLenFromCombination(CONST UCHAR *pCombinationBuff,USHORT *KeyIndex,DATA_LEN_LEN *DataLen);
	}

	/**************************************************************************************************
	  Object      : RsaCrypto
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 11:15
	  Description : RSA���ܽ���(��Կ���ܣ�˽Կ����)����Կ�ĵ��뵼����˽Կ�ĵ��뵼���ȹ���
	**************************************************************************************************/
	class RsaCrypto
	{
	public:
		RsaCrypto();
		virtual ~RsaCrypto();
	public:
		/************************************************************************/
		/*  ע�⣺openssl �㷨�Խӵ���Կ�ĵ��룬
		/*  ������Կ˽Կ����������ܱ�����openssl��
		/************************************************************************/
		INT			LoadRsaPubKeyFromFile(CONST	CHAR	*pKeyFile);
		INT			SaveRsaPubkeyToFile(CONST	CHAR	*pKeyFile);
		INT			LoadRsaPriKeyFromFile(CONST	CHAR	*pKeyFile);
		INT			SaveRsaPrikeyToFile(CONST	CHAR	*pKeyFile);

		/************************************************************************/
		/*  ����ras��Կ����Կ˽Կ��                                                                    
		/************************************************************************/
		INT			RenewGenerateKey(CONST INT	bits = conCryptoBit_1024,CONST ULONG pubExponnent = RSA_3);

	public:
		/************************************************************************/
		/* ע�⣺��openssl�㷨�⵼����Կģ����˽Կģ��������Կָ����˽Կָ��
		/* �������Խӣ����ܣ����ܣ����㷨ʹ��
		/* ��Կģ�� + ��Կָ�� ----> ��Կ
		/* ��Կģ�� + ��Կָ�� + ˽Կָ��----> ˽Կ
		/************************************************************************/
		// ����ģ��(��Կ˽Կһ����)��pHexModule ���ⲿ��OPENSSL_free�ͷ�
		INT			ExportModule(CHAR **pHexModule,INT *moduleLen);
		// ������Կָ��, ���ⲿ��OPENSSL_free�ͷ�
		INT			ExportPubExponent(CHAR **pHexPubExponent,INT *pubExponentLen);
		// ����˽Կָ��, ���ⲿ��OPENSSL_free�ͷ�
		INT			ExportPriExponnent(CHAR **pHexPriExponent,INT *priExponentLen);

	public:
		/************************************************************************/
		/* �ӱ�׼���㷨������Կ                                                                      
		/* ��Կģ�� + ��Կָ�� ----> ��Կ
		/* ��Կģ�� + ��Կָ�� + ˽Կָ��----> ˽Կ
		/************************************************************************/
		// ����˽Կ
		INT			ImportPrivateKey(CONST CHAR* pubHexExponent,CONST CHAR *hexModule,CONST CHAR *priHexExponent);
		// ���빫Կ
		INT			ImportPublicKey(CONST CHAR* pubHexExponent,CONST CHAR *hexModule);

	public:
		// ͨ�����ĳ��Ȼ�ȡ���ܺ�����ݴ洢����
		UINT		GetEncryptedLenFromSourceLen(CONST UINT sourceLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// ͨ�����ų��Ȼ�ȡ���ܺ�����ݳ���
		UINT		GetDecryptedLenFromSourceLen(CONST UINT sourceLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// ��ȡ��������״̬
		inline	INT	GetLockStatus(){ return m_cryptoState;};
		// ��ȡrsa���ܣ�����λ��
		inline	USHORT		GetRstEncryptoBits(){return m_bits;};

	public:
		// ���ܽ���,Ҫ֧�ֲ�ֹ���
		LONG		PubEncrypt(CONST UCHAR *pSource,CONST ULONG sourceLen,UCHAR *pDest,CONST ULONG destLen,ULONG* retDestLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// ÿ�ν��ܺ���ַ���Ҫ���н�����Ŀո�ضϴ���
		LONG		PriDecrypt(CONST UCHAR *pSource,CONST ULONG sourceLen,UCHAR *pDest,CONST ULONG destLen,ULONG* retDestLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		
	private:
		// ��֤���ܽ������ݰ������Ƿ�����Ҫ��,��PubEncrypt��PriDecrypt�ڲ�����
		LONG		CheckEncryptBuffLen(CONST UINT sourceLen,CONST UINT destBuffLen,CONST UINT bits = conCryptoBit_1024,CONST ULONG padding = RSA_PKCS1_PADDING);
		LONG		CheckDecryptBuffLen(CONST UINT sourceLen,CONST UINT destBuffLen,CONST UINT bits = conCryptoBit_1024,CONST ULONG padding = RSA_PKCS1_PADDING);

	private:
		USHORT		m_bits;						// ����λ����Ĭ��Ϊ1024
		USHORT		m_fLen;						// rsa���󳤶�,ÿ�μ��ܵ�ʱ�����ģ����ܽ��ܵ�ʱ����pading��ʽ�й�
		RSA*		m_rsaObj;					// ���ܶ���		
		USHORT		m_rsaLen;					// ras���ȣ�RSA_Size()
		BIGNUM*		m_bne;						// pub Exponent
		CryptoStatus m_cryptoState;				// ����״̬
	};

	/**************************************************************************************************
	  Object      : DesPlainDataAdapter
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/24 18:26
	  Description : Des���ܣ�openssl Ҫ�ܹ���EliteIV���жԽӣ�������������2��
					1. ���ĳ��ȱ���Ϊ8�ֽڵ�������
					2. ����������ݲ���8�ֽڣ���ô��Ҫ������䣬��ָ������������䣬�������Ϊ�̶���ֵ
					ע�⣺Ϊ��֤ͨ���ԣ�DES��ؼ��ܽ����࣬��������Ҫ�Ӹ�������
					IOpensslUtility::DesCryptoHelper
					IOpensslUtility::DesCryptoRandomKeyHelper
					SenseEliteIVUtility::EliteIVHelper
	**************************************************************************************************/
	class DesPlainDataAdapter
	{
	public:
		DesPlainDataAdapter();
		virtual ~DesPlainDataAdapter();

	protected:
		// ����ԭ���ݳ��Ȼ�ȡҪ�洢��Ҫ�Ŀռ�
		DATA_LEN_LEN	GetCryptoSourceLen(CONST DATA_LEN_LEN inSourceLen);
		// �����Ȳ���8�ֽ������������ݰ��������
		INT				PackCryptoSource(CONST UCHAR *inSource,CONST DATA_LEN_LEN inSourceLen,UCHAR *outSource,CONST DATA_LEN_LEN cryptoLen);
	};

	/**************************************************************************************************
	  Object      : DesCrypto
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 11:16
	  Description : DES ���ܽ��ܹ��ܣ�Ŀǰֻ����ʱ֧��ecbģʽ���������ecb3��������
	**************************************************************************************************/
	class DesCrypto
	{
	public:
		DesCrypto();
		virtual	~DesCrypto();
	public:
		INT		SetKey(CONST UCHAR *pkey,CONST INT keyLen = 8);
		inline	INT	GetLockStatus(){ return m_cryptoState;};
		// ���ܽ���
		LONG	EcbEncrypt(CONST UCHAR *pSource,CONST UINT sourceLen,UCHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);
		LONG	EcbDecrypt(CONST UCHAR *pSource,CONST UINT sourceLen,UCHAR	*pDest,CONST UINT destBuffLen,UINT* retDestLen);
		// ��ȡ���ܺ�ĳ��Ⱥͽ��ܺ�ĳ���
		LONG	GetEncryptLenFromSourceLen(CONST UINT sourceLen,CONST INT type = ECB);
		LONG	GetDecryptLenFromSourceLen(CONST UINT sourceLen,CONST INT type = ECB);

	private:
		DES_key_schedule	m_schedule;
		DES_cblock			m_cblock;					
		CryptoStatus		m_cryptoState;				// ����״̬
		// ���ܽ�������
		enum DesCryptType
		{
			ECB,
			ECB3,
		};
		// ��֤���ܽ������ݰ������Ƿ�����Ҫ��
		LONG	CheckEncryptBuffLen(CONST UINT sourceLen,CONST UINT destBuffLen,CONST INT type = ECB);
		LONG	CheckDecryptBuffLen(CONST UINT sourceLen,CONST UINT destBuffLen,CONST INT type = ECB);
	};

	/**************************************************************************************************
	  Object      : Base64
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 10:40
	  Description : base64ת���࣬��������ת����base64�����ַ�������base64�����ַ���ת����2����
					����base64�ַ�����ȡ�洢ת��������Ƶ��ַ������ȣ����ݶ����Ƴ��Ȼ�ȡ�洢ת����
					base64��λ��
	**************************************************************************************************/
	class Base64
	{
	public:
		Base64(void);
		virtual ~Base64(void);
	private:
		// typedef struct evp_Encode_Ctx_st
		EVP_ENCODE_CTX	m_encCtx,m_decCtx;				
		// bytes ---> base64 �ַ�����У�鳤���Ƿ����,����3�����
		INT	CheckB64BuffLen(CONST UINT	sourceLen,CONST UINT destLen);
		// base64 ---> bytes �ַ�����У���Ƿ����
		INT	CheckBytesBuffLen(CONST UINT	sourceLen,CONST UINT destLen);

	public:
		// ͨ��base������ȡ��Ҫ�洢��bytes����С�ֽ���
		UINT GetBytesNumFromB64(CONST UINT B64Len);
		// ͨ��bytes�ֽ�������ȡ��Ҫ�洢��base64����С�ֽ���
		UINT GetB64NumFromBytes(CONST UINT BytLen);
		// ��base64ת����bytes
		INT	B64ToBytes(CONST CHAR*	pSource,CONST UINT sourceLen,UCHAR	*pDest,CONST UINT destBuffLen,UINT* retDestLen);
		// ��bytesת����base64
		INT	BytesToB64(CONST UCHAR* pSource,CONST UINT sourceLen,CHAR	*pDest,CONST UINT destBuffLen,UINT* retDestLen);
	};

	/**************************************************************************************************
	  Object      : RsaCryptoHelper
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 11:18
	  Description : Rsa���ܽ��ܸ�����
					RSA����(��Կ���ܣ�˽Կ����)�����ܵĶ���һ�����ܽ��ܸ�����
					���ܹ��̣�����--->���� + ����������--->BASE64����
					���ܹ��̣�BASE--->���� + ����������--->����������
					==����Ҫ�������ݳ�������Կͷ��
	**************************************************************************************************/
	class RsaCryptoHelper
	{
	public:
		RsaCryptoHelper();
		virtual ~RsaCryptoHelper();

	public: // ���ܣ����ܹ���
		// ���ܹ��̣�����--->���ĳ���+����������--->BASE64����
		LONG	Encrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// ���ܹ��̣�BASE--->���ĳ���+����������--->����������
		LONG	Decrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen,CONST ULONG padding = RSA_PKCS1_PADDING);

		// ͨ�����ĳ��Ȼ�ȡ���ܺ�����ݴ洢����
		UINT	GetEncryedLenFromSourceLen(CONST UINT sourceLen,CONST ULONG padding = RSA_PKCS1_PADDING);
		// ͨ�����ĳ��Ȼ�ȡ���ܺ�����ݳ���
		UINT	GetDecryptedFromSourceLen(CONST UINT sourceLen,CONST ULONG padding = RSA_PKCS1_PADDING);

	public:
		// �����ⲿ���ýӿ�,�����Ե�����Կ�����ߵ�����Կ�ȣ�Ҳ����ֱ�ӵ��øýӿڼ��ܣ�����
		inline	RsaCrypto*	GetRsaCrypto(){return &m_oRsaCrypto;}

	private:
		RsaCrypto	m_oRsaCrypto;			// rsa object
		Base64		m_oBase64;				// base64 object
	};

	/**************************************************************************************************
	  Object      : DesCryptoHelper
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/6 11:19
	  Description : DES���ܽ��ܸ�����
					DES����ecb�����ܵĶ���һ�����ܽ��ܸ�����
					���ܹ��̣�����--->����������--->BASE64����
					���ܹ��̣�BASE--->����������--->����������
					==����Ҫ�������ݳ�������Կͷ��
	**************************************************************************************************/
	class DesCryptoHelper:public DesPlainDataAdapter
	{
	public:
		DesCryptoHelper();
		virtual ~DesCryptoHelper();

	public: // ���ܣ����ܹ���
		// ���ܹ��̣�����--->����+����������--->BASE64����
		LONG	Encrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);
		// ���ܹ��̣�BASE--->����+����������--->����������
		LONG	Decrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);

		// ͨ�����ĳ��Ȼ�ȡ���ܺ�����ݴ洢����
		UINT	GetEncryedLenFromSourceLen(CONST UINT sourceLen);
		// ͨ�����ų��Ȼ�ȡ���ܺ�����ݳ���
		UINT	GetDecryptedFromSourceLen(CONST UINT sourceLen);

	public:
		// �����ⲿ���ýӿ�,�����Ե�����Կ�����ߵ�����Կ�ȣ�Ҳ����ֱ�ӵ��øýӿڼ��ܣ�����
		inline	DesCrypto*	GetDesCrypto(){return &m_oDesCrypto;}

	private:
		DesCrypto	m_oDesCrypto;				// des object 
		Base64		m_oBase64;					// base64 object
	};

	/**************************************************************************************************
	  Object      : DesCryptoRandomKeyHelper
	  Author      : Liujs     
	  Version     : V1.00        
	  DateTime    : 2012/9/20 9:10
	  Description :  DES���ܽ��ܸ�����(֧�ֶ�̬��Կ�Ĳ�������������Դ����̬��������Կ��λ�ã�
					��8K��Կ�е�����Կ���м��ܽ���)��DES����ecb�����ܵĶ���һ�����ܽ��ܸ�����
	  ���ܹ��̣�����--->8K��Կ�ļ���ȡ��Կ--->����������--->��Կ����+���ĳ���+����������--->BASE64����
	  ���ܹ��̣�BASE64����--->��Կ����+���ĳ���+����������--->8K��Կ�ļ���ȡ��Կ + Դ���ݳ��� ---> ����
	**************************************************************************************************/
	class DesCryptoRandomKeyHelper:public DesPlainDataAdapter
	{
	public:
		DesCryptoRandomKeyHelper();
		virtual ~DesCryptoRandomKeyHelper();

	public: 
		// ���ܹ��̣�����--->8K��Կ�ļ���ȡ��Կ--->����������--->��Կ����+���ĳ���+����������--->BASE64����
		LONG	Encrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);
		// ���ܹ��̣�BASE64����--->��Կ����+���ĳ���+����������--->8K��Կ�ļ���ȡ��Կ + Դ���ݳ��� ---> ����
		LONG	Decrypt(CONST CHAR *pSource,CONST UINT sourceLen,CHAR *pDest,CONST UINT destBuffLen,UINT* retDestLen);

		// ͨ�����ĳ��Ȼ�ȡ���ܺ�����ݴ洢����
		UINT	GetEncryedLenFromSourceLen(CONST UINT sourceLen);
		// ͨ�����ų��Ȼ�ȡ���ܺ�����ݳ���
		UINT	GetDecryptedFromSourceLen(CONST UINT sourceLen);

	public:
		// �����ⲿ���ýӿ�,�����Ե�����Կ�����ߵ�����Կ�ȣ�Ҳ����ֱ�ӵ��øýӿڼ��ܣ�����
		inline	DesCrypto*	GetDesCrypto(){return &m_oDesCrypto;}

	public:
		// dump keys source file data to m_pKeySourceFile,dump keys size 1024 ,file size 8192
		INT		DumpKeysSourceFile(CONST	UCHAR *pKeySource,CONST UINT keySourceLen);
		// find key from the keys source file with the source and source len,encrypt using
		INT		FindKeyFromKeysSourceFile(CONST UCHAR *pSource,CONST UINT sourceLen,UCHAR *pKey);
		// find key from the keys source file with keyIndex,decrypt using
		INT		FindKeyFromKeysSourceFile(CONST USHORT	keyIndex,UCHAR *pKey);

	private:
		// key index get from data source and len 
		USHORT	GetKeyIndexFromPlainText(CONST UCHAR *pSource,CONST UINT sourceLen);

	private:
		DesCrypto	m_oDesCrypto;				// des object 
		Base64		m_oBase64;					// base64 object
		UCHAR		*m_pKeySourceFile;			// keys source file ,size 8KB

	private:
		UCHAR		m_mdHash[conMd5SumLen];		// md5 hash len for each encrypt

	};
	//#pragma pack()
}

#endif
