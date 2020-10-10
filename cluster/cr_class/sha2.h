#ifndef _SHA2_H
#define _SHA2_H

#include <cr_class/cr_addon.h>

/*** SHA-256/384/512 Various Length Definitions ***********************/
#define SHA256_BLOCK_LENGTH		64
#define SHA256_DIGEST_LENGTH		32
#define SHA256_DIGEST_STRING_LENGTH	(SHA256_DIGEST_LENGTH * 2 + 1)
#define SHA384_BLOCK_LENGTH		128
#define SHA384_DIGEST_LENGTH		48
#define SHA384_DIGEST_STRING_LENGTH	(SHA384_DIGEST_LENGTH * 2 + 1)
#define SHA512_BLOCK_LENGTH		128
#define SHA512_DIGEST_LENGTH		64
#define SHA512_DIGEST_STRING_LENGTH	(SHA512_DIGEST_LENGTH * 2 + 1)

#ifdef __cplusplus
extern "C" {
#endif

/*** SHA-256/384/512 Context Structure *******************************/
typedef struct _SHA2_CTX {
	union {
		uint32_t	st32[8];
		uint64_t	st64[8];
	} state;
	uint64_t	bitcount[2];
	uint8_t	buffer[SHA512_BLOCK_LENGTH];
} SHA2_CTX;

void SHA256Init(SHA2_CTX *);
void SHA256Update(SHA2_CTX *, const uint8_t *, size_t);
void SHA256Final(uint8_t[SHA256_DIGEST_LENGTH], SHA2_CTX *);

void SHA384Init(SHA2_CTX *);
void SHA384Update(SHA2_CTX *, const uint8_t *, size_t);
void SHA384Final(uint8_t[SHA384_DIGEST_LENGTH], SHA2_CTX *);

void SHA512Init(SHA2_CTX *);
void SHA512Update(SHA2_CTX *, const uint8_t *, size_t);
void SHA512Final(uint8_t[SHA512_DIGEST_LENGTH], SHA2_CTX *);

#ifdef __cplusplus
}
#endif

#endif /* _SHA2_H */
