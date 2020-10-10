#ifndef _MD5_H_
#define _MD5_H_

#include <cr_class/cr_addon.h>

#define	MD5_BLOCK_LENGTH		64
#define	MD5_DIGEST_LENGTH		16

#ifdef __cplusplus
extern "C" {
#endif

typedef struct MD5Context {
	uint32_t state[4];			/* state */
	uint64_t count;			/* number of bits, mod 2^64 */
	uint8_t buffer[MD5_BLOCK_LENGTH];	/* input buffer */
} MD5_CTX;

void	 MD5Init(MD5_CTX *);
void	 MD5Update(MD5_CTX *, const uint8_t *, size_t);
void	 MD5Final(uint8_t [MD5_DIGEST_LENGTH], MD5_CTX *);
void	 MD5Transform(uint32_t [4], const uint8_t [MD5_BLOCK_LENGTH]);

#ifdef __cplusplus
}
#endif

#endif /* _MD5_H_ */
