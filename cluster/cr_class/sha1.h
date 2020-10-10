#ifndef _SHA1_H_
#define _SHA1_H_

#include <cr_class/cr_addon.h>

#define	SHA1_BLOCK_LENGTH		64
#define	SHA1_DIGEST_LENGTH		20

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
	uint32_t	state[5];
	uint64_t	count;
	uint8_t	buffer[SHA1_BLOCK_LENGTH];
} SHA1_CTX;

void SHA1Init(SHA1_CTX * context);
void SHA1Transform(uint32_t state[5], const uint8_t buffer[SHA1_BLOCK_LENGTH]);
void SHA1Update(SHA1_CTX *context, const uint8_t *data, unsigned int len);
void SHA1Final(uint8_t digest[SHA1_DIGEST_LENGTH], SHA1_CTX *context);

#ifdef __cplusplus
}
#endif

#endif /* _SHA1_H_ */
