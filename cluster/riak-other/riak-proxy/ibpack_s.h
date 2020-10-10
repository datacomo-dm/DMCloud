#ifndef __H_IBPACK_S_H__
#define __H_IBPACK_S_H__

#include <stdint.h>
#include <vector>
#include <string>

#include "msgIB.pb.h"

class IBDataPack_S {
	msgRCAttr_packS msgPackS;
public:
	IBDataPack_S(const std::string &packbuf);
	~IBDataPack_S();

	int Save(std::string &packbuf, IBRiakDataPackQueryParam &msgIBQP);
private:
	uint32_t no_obj;
	const char **lines_p;
	const uint16_t *lens;
	size_t lens_count;

	bool _cond_cmp(const uint16_t pos, const IBRiakDataPackQueryParam &msgIBQP);
	bool _keep_full_data_pic(const IBRiakDataPackQueryParam &msgIBQP, const uint32_t cond_value_count);
};

#endif
