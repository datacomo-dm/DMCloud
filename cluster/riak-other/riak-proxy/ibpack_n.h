#ifndef __H_IBPACK_N_H__
#define __H_IBPACK_N_H__

#include <stdint.h>
#include <vector>

#include "msgIB.pb.h"

class IBDataPack_N {
	msgRCAttr_packN msgPackN;
	enum ValueType{ UCHAR = 1, USHORT = 2, UINT = 4, UINT64 = 8};
public:
	IBDataPack_N(const std::string &packbuf);
	~IBDataPack_N();

	int Save(std::string &packbuf, IBRiakDataPackQueryParam &msgIBQP);
private:
	const char *data_ptr;
	size_t data_size;
	const uint8_t *nulls_ptr;
	size_t nulls_size;
	uint32_t no_obj;
	ValueType value_type;

	inline int CalculateByteSize(uint64_t n) const;
	inline ValueType ChooseValueType(const int bytesize) const;

	uint64_t get_value_unsigned(const uint16_t pos, const bool pack_is_float);
	int64_t get_value_signed(const uint16_t pos, const bool pack_is_float);
	double get_value_double(const uint16_t pos, const bool pack_is_float);

	bool _cond_cmp(const uint16_t pos, const IBRiakDataPackQueryParam &msgIBQP);

	bool _keep_full_data_pic(const IBRiakDataPackQueryParam &msgIBQP, const uint32_t cond_value_count);
};

#endif
