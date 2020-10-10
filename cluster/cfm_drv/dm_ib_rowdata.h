#ifndef __H_DM_IB_ROWDATA_H__
#define __H_DM_IB_ROWDATA_H__

#include <time.h>
#include <cr_class/cr_class.h>
#include <cfm_drv/dm_ib_tableinfo.h>

#define DM_IB_ROWDATA_DATE_FORMAT_STRING	"%Y%m%d%H%M%S"

namespace DM_IB_RowData_NS {
	typedef union {
		struct {
			uint64_t second : 6;
			uint64_t minute : 6;
			uint64_t hour   : 16;
			uint64_t day    : 5;
			uint64_t month  : 4;
			uint64_t year   : 14;
			uint64_t unused : 13;
		} fields;
		CR_Class_NS::union64_t union_v;
	} ib_datetime_t;

	int64_t mkibdate(int16_t year, uint8_t month, uint8_t day,
			uint8_t hour, uint8_t minute, uint8_t second);

	void tm2ibdate(const struct tm &t, int64_t &ibdate);
	void ibdate2tm(const int64_t ibdate, struct tm &t);

	void tm2str(const struct tm &t, char *t_str);
	void str2tm(const char *t_str, struct tm &t);

	void str2ibdate(const char *t_str, int64_t &ibdate);
	void ibdate2str(const int64_t ibdate, char *t_str);
};

class DM_IB_RowData {
public:
	DM_IB_RowData()
		: _table_info_p(NULL), _row_data_p(NULL), _next_str_slot(0),
		  _null_slot(0), _max_slot(0), _is_readonly(false)
	{
	}

	~DM_IB_RowData();

	void Reset();

	void Clear();

	void BindTableInfo(const DM_IB_TableInfo &table_info);

	int SetNull(const size_t col_id);
	int SetInt64(const size_t col_id, const int64_t v);
	int SetDouble(const size_t col_id, const double v);
	int SetDateTime(const size_t col_id, const int64_t ibdate);
	int SetDateTime(const size_t col_id, const struct tm &v);
	int SetDateTime(const size_t col_id, const char *v);
	int SetString(const size_t col_id, const char *v, const size_t vlen);

	bool IsNull(const size_t col_id) const;

	int GetInt64(const size_t col_id, int64_t &v) const;
	int GetDouble(const size_t col_id, double &v) const;
	int GetDateTime(const size_t col_id, int64_t &v) const;
	int GetDateTime(const size_t col_id, struct tm &v) const;
	int GetDateTime(const size_t col_id, char *v) const;
	int GetString(const size_t col_id, const char *&v, size_t &vlen) const;

	std::string GetSortAbleColKey(size_t col_id, const bool use_null_prefix=true) const;

	int Data(const void *&data, size_t &size) const;
	int MapFromArray(const void * buf, const size_t buf_size);

private:
	const DM_IB_TableInfo *_table_info_p;
	CR_Class_NS::union64_t *_row_data_p;
	uint16_t _next_str_slot;
	uint16_t _null_slot;
	uint16_t _max_slot;
	bool _is_readonly;
};

#endif /* __H_DM_IB_ROWDATA_H__ */
