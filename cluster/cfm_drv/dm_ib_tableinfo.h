#ifndef __H_DM_IB_TABLEINFO_H__
#define __H_DM_IB_TABLEINFO_H__

#include <string>
#include <vector>
#include <cr_class/cr_addon.h>
#include <cr_class/cr_automsg.h>

#define DM_IB_TABLEINFO_CT_INT		(0)
#define DM_IB_TABLEINFO_CT_BIGINT	(1)
#define DM_IB_TABLEINFO_CT_CHAR		(2)
#define DM_IB_TABLEINFO_CT_VARCHAR	(3)
#define DM_IB_TABLEINFO_CT_FLOAT	(4)
#define DM_IB_TABLEINFO_CT_DOUBLE	(5)
#define DM_IB_TABLEINFO_CT_DATETIME	(6)

class DM_IB_TableInfo {
public:
	typedef struct {
		uint8_t column_type;
		uint16_t column_scale;	// max length
		uint16_t column_flags;
		std::string column_name;
		std::string column_comment;
	} column_info_t;

	DM_IB_TableInfo();
	~DM_IB_TableInfo();

	inline size_t size() const
	{
		return this->table_info.size();
	}

	inline void clear()
	{
		this->table_info.clear();
	}

	bool push_back(const std::string &column_name,
		const uint8_t column_type, const uint16_t column_scale,
		const uint16_t column_flags=0, const std::string &column_comment="");

	inline const column_info_t& operator[](size_t pos) const
	{
		return this->table_info[pos];
	}

	int SerializeToString(std::string &tbl_info_str) const;
	int ParseFromString(const std::string &tbl_info_str);

	std::string Print(ssize_t col_id=-1) const;
private:
	std::vector<column_info_t> table_info;

	std::string print_one(size_t col_id) const;
};

#endif /* __H_DM_IB_TABLEINFO_H__ */
