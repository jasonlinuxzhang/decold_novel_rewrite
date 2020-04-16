#ifndef CAL_H_
#define CAL_H_

#include "common.h"
#include "recipe.h"

struct identified_file_info
{
	uint64_t fid;
	uint64_t num;
	uint64_t filesize;
	uint64_t flag; //1-> dedup, 0-> stay
	fingerprint *fps; //sorted by order
	int32_t *sizes; //for size
	containerid *fp_cids; //for container ids
};

struct migrated_file_info
{
	uint64_t fid;
	uint64_t total_num;
	uint64_t filesize;
	fingerprint *fps; //sorted by order
	uint64_t *arr;//0~total, for size; total~2*total, for if it exist
	// int64_t *sizes; //for size
	// int64_t *in; //if it exist
	containerid *fp_cids;
	uint64_t fp_info_start;
	uint64_t *migrated_chunk_offset;
};

void cal_inter(GHashTable *g1_unique_chunks, GHashTable *g2_unique_chunks, struct fp_info *s1, int64_t s1_count, struct fp_info **scommon1, int64_t *sc1_count);

void file_find(GHashTable *g2_unique_chunks, struct file_info *mr, int64_t mr_count, struct fp_info *sc, int64_t sc_count, struct identified_file_info **fff, int64_t *ff_count, struct migrated_file_info **mm, int64_t *m_count, int64_t mig_count[8]);

#endif
