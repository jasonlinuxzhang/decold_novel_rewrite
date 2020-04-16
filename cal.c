#include "cal.h"
#include <unistd.h>

int64_t MIGRATION_COUNT = 0;
extern int enable_migration;
extern int enable_refs;
extern int enable_topk;
extern unsigned long int big_file;
extern float migration_threshold;


static int comp(const void *s1, const void *s2)
{
	return comp_code(((struct fp_info *)s1)->fp, ((struct fp_info *)s2)->fp);
}

static int comp_scommon(const void *s1, const void *s2)
{
	return ((struct fp_info *)s1)->fid - ((struct fp_info *)s2)->fid;
}

static int comp_mr(const void *mr1, const void *mr2)
{
	return ((struct file_info *)mr1)->fid - ((struct file_info *)mr2)->fid;
}


//we copy another S1 S2 for R1 and migartion sort
void cal_inter(GHashTable *g1_unique_chunks, GHashTable *g2_unique_chunks, struct fp_info *s1, int64_t s1_count, struct fp_info **scommon1, int64_t *sc1_count)
{
	struct fp_info *scommon11 = (struct fp_info *)malloc(sizeof(struct fp_info) * s1_count);

	int64_t a = 0;
	while (a < s1_count)
	{

		struct chunk * ck = g_hash_table_lookup(g1_unique_chunks, &s1[a].fp);
		if (NULL == ck) {
			printf(BACKGROUND_COLOR_RED"can't find ck in g1 hash table"COLOR_NONE);
			exit(-1);
		}

		if (NULL == g_hash_table_lookup(g2_unique_chunks, &s1[a].fp)) {
			a++;
			continue;
		}

		memcpy(scommon11[*sc1_count].fp, s1[a].fp, sizeof(fingerprint));
		scommon11[*sc1_count].size = s1[a].size;

		scommon11[*sc1_count].cid = s1[a].cid;
		scommon11[*sc1_count].fid = s1[a].fid;
		scommon11[*sc1_count].order = s1[a].order;
		scommon11[*sc1_count].fi = s1[a].fi;
		(*sc1_count)++;
		a++;
	}

	scommon11 = (struct fp_info *)realloc(scommon11, *sc1_count * sizeof(struct fp_info));

	*scommon1 = scommon11;
}

//check the full chunks in the S common
//0 none and migration < shreshold, <0 ->migration, >0 intersection
//if the bigger file, threshold smaller
static int64_t file_dedup_migration(struct file_info *mr, uint64_t count, uint64_t fid, uint64_t chunknum, int64_t mig_count[8], uint64_t *file_index)
{
	//binarySearch
	int64_t begin = 0, end = count - 1;
	while (begin <= end)
	{
		int64_t mid = (begin + end) / 2;
		if (mr[mid].fid == fid)
		{
			*file_index = mid;
			//TO-DO here for miagration!!!
			if (mr[mid].chunknum == chunknum)
				return mr[mid].chunknum; //>0 intersection
			else
			{
				//only big file migration
				if (enable_migration == 1)
				{
					double ratio = 1.0 * chunknum / mr[mid].chunknum;
					if (mr[mid].size >= big_file && ratio >= migration_threshold)
					{
						if (LEVEL >= 2)
							VERBOSE("BIG MIGRATION NO.%" PRId64 ", [%8" PRId64 "](size=%"PRId64"), { get [%" PRId64 "] of [%" PRId64 "] = %lf.} ==> MAY BE MIGRATION!\n", \
							MIGRATION_COUNT++, fid, mr[mid].size, chunknum, mr[mid].chunknum, ratio);

						if(ratio >=0.6)
						{
							mig_count[0]++;
							if(ratio >=0.65)
							{
								mig_count[1]++;
								if(ratio >=0.7)
								{
									mig_count[2]++;
									if(ratio >=0.75)
									{
										mig_count[3]++;
										if(ratio >=0.8)
										{
											mig_count[4]++;
											if(ratio >=0.85)
											{
												mig_count[5]++;
												if(ratio >=0.9)
												{
													mig_count[6]++;
													if(ratio >=0.95)
													{
														mig_count[7]++;
													}
												}
											}
										}
									}
								}
							}

						}

						return 0 - mr[mid].chunknum; //mark for migration
					}

				}

				return 0; //migration < shreshold
			}
		}
		else if (mr[mid].fid < fid)
			begin = mid + 1;
		else
			end = mid - 1;
	}

	return 0; //none
}



void file_find(GHashTable *g2_unique_chunks, struct file_info *mr, int64_t mr_count, struct fp_info *sc, int64_t sc_count, struct identified_file_info **fff, int64_t *ff_count, struct migrated_file_info **mm, int64_t *m_count, int64_t mig_count[8])
{
	int64_t i;

	struct identified_file_info *ff = (struct identified_file_info *)malloc(mr_count * sizeof(struct identified_file_info));
	struct migrated_file_info *m = (struct migrated_file_info *)malloc(mr_count * sizeof(struct migrated_file_info));


	char *chunk_state = NULL;
	uint64_t last_chunk_num = 0;
	for (i = 0; i < mr_count; i++)
	{
		int32_t chunk_num = mr[i].chunknum;
		uint64_t fp_info_start = mr[i].fp_info_start;
		int32_t file_size = mr[i].size;
		struct file_info *fi = mr + i;
		
		if (chunk_state == NULL || chunk_num > last_chunk_num) {
			if (NULL != chunk_state) 
				free(chunk_state);

			chunk_state = malloc(chunk_num);
			memset(chunk_state, 0, chunk_num);
			last_chunk_num = chunk_num;
		}

		int j = 0;
		int non_same_chunk_count = 0;
		for (j = fp_info_start; j < fp_info_start + chunk_num; j++) {
			if ((1.0)*non_same_chunk_count/chunk_num > 1 - migration_threshold)
				break;
			if (NULL == g_hash_table_lookup(g2_unique_chunks, &sc[j].fp)) {
				non_same_chunk_count++;		
				if (file_size <= big_file) 
					break;
				chunk_state[j - fp_info_start] = 1;
			}
		}
	
		if (j != fp_info_start + chunk_num)
			continue;
		if ((1.0)*non_same_chunk_count/chunk_num > 1 - migration_threshold)
			continue;

		if (0 == non_same_chunk_count) {
			//put the files in the common file
			ff[*ff_count].filesize = fi->size;
			ff[*ff_count].fid = fi->fid;
			ff[*ff_count].num = chunk_num;
			ff[*ff_count].fps = (fingerprint *)malloc(chunk_num * sizeof(fingerprint));
			ff[*ff_count].sizes = (int32_t *)malloc(chunk_num * sizeof(int64_t));

			int64_t s;
			for (s = fp_info_start; s < fp_info_start + chunk_num ; s++)
			{
				int64_t order = s - fp_info_start;
				memcpy((ff[*ff_count].fps)[order], sc[s].fp, sizeof(fingerprint));
				(ff[*ff_count].sizes)[order] = sc[s].size;
			}
			(*ff_count)++;
		} else {
			if (1 - (1.0)*non_same_chunk_count/chunk_num >= 0.95) 
				mig_count[7]++;
			else if (1 - (1.0)*non_same_chunk_count/chunk_num >= 0.90)
				mig_count[6]++;
			else if (1 - (1.0)*non_same_chunk_count/chunk_num >= 0.85)
				mig_count[5]++;
			else if (1 - (1.0)*non_same_chunk_count/chunk_num >= 0.80)
				mig_count[4]++;
			else if (1 - (1.0)*non_same_chunk_count/chunk_num >= 0.75)
				mig_count[3]++;
			else if (1 - (1.0)*non_same_chunk_count/chunk_num >= 0.70)
				mig_count[2]++;
			

			m[*m_count].filesize = fi->size;
			m[*m_count].fid = fi->fid;
			m[*m_count].total_num = fi->chunknum;
			m[*m_count].fps = (fingerprint *)malloc(fi->chunknum * sizeof(fingerprint));
			m[*m_count].arr = (uint64_t *)calloc(1, 2 * fi->chunknum * sizeof(int64_t));
			m[*m_count].fp_cids = (int64_t *)calloc(1, fi->chunknum * sizeof(containerid));
			m[*m_count].fp_info_start = fi->fp_info_start;

			memset(m[*m_count].arr, 0, 2 * fi->chunknum * sizeof(int64_t));
			memset(m[*m_count].fp_cids, 0, fi->chunknum * sizeof(containerid));

			int64_t s;
			for (s = 0; s < chunk_num; s++)
			{
				//fps are sorted by order
				memcpy((m[*m_count].fps)[s], sc[s + fp_info_start].fp, sizeof(fingerprint));

				(m[*m_count].arr)[s] = sc[s + fp_info_start].size;
				//in ==1, mean it in the scommon
				if (0 == chunk_state[s]) 
					(m[*m_count].arr)[s + fi->chunknum] = 1;
				else 
					(m[*m_count].arr)[s + fi->chunknum] = 0;
				//cid
				(m[*m_count].fp_cids)[s] = sc[s + fp_info_start].cid;
			}
			(*m_count)++;
		}
	}

	ff = (struct identified_file_info *)realloc(ff, *ff_count * sizeof(struct identified_file_info));
	*fff = ff;

	if (enable_migration == 1)
	{
		m = (struct migrated_file_info *)realloc(m, *m_count * sizeof(struct migrated_file_info));
		*mm = m;
	}
}

