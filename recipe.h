#ifndef __RECIPE_H
#define __RECIPE_H

#include "common.h"
#include "decold.h"

//Destor recipe meta struct
struct fileRecipeMeta
{
	uint64_t chunknum;
	uint64_t filesize;

	//filename may do not need, with NameNode, we only need fid.
	uint64_t fid;
	uint64_t offset;
};
//Destor recipe struct
struct chunkPointer
{
	fingerprint fp;
	containerid id;
	int32_t size;
};

struct file_info;

//we ignore the container, but recorder the size and order
struct fp_info
{
	fingerprint fp;
	uint64_t order; // record the sequence of fp in the file
	int32_t size; //recorder the size
	uint64_t fid;
	containerid cid;
	struct file_info *fi;
};

struct file_info
{
	uint64_t fid;

	uint64_t size;
	uint64_t chunknum; //for to choose intersection & migration
	// TO-DO: in which recipe
	uint64_t offset_m;
	uint64_t offset_r;

	uint64_t fp_info_start;
	struct fp_info *first_fp_in_inter_fps;
};

struct remained_file_info
{
    uint64_t fid;
    uint64_t filesize;
    uint64_t chunknum;
    fingerprint *fps;
    uint64_t *fps_cid;
	int32_t *fps_sizes;
};


#define CONTAINER_SIZE (4194304ll) //4MB
#define CONTAINER_META_SIZE (32768ll) //32KB
#define CONTAINER_HEAD 16
#define CONTAINER_META_ENTRY 28
#define TEMPORARY_ID (-1L)

void read_recipe(const char *path, struct fp_info **s1, int64_t *s1_count, struct file_info **mr, int64_t *mr_count, int64_t *empty_count);

//Destor func
int32_t retrieve_from_container(FILE *pool_fp, containerid cid, unsigned char** v, fingerprint fps);

//void get_chunk_in_container();

#endif
