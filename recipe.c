#include "recipe.h"
#include "containerstore.h"
#include "serial.h"

// int64_t demo_fid =0;

char base_path[128] = {0};

static int64_t read_next_n_chunk_pointers(FILE *recipe_fp, int64_t fid, struct fp_info *s1, int64_t *s1_count, int64_t num, struct file_info *mr)
{
	struct chunkPointer *cp = (struct chunkPointer *) malloc(sizeof(struct chunkPointer));

	int64_t max_cid=0;

	int64_t i;
	for (i = 0; i < num; i++)
	{
		fread(&(cp->fp), sizeof(fingerprint), 1, recipe_fp);
		fread(&(cp->id), sizeof(containerid), 1, recipe_fp);
		fread(&(cp->size), sizeof(int32_t), 1, recipe_fp);
	    
		// Ignore segment boundaries
		//only count really chunk, escape FILE and SEGMENT
		if (cp->id == 0 - CHUNK_SEGMENT_START || cp->id == 0 - CHUNK_SEGMENT_END)
		{
			i--;
			continue;
		};

		memcpy(s1[*s1_count].fp, cp->fp, sizeof(fingerprint));
		s1[*s1_count].fid = fid;
		s1[*s1_count].order = i;
		s1[*s1_count].size = cp->size;
		s1[*s1_count].cid = cp->id;
		s1[*s1_count].fi = mr;

		if(cp->id > max_cid)
			max_cid = cp->id;

		(*s1_count)++;
	}
	free(cp);

	return max_cid;
}

void read_recipe(const char *path, struct fp_info **s1_t, int64_t *s1_count, struct file_info **mr_t, int64_t *mr_count, int64_t *empty_count)
{

	static uint64_t fp_start = 0;

	char path_meta[100], path_recipe[100];
	sprintf(path_meta, "%s/%s", path, "bv0.meta");
	sprintf(path_recipe, "%s/%s", path, "bv0.recipe");

	FILE *meta_fp = fopen(path_meta, "r");
	if (!meta_fp) 
		printf("fopen %s failed\n", path_meta);
	FILE *recipe_fp = fopen(path_recipe, "r");
	if (!recipe_fp)
		printf("fopen %s failed\n", path_recipe);

	//head of metadata file, some for backup job
	int32_t bv_num;
	int deleted;
	int64_t number_of_files;
	int64_t number_of_chunks;

	//backup info
	fseek(meta_fp, 0, SEEK_SET);
	fread(&bv_num, sizeof(bv_num), 1, meta_fp);
	fread(&deleted, sizeof(deleted), 1, meta_fp);
	fread(&number_of_files, sizeof(number_of_files), 1, meta_fp);
	fread(&number_of_chunks, sizeof(number_of_chunks), 1, meta_fp);

	int pathlen;
	//path
	fread(&pathlen, sizeof(int), 1, meta_fp);

	fread(base_path, pathlen, 1, meta_fp);
	base_path[pathlen] = 0;

	//fp->fid
	//int64_t s1_count=0, mr_count=0;
	struct fp_info *s1 = (struct fp_info *)malloc(number_of_chunks * sizeof(struct fp_info));
	struct file_info *mr = (struct file_info *)malloc(number_of_files * sizeof(struct file_info));

	uint64_t total_data_size = 0;
	int64_t i;
	for (i = 0; i < number_of_files; i++)
	{
		//meta start
		mr[*mr_count].offset_m = ftell(meta_fp);

		struct fileRecipeMeta *file = (struct fileRecipeMeta *) malloc(sizeof(struct fileRecipeMeta));	

		fread(&file->fid, sizeof(file->fid), 1, meta_fp);
		fread(&file->offset, sizeof(file->offset), 1, meta_fp);
		fread(&file->chunknum, sizeof(file->chunknum), 1, meta_fp);
		fread(&file->filesize, sizeof(file->filesize), 1, meta_fp);

		total_data_size += file->filesize;	

		//map_recipe
		mr[*mr_count].fid = file->fid;
		mr[*mr_count].size = file->filesize;
		mr[*mr_count].chunknum = file->chunknum;
		//recipe start
		mr[*mr_count].offset_r = ftell(recipe_fp);

		mr[*mr_count].fp_info_start = fp_start;	
		fp_start +=  file->chunknum;
		
		mr[*mr_count].first_fp_in_inter_fps = s1 + *s1_count;

		if (file->filesize == 0)
			(*empty_count)++;

		(*mr_count)++;

		//summary
		read_next_n_chunk_pointers(recipe_fp, file->fid, s1, s1_count, file->chunknum, mr + *mr_count - 1);
	}

	//assign
	*s1_t = s1;
	*mr_t = mr;

	fclose(meta_fp);
	fclose(recipe_fp);

	printf("%s data size:%lu\n", path, total_data_size);
}

static int32_t get_chunk_from_container(unsigned char **vv, struct container *c, fingerprint fp)
{
	struct metaEntry* me = g_hash_table_lookup(c->meta.map, fp);
	if (!c->meta.map)
		VERBOSE("ASSERT, META.MAP == NULL!\n");

	if (!me)
		VERBOSE("ASSERT, ME == NULL!\n");

	if (me) {
	    *vv = (unsigned char *)malloc(me->len);
	    memcpy(*vv, c->data + me->off, me->len);
	}

	if (me)
	    return me->len;
	else 
	    return -1;
}

//for Destor func, locality
int32_t retrieve_from_container(FILE* pool_fp, containerid cid, unsigned char **v, fingerprint fp)
{
	struct container *c = (struct container*) malloc(sizeof(struct container));
	c->meta.chunk_num = 0;
	c->meta.data_size = 0;
	c->meta.id = cid;
	c->meta.map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL, free);

	unsigned char *cur = 0;

	//only malloc the meta data
	c->data = malloc(CONTAINER_SIZE);

	if (-1 == fseek(pool_fp, cid * CONTAINER_SIZE + 8, SEEK_SET)) {
		printf("fseek failed, offset:%d\n",  cid * CONTAINER_SIZE + 8);
	}

	int32_t ret = fread(c->data, CONTAINER_SIZE, 1, pool_fp);
	if (ret != 1) {
		printf("fread container failed:%d != %d\n", ret, 1);
	}
	cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];

	int i;
	unser_declare;
	unser_begin(cur, CONTAINER_META_SIZE);

	unser_int64(c->meta.id);
	unser_int32(c->meta.chunk_num);
	unser_int32(c->meta.data_size);

	myprintf("read container id:%ld, chunk_num:%d, data_size:%d\n", c->meta.id, c->meta.chunk_num, c->meta.data_size);

	
	for (i = 0; i < c->meta.chunk_num; i++)
	{
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		g_hash_table_insert(c->meta.map, &me->fp, me);
	}

	if (!c->meta.map)
		VERBOSE("ASSERT, META.MAP == NULL!\n");

	unser_end(cur, CONTAINER_META_SIZE);

	int32_t chunk_size = get_chunk_from_container(v, c, fp);

	g_hash_table_destroy(c->meta.map);
	free(c->data);
	free(c);

	return chunk_size;
}
