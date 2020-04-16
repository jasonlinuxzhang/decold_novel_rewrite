#include "recipe.h"
#include "cal.h"
#include "common.h"
#include "queue.h"
#include "sync_queue.h"
#include "decold.h"
#include "containerstore.h"

extern char base_path[128];

int enable_migration = 1;
int enable_refs = 0;
int enable_topk = 0;
long int big_file = 0;
float migration_threshold = 0.8;

char g1_temp_path[128] = {0};
char g2_temp_path[128] = {0};

char g1_path[128] = {0};
char g2_path[128] = {0};

SyncQueue *write_identified_file_temp_queue;
SyncQueue *write_identified_file_to_destor_queue;

SyncQueue *write_migrated_file_temp_queue;

SyncQueue *write_destor_queue;

SyncQueue *remained_files_queue;


SyncQueue *write_g1_remained_files_queue;
SyncQueue *write_g2_remained_files_queue;


pthread_t tid1;
pthread_t tid3;

pthread_t tid5;

containerid container_count;

GHashTable *g1_unique_chunks, *g2_unique_chunks;
GHashTable *rewrited_files;


static int comp_fp_by_fid(const void *s1, const void *s2)
{
    return ((struct fp_info *)s1)->fid - ((struct fp_info *)s2)->fid;
}

static int64_t find_first_fp_by_fid(struct fp_info *fps, uint64_t fp_count, uint64_t fid, uint64_t left_start, uint64_t right_start)
{
    uint64_t middle = 0;
    uint64_t left = left_start, right = right_start;
    while (left <= right) {
	middle = (left + right) / 2;
	if (fps[middle].fid == fid)
	    break;
	else if (fps[middle].fid < fid)
	    left = middle + 1;
	else
	    right = middle - 1;
    }

    if (left > right) {
	return -1;
    }

    return middle -  fps[middle].order;
}

void push_migriated_files(struct migrated_file_info *migrated_files, uint64_t migrated_file_count, SyncQueue *queue) {
	int i = 0;
	for(i = 0; i < migrated_file_count; i++) {
		//printf ("push migrated file %ld\n", migrated_files->fid);
	    sync_queue_push(queue, migrated_files + i);
	}
	sync_queue_term(queue);
}

void push_identified_files(struct identified_file_info *identified_files, uint64_t identified_file_count, SyncQueue *queue) {
	int i = 0;
	for(i = 0; i < identified_file_count; i++) {
	    sync_queue_push(queue, identified_files + i);
	}
	sync_queue_term(queue);
}



void update_remained_files(int group, struct file_info *files, uint64_t file_count, struct fp_info *s1_ord, uint64_t s1_count, struct identified_file_info *identified_files, uint64_t identified_file_count, struct migrated_file_info *migrated_files, uint64_t migrated_file_count)
{
	uint64_t remained_file_count = 0;
	int low = 0, high = s1_count - 1;
	qsort(s1_ord, s1_count, sizeof(struct fp_info), comp_fp_by_fid);
	int left = 0, right = 0;
	int i = 0;

	GHashTable *rewrite_files = g_hash_table_new_full(g_int64_hash, g_fid_equal, NULL, free_fid);
	
	double time_1 = 0;
    TIMER_DECLARE(1);
    TIMER_BEGIN(1);
	
	for (i = 0; i < identified_file_count; i++) {
		uint64_t *one_fid = malloc(sizeof(uint64_t));
		*one_fid = identified_files[i].fid;
		g_hash_table_insert(rewrite_files, &identified_files[i].fid, one_fid);
	}

	for (i = 0; i < migrated_file_count; i++) {
		uint64_t *one_fid = malloc(sizeof(uint64_t));
		*one_fid = migrated_files[i].fid;
		g_hash_table_insert(rewrite_files, &migrated_files[i].fid, one_fid);
	}

	TIMER_END(1, time_1);
	printf("construct rewrite(identified/migriated) files cost:%lf(s)\n", time_1);

	uint64_t start = 0;
	uint64_t total_fps = 0;
	double time_2 = 0;
	TIMER_DECLARE(2);
	TIMER_BEGIN(2);
	for(i = 0; i < file_count; i++) {
	    uint64_t fid = files[i].fid; 
		total_fps += files[i].chunknum;
		if (NULL != g_hash_table_lookup(rewrite_files, &fid)) 
			continue;
	
	    struct remained_file_info *one_file = (struct remained_file_info *)malloc(sizeof(struct remained_file_info));
		one_file->filesize = files[i].size;
	    one_file->fid = fid;
	    one_file->chunknum = files[i].chunknum;
	    one_file->fps = (fingerprint *)malloc(sizeof(fingerprint) * one_file->chunknum);
	    one_file->fps_cid = (uint64_t *)malloc(sizeof(uint64_t) * one_file->chunknum);
	    one_file->fps_sizes = (int32_t *)malloc(sizeof(int32_t) * one_file->chunknum);

		start = total_fps - files[i].chunknum;
	    uint32_t i = 0;
	    for (i = 0; i < one_file->chunknum; i++) {
			memcpy(&one_file->fps[i], &s1_ord[start + i].fp,  sizeof(fingerprint));	
			one_file->fps_cid[i] = s1_ord[start + i].cid;
			one_file->fps_sizes[i] = s1_ord[start + i].size;
	    } 

	    remained_file_count++;
	    sync_queue_push(remained_files_queue, one_file);
	     
	}
	TIMER_END(2, time_2);
	printf("find remained files cost:%lf(s)\n", time_2);

	printf("remained %lu files\n", remained_file_count);
	sync_queue_term(remained_files_queue);

	g_hash_table_destroy(rewrite_files);
}

void find_dedup_chunks(GHashTable *unique_chunks, struct identified_file_info *identified_files, uint64_t identified_file_count, struct migrated_file_info *migrated_files, uint64_t migrated_file_count)
{
	uint64_t g1_dedup_size = 0;
	uint64_t g1_dedup_chunk = 0;
	int i = 0;
	int j = 0;
	for (i = 0; i < identified_file_count; i++) {
		struct identified_file_info *one_file = identified_files + i;
		for (j = 0; j < one_file->num; j++)	
		{
			struct chunk *cuk = g_hash_table_lookup(unique_chunks, &one_file->fps[j]);
			if (NULL == cuk) {
				printf(BACKGROUND_COLOR_RED"can't find identified files's chunk in curent group"COLOR_NONE"\n");
				exit(-1);
			}
			cuk->ref_count--;
			if (0 == cuk->ref_count) {
				g1_dedup_size += one_file->sizes[j];
				g_hash_table_remove(unique_chunks, &one_file->fps[j]);
				g1_dedup_chunk++;
			}
		}
	}	

	for (i = 0; i < migrated_file_count; i++) {
		struct migrated_file_info *one_file = migrated_files + i;
		for (j = 0; j < one_file->total_num; j++)	
		{
			struct chunk *cuk = g_hash_table_lookup(unique_chunks, &one_file->fps[j]);
			if (NULL == cuk) {
				printf(BACKGROUND_COLOR_RED"can't find migrated rewrite files's chunk in curent group"COLOR_NONE"\n");
				exit(-1);
			}
			cuk->ref_count--;
			if (0 == cuk->ref_count) {
				g1_dedup_size += one_file->arr[j];
				g_hash_table_remove(unique_chunks, &one_file->fps[j]);
				g1_dedup_chunk++;
			}
		}
	}	

	printf(FONT_COLOR_RED"g1 can dedup %lu sizes data\n"COLOR_NONE, g1_dedup_size);
	printf(FONT_COLOR_RED"g1 can dedup %lu chunk\n"COLOR_NONE, g1_dedup_chunk);
}

static void free_chunk(void *agrv)
{
	return ;
}

void intersection(const char *path1, const char *path2)
{

	double time_1 = 0, time_2;

	struct fp_info *s1, *s2;
	struct file_info *file1, *file2;
	int64_t s1_count = 0, s2_count = 0;
	int64_t file1_count = 0, file2_count = 0;
	int64_t empty1_count = 0, empty2_count = 0;
	int64_t i, j;

	char recipe_path1[128] = {0};
	char recipe_path2[128] = {0};
	sprintf(recipe_path1, "%s/recipes/", path1);
	sprintf(recipe_path2, "%s/recipes/", path2);

	read_recipe(recipe_path1, &s1, &s1_count, &file1, &file1_count, &empty1_count);
	printf("g1 have %ld files and %ld fps\n", file1_count, s1_count);
	uint64_t g1_data_size = 0;
	uint64_t g1_unique_chunk_count = 0;
	g1_unique_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);
	for (i = 0; i < s1_count; i++) {
		struct chunk *ruc;	
		ruc = g_hash_table_lookup(g1_unique_chunks, &s1[i].fp);
		if ( ruc ) {
			ruc->ref_count++;
			continue;
		}
		ruc = (struct chunk *)malloc(sizeof(struct chunk));
		memset(ruc, 0, sizeof(struct chunk));
		memcpy(&ruc->fp, &s1[i].fp, sizeof(fingerprint));
		ruc->size = s1[i].size;
		ruc->id = s1[i].cid;
		ruc->ref_count = 1;
		g_hash_table_insert(g1_unique_chunks, &ruc->fp, ruc);	
		g1_data_size += ruc->size;
		g1_unique_chunk_count++;
	}
	printf(FONT_COLOR_RED"g1 unique chunk total %lu\n"COLOR_NONE, g1_unique_chunk_count);
	printf(FONT_COLOR_RED"g1 unique chunk total size %lu\n"COLOR_NONE, g1_data_size);

	uint64_t g2_data_size = 0;
	uint64_t g2_unique_chunk_count = 0;
	read_recipe(recipe_path2, &s2, &s2_count, &file2, &file2_count, &empty2_count);
	printf("g2 have %ld files and %ld fps\n", file2_count, s2_count);
	g2_unique_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);
	for (i = 0; i < s2_count; i++) {
		struct chunk *ruc;	
		ruc = g_hash_table_lookup(g2_unique_chunks, &s2[i].fp);
		if ( ruc ) {
			ruc->ref_count++;
			continue;
		}
		ruc = (struct chunk *)malloc(sizeof(struct chunk));
		memset(ruc, 0, sizeof(struct chunk));
		memcpy(&ruc->fp, &s2[i].fp, sizeof(fingerprint));
		ruc->size = s2[i].size;
		ruc->id = s2[i].cid;
		ruc->ref_count = 1;
		g_hash_table_insert(g2_unique_chunks, &ruc->fp, ruc);	
		g2_data_size += ruc->size;
		g2_unique_chunk_count++;
	}
	printf(FONT_COLOR_RED"g2 unique chunk total %lu\n"COLOR_NONE, g2_unique_chunk_count);
	printf(FONT_COLOR_RED"g2 unique chunk total size %lu\n"COLOR_NONE, g2_data_size);
}


void *write_migrated_file_temp_thread(void *arg) {
    	char temp_migrated_file_path[128];
	char pool_path[128];
	sprintf(temp_migrated_file_path, "%s/similar_file", g1_temp_path);
	sprintf(pool_path, "%s/%s", g1_path, "container.pool");

	FILE *pool_fp = fopen(pool_path, "r");
	if (NULL ==  pool_fp) {
		printf("fopen %s failed\n", pool_path);
	}
	double time_1 = 0;
	TIMER_DECLARE(1);

	uint64_t migrated_file_size = 0, migrated_file_migrated_size = 0;
    
    uint64_t migrated_file_count = 0;
    struct migrated_file_info *file;
    FILE *filep = NULL;
    while ((file = sync_queue_pop(write_migrated_file_temp_queue))) {
		if (NULL == filep) {
			TIMER_BEGIN(1);
	        filep = fopen(temp_migrated_file_path, "w+");
	       	if (NULL == filep) {
		       	printf("fopen %s failed\n", temp_migrated_file_path);
		       	break;
	       	}
	       	fwrite(&migrated_file_count, sizeof(uint64_t), 1, filep);
	    }
	    fwrite(file, sizeof(struct identified_file_info), 1, filep); 
	   	uint64_t i = 0;
	    for (i = 0; i < file->total_num; i++)
	       	fwrite(&file->fps[i], sizeof(fingerprint), 1, filep); 

		// arr for state
		for (i = 0; i < file->total_num; i++)
			fwrite(&file->arr[i], sizeof(uint64_t), 1, filep);		
		for (i = 0; i < file->total_num; i++)
			fwrite(&file->arr[file->total_num + i], sizeof(uint64_t), 1, filep);		

		migrated_file_size += file->filesize;
		// write chunk data
		char *data = NULL;
		int32_t chunk_size;
		for (i = 0; i < file->total_num; i++) {
			if (file->arr[i + file->total_num] != 1) {
				chunk_size = file->arr[i];
				migrated_file_migrated_size += file->arr[i];
			}
		}
		free(file->fps);
		free(file->arr);
		free(file->fp_cids);

	    migrated_file_count++;
	}
	TIMER_END(1, time_1);

	printf("migrated files size:%lu, missing size:%lu\n", migrated_file_size, migrated_file_migrated_size);
	printf("write migrated files to temp migrated temp files cost:%lf(s)\n", time_1);
			

	if (NULL != filep) {
		fseek(filep, 0,SEEK_SET);
		fwrite(&migrated_file_count, sizeof(uint64_t), 1, filep);
		fclose(filep);
	}
	if (NULL != pool_fp)
		fclose(pool_fp);

}

void * write_identified_file_to_temp_thread(void *arg)
{

    char temp_identified_file_path[128];
    sprintf(temp_identified_file_path, "%s/identified_file", g1_temp_path);

	double time_1 = 0;
	TIMER_DECLARE(1);

	uint64_t identified_file_size = 0;
    
    uint64_t identified_file_count = 0;
    struct identified_file_info *file;
    FILE *filep = NULL;
	uint64_t count = 0;
    while ((file = sync_queue_pop(write_identified_file_temp_queue))) {
		if (NULL == filep) {
			TIMER_BEGIN(1);
	    	filep = fopen(temp_identified_file_path, "w+");
	    	if (NULL == filep) {
				printf("fopen %s failed\n", temp_identified_file_path);
				break;
	    	}

	    	fwrite(&identified_file_count, sizeof(uint64_t), 1, filep);

		}
		//printf("write file:%lu , chunk:%lu to temp file\n", file->fid, file->num);
		
		fwrite(file, sizeof(struct identified_file_info), 1, filep); 
		uint64_t i = 0;
		for (i = 0; i < file->num; i++)
	    	fwrite(&file->fps[i], sizeof(fingerprint), 1, filep); 
		identified_file_count++;


		identified_file_size += file->filesize;
		count++;

		free(file->fps);
		free(file->sizes);

    }
	printf("write %lu identified files\n", count);
	printf("%s have identified files size:%lu\n", g1_temp_path, identified_file_size);

    // if have identified file
    if (NULL != filep) {
		fseek(filep, 0,SEEK_SET);
		fwrite(&identified_file_count, sizeof(uint64_t), 1, filep);
		fclose(filep);
		TIMER_END(1, time_1);
		printf("write identified file to identified temp file cost:%lf(s)\n", time_1);
    }
    else {
		printf("no identified files\n");
    }
    		
    return NULL;
}

void *read_remained_files_data_thread(void *arg) {

    struct remained_file_info *one_file;
    char new_meta_path[128];
    char new_record_path[128];

	sprintf(new_meta_path, "%s/%s", g1_path, "new.meta");
	sprintf(new_record_path, "%s/%s", g1_path, "new.recipe");

    FILE *new_metadata_fp = NULL;
    static int metabufsize = 64*1024;
    char *metabuf = malloc(metabufsize);
    int32_t metabufoff = 0;
    uint64_t recipe_offset = 0;
    int one_chunk_size = sizeof(fingerprint) + sizeof(containerid) + sizeof(int32_t);

    FILE *new_record_fp = NULL;
    static int recordbufsize = 64*1024;
    int32_t recordbufoff = 0;
    char *recordbuf = malloc(recordbufsize);
    //recipe_offset = one_chunk_size;


    uint64_t containerid = 0;

    FILE *old_pool_fp = NULL;

    int32_t bv_num = 0;
    int deleted = 0;
    int64_t number_of_files = 0;
    int64_t number_of_chunks = 0;

	uint64_t remained_files_size = 0;

	double time_1 = 0;
	TIMER_DECLARE(1);

	uint64_t remained_unique_data_size = 0;
	GHashTable *recently_unique_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);

    char *data = NULL;
	while ((one_file = sync_queue_pop(remained_files_queue))) {
		if (NULL == new_metadata_fp) {
			TIMER_BEGIN(1);
    		new_metadata_fp = fopen(new_meta_path, "w+");
    		if (NULL == new_metadata_fp) {
				printf("fopen %s failed\n", new_meta_path);
    		}
    		new_record_fp = fopen(new_record_path, "w+");
    		if (NULL == new_record_fp) {
				printf("fopen %s failed\n", new_record_path);
    		}
			memcpy(metabuf + metabufoff, &bv_num, sizeof(bv_num));
    		metabufoff += sizeof(bv_num);
    		memcpy(metabuf + metabufoff, &deleted, sizeof(deleted));
   			metabufoff += sizeof(deleted);
   			memcpy(metabuf + metabufoff, &number_of_files, sizeof(number_of_files));
   			metabufoff += sizeof(number_of_files);
   			memcpy(metabuf + metabufoff, &number_of_chunks, sizeof(number_of_chunks));
   			metabufoff += sizeof(number_of_chunks);

    		int32_t path_len = strlen(base_path);
    		memcpy(metabuf + metabufoff, &path_len, sizeof(int32_t));
   			metabufoff += sizeof(int32_t);

    		memcpy(metabuf + metabufoff, base_path, path_len);
    		metabufoff += path_len;
		}
	
		number_of_files++;
		uint64_t i = 0;
	
    	memcpy(metabuf + metabufoff, &(one_file->fid), sizeof(one_file->fid));
    	metabufoff += sizeof(one_file->fid);
    	memcpy(metabuf + metabufoff, &recipe_offset, sizeof(recipe_offset));
    	metabufoff += sizeof(recipe_offset);

    	memcpy(metabuf + metabufoff, &one_file->chunknum, sizeof(one_file->chunknum));
    	metabufoff += sizeof(one_file->chunknum);
    	memcpy(metabuf + metabufoff, &one_file->filesize, sizeof(one_file->filesize));
    	metabufoff += sizeof(one_file->filesize);

		if (sizeof(one_file->fid) + sizeof(recipe_offset) + sizeof(one_file->chunknum) + sizeof(one_file->filesize) > metabufsize - metabufoff) {
	    	fwrite(metabuf, metabufoff, 1, new_metadata_fp);
	    	metabufoff = 0;
		}

		recipe_offset += (one_file->chunknum) * one_chunk_size;
	
		number_of_chunks += one_file->chunknum;
		int32_t chunk_size ;
		for (i = 0; i < one_file->chunknum; i++) {
			chunk_size = one_file->fps_sizes[i];
			struct chunk* ruc = g_hash_table_lookup(recently_unique_chunks, &(one_file->fps[i]));
			if (NULL == ruc) {
                ruc = (struct chunk *)malloc(sizeof(struct chunk));
                ruc->size = chunk_size;
                ruc->id = container_count - 1;
                ruc->data = NULL;
				ruc->ref_count = 1;
				memcpy(&ruc->fp, &one_file->fps[i], sizeof(fingerprint));
				g_hash_table_insert(recently_unique_chunks, &(ruc->fp), ruc);
				remained_unique_data_size += chunk_size;
			}
			else 
				ruc->ref_count++;
	    
	    	if(recordbufoff + sizeof(fingerprint) + sizeof(containerid) + sizeof(chunk_size) > recordbufsize) {
				fwrite(recordbuf, recordbufoff, 1, new_record_fp);
				recordbufoff = 0;
	   		}		

	    	memcpy(recordbuf + recordbufoff, one_file->fps[i], sizeof(fingerprint)); 
	    	recordbufoff += sizeof(fingerprint);
	    	memcpy(recordbuf + recordbufoff, &ruc->id, sizeof(containerid)); 
	    	recordbufoff += sizeof(containerid);
	    	memcpy(recordbuf + recordbufoff, &chunk_size, sizeof(chunk_size)); 
	    	recordbufoff += sizeof(chunk_size);
		}
		if (NULL != one_file->fps)
			free(one_file->fps);
		if (NULL != one_file->fps_cid)
			free(one_file->fps_cid);
		if (NULL != one_file->fps_sizes)
			free(one_file->fps_sizes);
		free(one_file);
		one_file = NULL;
	}

    printf(FONT_COLOR_RED"%s remained %lu files\n"COLOR_NONE, g1_path, number_of_files);
    printf(FONT_COLOR_RED"%s remained %lu unique data size\n"COLOR_NONE, g1_path, remained_unique_data_size);
    //display_hash_table(recently_unique_chunks);
    if (0 == number_of_files)
		goto out;

    if( recordbufoff ) {
		fwrite(recordbuf, recordbufoff, 1, new_record_fp);
    	recordbufoff = 0;
    }
    if( metabufoff ) {
        fwrite(metabuf, metabufoff, 1, new_metadata_fp);
        metabufoff = 0;
    }

    fseek(new_metadata_fp, 0, SEEK_SET);
    fwrite(&bv_num, sizeof(bv_num), 1, new_metadata_fp);
    fwrite(&deleted, sizeof(deleted), 1, new_metadata_fp);
    fwrite(&number_of_files, sizeof(number_of_files), 1, new_metadata_fp);
    fwrite(&number_of_chunks, sizeof(number_of_chunks), 1, new_metadata_fp);

    fclose(new_metadata_fp);
    fclose(new_record_fp);

	TIMER_END(1, time_1);
	printf("rewrite remianed files cost:%lf(s)\n", time_1);

	char ghash_file[128] = {0};
	sprintf(ghash_file, "%s/ghash_file", g1_path);
    storage_hash_table(recently_unique_chunks, ghash_file);
    g_hash_table_destroy(recently_unique_chunks);

    free(recordbuf);
    free(metabuf);

out:

    return NULL;
}

int main(int argc, char *argv[])
{

	double time_1;
	if (3 != argc) {
		printf("usage: ./decold g1 g2\n");
		return -1;
	}
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
    
	strcpy(g1_path, argv[1]);
	strcpy(g2_path, argv[2]);
	
	strcpy(g1_temp_path, g1_path);
	strcpy(g2_temp_path, g2_path);
	
	printf("g1=%s g2=%s\n", g1_path, g2_path);	
	
    
	intersection(g1_path, g2_path);
	TIMER_END(1, time_1);

	return 0;
}
