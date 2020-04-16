#include "recipe.h"
#include "cal.h"
#include "common.h"
#include "queue.h"
#include "sync_queue.h"
#include "decold.h"
#include "containerstore.h"
#include "serial.h"

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

GHashTable *need_migrated_chunks;
GHashTable *remained_chunks;


double time_rewrite_remained_files = 0;
TIMER_DECLARE(10);

void free_chunk(struct chunk* ck) {
    if (ck->data) {
	free(ck->data);
	ck->data = NULL;
    }
    free(ck);
}

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
	int left = 0, right = 0;
	int i = 0;

	TIMER_BEGIN(10);

	GHashTable *rewrite_files = g_hash_table_new_full(g_int64_hash, g_fid_equal, NULL, free_fid);
	
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

	uint64_t start = 0;
	uint64_t total_fps = 0;
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
			memcpy(one_file->fps[i], s1_ord[start + i].fp,  sizeof(fingerprint));	
			one_file->fps_cid[i] = s1_ord[start + i].cid;
			one_file->fps_sizes[i] = s1_ord[start + i].size;
	    } 

	    remained_file_count++;
	    sync_queue_push(remained_files_queue, one_file);
	     
	}

	printf("remained %lu files\n", remained_file_count);
	sync_queue_term(remained_files_queue);

	g_hash_table_destroy(rewrite_files);
}

uint64_t  find_dedup_chunks(GHashTable *unique_chunks, struct identified_file_info *identified_files, uint64_t identified_file_count, struct migrated_file_info *migrated_files, uint64_t migrated_file_count)
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
	return g1_dedup_size;
}

void rewrite_container(char *path1)
{
	char migrated_temp_path[128] = {0};
	FILE *migrated_temp_fp = NULL;
	sprintf(migrated_temp_path, "%s/similar_file_data", path1);
	migrated_temp_fp = fopen(migrated_temp_path, "w");
	if (NULL == migrated_temp_fp) {
		printf("fopen %s failed\n", migrated_temp_path);
		exit(-1);
	}

	char pool_path[128] = {0};
	sprintf(pool_path, "%s/container.pool", path1);
	
	char new_pool_path[128] = {0};
	sprintf(new_pool_path, "%s/new_container.pool", path1);
	
	FILE *old_pool_fp = fopen(pool_path, "r");
    if (NULL == old_pool_fp) {
        printf("fopen %s failed\n", pool_path);
    }

	init_container_store(new_pool_path, "w");

	uint64_t old_container_count = 0;
	fread(&old_container_count, sizeof(old_container_count), 1, old_pool_fp);		

	remained_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);

	struct chunk *mc = NULL;
	uint64_t remained_unique_chunks = 0;
	uint64_t remained_unique_chunks_size = 0;
	uint64_t migrated_unique_chunks = 0;
	uint64_t migrated_unique_chunks_size = 0;

	printf("old container pool contain %lu container\n", old_container_count);
	uint64_t i = 0;
	for ( i = 0; i < old_container_count; i++) {
    	struct container *c = (struct container*) malloc(sizeof(struct container));
    	c->meta.chunk_num = 0;
    	c->meta.data_size = 0;
    	c->meta.id = i;

    	unsigned char *cur = 0;

    	c->data = malloc(CONTAINER_SIZE);

    	if (-1 == fseek(old_pool_fp,  i* CONTAINER_SIZE + 8, SEEK_SET)) {
        	printf("fseek failed, offset:%d\n",  i * CONTAINER_SIZE + 8);
    	}

    	int32_t ret = fread(c->data, CONTAINER_SIZE, 1, old_pool_fp);
    	if (ret != 1) {
        	printf("fread container failed:%d != %d\n", ret, 1);
    	}
    	cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];

    	unser_declare;
    	unser_begin(cur, CONTAINER_META_SIZE);

    	unser_int64(c->meta.id);
    	unser_int32(c->meta.chunk_num);
    	unser_int32(c->meta.data_size);

		int j = 0;
	    for (j = 0; j < c->meta.chunk_num; j++)
    	{
       		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
        	unser_bytes(&me->fp, sizeof(fingerprint));
        	unser_bytes(&me->len, sizeof(int32_t));
        	unser_bytes(&me->off, sizeof(int32_t));

			if ( g_hash_table_lookup(g1_unique_chunks, &me->fp) ) {
				struct chunk *ck = (struct chunk *)malloc(sizeof(struct chunk));
				ck->data = malloc(me->len);
				memcpy(ck->data, c->data + me->off, me->len);
				ck->size = me->len;
				memcpy(&ck->fp, &me->fp, sizeof(fingerprint));


				if (storage_buffer.container_buffer == NULL) {
                    storage_buffer.container_buffer = create_container();
                    storage_buffer.chunks = g_sequence_new(free_chunk);
				}
                if (container_overflow(storage_buffer.container_buffer, ck->size))
                {
                    write_container_async(storage_buffer.container_buffer);
                    storage_buffer.container_buffer = create_container();
                    storage_buffer.chunks = g_sequence_new(free_chunk);
                }
	
				ck->id = container_count - 1;
				g_hash_table_insert(remained_chunks, &ck->fp, ck);

				add_chunk_to_container(storage_buffer.container_buffer, ck);

				remained_unique_chunks++;
				remained_unique_chunks_size += me->len;
			}
			if ( (mc = g_hash_table_lookup(need_migrated_chunks, &me->fp)) != NULL ) {
				long cur_off = ftell(migrated_temp_fp);
				if (-1 == cur_off) {
					printf("ftell failed, errcode:%d\n", errno);
					exit(-1);	
				}
				mc->id =(uint64_t) cur_off;
				if (1 != fwrite(c->data + me->off, me->len, 1, migrated_temp_fp)) {
					printf("write migrated chunks to file failed, len:%d in file offset:%ld\n", me->len, mc->id);
					exit(-1);	
				}

				migrated_unique_chunks++;
				migrated_unique_chunks_size += me->len;
			}

    	}

    	if (!c->meta.map)
        	VERBOSE("ASSERT, META.MAP == NULL!\n");

		unser_end(cur, CONTAINER_META_SIZE);


		free(c->data);
		free(c);
	}

	write_container_async(storage_buffer.container_buffer);
	close_container_store();


	fclose(migrated_temp_fp);
	fclose(old_pool_fp);


	printf("really remained unique chunks count:%lu\n", remained_unique_chunks);
	printf("really remained unique chunks size:%lu\n", remained_unique_chunks_size);
	printf("really migrated chunk count:%lu\n", migrated_unique_chunks);
	printf("really migrated chunk size:%lu\n", migrated_unique_chunks_size);

}

void intersection(const char *path1, const char *path2)
{

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

	double time_1;
    TIMER_DECLARE(1);
    TIMER_BEGIN(1);
    uint64_t g1_data_size = 0;
    uint64_t g1_unique_chunk_count = 0;
    char g1_ghash_file[128] = {0};
    sprintf(g1_ghash_file, "%s/ghash_file", path1);
    if (0 != access(g1_ghash_file, F_OK)) {
        printf("g1 no hash table ,now construct it\n");
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
    } else {
        g1_unique_chunks = load_hash_table(g1_ghash_file, &g1_data_size);
    }
    printf("g1 hash table size: %lu\n", g_hash_table_size(g1_unique_chunks));
    uint64_t g1_fp_count  = get_fp_count_from_hash(g1_unique_chunks);
    printf("get fp count:%lu from hash\n", g1_fp_count);

    uint64_t g2_data_size = 0;
    uint64_t g2_unique_chunk_count = 0;
    char g2_ghash_file[128] = {0};
    sprintf(g2_ghash_file, "%s/ghash_file", path2);
    if (0 != access(g2_ghash_file, F_OK)) {
        printf("g1 no hash table ,now construct it\n");
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
        storage_hash_table(g2_unique_chunks, g2_ghash_file);
    } else {
        g2_unique_chunks = load_hash_table(g2_ghash_file, NULL);
    }
    printf("g2 hash table size: %lu\n", g_hash_table_size(g2_unique_chunks));
    TIMER_END(1, time_1);
    printf("construct hash table cost:%lf(s)\n", time_1);


    struct identified_file_info *identified_file1;
    int64_t identified_file1_count = 0;

    struct migrated_file_info *m1;
    int64_t m1_count = 0;

    int64_t mig1_count[8]={0,0,0,0,0,0,0,0}; //60,65,70,75,80,85,90,95%

	double time_2;
    TIMER_DECLARE(2);
    TIMER_BEGIN(2);
    file_find(g2_unique_chunks, file1, file1_count, s1, s1_count, &identified_file1, &identified_file1_count, &m1, &m1_count, mig1_count);
    TIMER_END(2, time_2);
    printf("find identified/migriated files cost:%lf(s)\n", time_2);

    printf("simility 0.7:%ld 0.75:%ld 0.80:%ld 0.85:%ld 0.90:%ld 0.95:%ld\n", mig1_count[2], mig1_count[3], mig1_count[4], mig1_count[5], mig1_count[6], mig1_count[7]);

    printf("%s total file count:%ld fingerprint count:%ld identified file count:%ld similar file count:%ld\n", g1_path, file1_count, s1_count, identified_file1_count, m1_count);

    double time_3 = 0;
    TIMER_DECLARE(3);
    TIMER_BEGIN(3);
    uint64_t g1_dedup_size = find_dedup_chunks(g1_unique_chunks, identified_file1, identified_file1_count, m1, m1_count);
    TIMER_END(3, time_3);
    printf("find dedup chunk cost:%lf(s)\n", time_3);

    printf("after delete i/m files, hash table size:%lu\n", g_hash_table_size(g1_unique_chunks));

	printf("will remain %lu unique data, need %d conatiner\n", g1_data_size - g1_dedup_size, (g1_data_size - g1_dedup_size)/CONTAINER_SIZE);

    push_identified_files(identified_file1, identified_file1_count, write_identified_file_temp_queue);

	
	uint64_t unique_migrated_chunk_count = 0, unique_migrated_chunk_size = 0;
	need_migrated_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);
	for (i = 0; i < m1_count; i++) {
		struct migrated_file_info *file = m1 + i;
		for (j = 0; j < file->total_num; j++) {
			if (file->arr[j + file->total_num] != 1) {
				if (NULL == g_hash_table_lookup(need_migrated_chunks, &file->fps[j])) {
					struct chunk * ck = malloc(sizeof(struct chunk));
					ck->data = NULL;
					ck->id = -1;
					memcpy(&ck->fp, &file->fps[j], sizeof(fingerprint));
					g_hash_table_insert(need_migrated_chunks, &ck->fp, ck);
					unique_migrated_chunk_count++;
					unique_migrated_chunk_size += file->arr[j];
				}
			}
		}
	}
	printf("unique migrated chunk count :%lu\n", unique_migrated_chunk_count);
	printf("unique migrated chunk size :%lu\n", unique_migrated_chunk_size);


	rewrite_container(path1);

    push_migriated_files(m1, m1_count, write_migrated_file_temp_queue);
    pthread_join(tid5, NULL);


    if (identified_file1_count || m1_count) {

        update_remained_files(0, file1, file1_count, s1, s1_count, identified_file1, identified_file1_count, m1, m1_count);
    } else {
        printf("remained files don't need to be updated\n");
        sync_queue_term(remained_files_queue);
    }

    pthread_join(tid1, NULL);
    pthread_join(tid3, NULL);


    free(file1);

    free(s1);

	free(identified_file1);
	free(m1);
}


void *write_migrated_file_temp_thread(void *arg) {
   	char temp_migrated_file_path[128];
	sprintf(temp_migrated_file_path, "%s/similar_file", g1_temp_path);

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

	   	uint64_t i = 0;

	    fwrite(file, sizeof(struct migrated_file_info), 1, filep); 
	    for (i = 0; i < file->total_num; i++)
	       	fwrite(&file->fps[i], sizeof(fingerprint), 1, filep); 

		file->migrated_chunk_offset =(uint64_t *) malloc(sizeof(uint64_t) * file->total_num);
		struct chunk *ck = NULL;
		for (i = 0; i < file->total_num; i++) {
			if (file->arr[i + file->total_num] != 1) {
				ck = g_hash_table_lookup(need_migrated_chunks, &file->fps[i]);
				if ( ck  != NULL) {
					file->migrated_chunk_offset[i] = ck->id;	
				} else  {
					printf("can't find ck in need_migrated_chunks\n");
					exit(-1);
				}
			}
		}
		for (i = 0; i < file->total_num; i++)
			fwrite(&file->migrated_chunk_offset[i], sizeof(uint64_t), 1, filep);		

		// arr for state
		for (i = 0; i < file->total_num; i++)
			fwrite(&file->arr[i], sizeof(uint64_t), 1, filep);		
		for (i = 0; i < file->total_num; i++)
			fwrite(&file->arr[file->total_num + i], sizeof(uint64_t), 1, filep);		

		migrated_file_size += file->filesize;
		free(file->fps);
		free(file->arr);
		free(file->fp_cids);
		free(file->migrated_chunk_offset);

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
    char pool_path[128];
    char new_meta_path[128];
    char new_record_path[128];

	sprintf(pool_path, "%s/%s", g1_path, "container.pool");
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

    GHashTable *recently_unique_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);

    uint64_t containerid = 0;

    FILE *old_pool_fp = NULL;

    int32_t bv_num = 0;
    int deleted = 0;
    int64_t number_of_files = 0;
    int64_t number_of_chunks = 0;

	uint64_t remained_files_size = 0;


    char *data = NULL;
	while ((one_file = sync_queue_pop(remained_files_queue))) {
		if (NULL == new_metadata_fp) {
    		new_metadata_fp = fopen(new_meta_path, "w+");
    		if (NULL == new_metadata_fp) {
				printf("fopen %s failed\n", new_meta_path);
    		}
    		new_record_fp = fopen(new_record_path, "w+");
    		if (NULL == new_record_fp) {
				printf("fopen %s failed\n", new_record_path);
    		}
    		old_pool_fp = fopen(pool_path, "r");
    		if (NULL == old_pool_fp) {
				printf("fopen %s failed\n", pool_path);
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
		int32_t chunk_size;
		for (i = 0; i < one_file->chunknum; i++) {
	    	struct chunk* ruc = g_hash_table_lookup(remained_chunks, &one_file->fps[i]);
	    	if (NULL != ruc) {
				chunk_size = ruc->size;

	    		if(recordbufoff + sizeof(fingerprint) + sizeof(containerid) + sizeof(chunk_size) > recordbufsize) {
					fwrite(recordbuf, recordbufoff, 1, new_record_fp);
					recordbufoff = 0;
	    		}		

	    		struct fp_data * one_data = (struct fp_data *)malloc(sizeof(struct fp_data));			
	    		one_data->data = data;	
	    		memcpy(recordbuf + recordbufoff, one_file->fps[i], sizeof(fingerprint)); 
	    		recordbufoff += sizeof(fingerprint);
	    		memcpy(recordbuf + recordbufoff, &ruc->id, sizeof(containerid)); 
	    		recordbufoff += sizeof(containerid);
	    		memcpy(recordbuf + recordbufoff, &chunk_size, sizeof(chunk_size)); 
	    		recordbufoff += sizeof(chunk_size);
			} else {
				printf("remained file's chunk can't finded in remaine_chunks\n");
				exit(-1);
			}
		}
		if (NULL != one_file->fps)
			free(one_file->fps);
		if (NULL != one_file->fps_cid)
			free(one_file->fps_cid);
		free(one_file);
		one_file = NULL;
		
	}

    printf(FONT_COLOR_RED"%s remained %lu files, remained_size:%lu\n"COLOR_NONE, g1_path, number_of_files, remained_files_size);
    //display_hash_table(recently_unique_chunks);
    //
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

    fclose(old_pool_fp);
    fclose(new_metadata_fp);
    fclose(new_record_fp);

	TIMER_END(10, time_rewrite_remained_files);
	printf("rewrite remianed files cost:%lf(s)\n", time_rewrite_remained_files);

	char ghash_file[128] = {0};
	sprintf(ghash_file, "%s/ghash_file", g1_path);
    storage_hash_table(remained_chunks, ghash_file);
    g_hash_table_destroy(remained_chunks);
    //storage_hash_table(g1_unique_chunks, ghash_file);
    //g_hash_table_destroy(g1_unique_chunks);

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
	
	write_identified_file_temp_queue = sync_queue_new(100);	
	pthread_create(&tid1, NULL, write_identified_file_to_temp_thread, NULL);

	remained_files_queue = sync_queue_new(100);
	pthread_create(&tid3, NULL, read_remained_files_data_thread, NULL);
	
	write_migrated_file_temp_queue = sync_queue_new(100);
	pthread_create(&tid5, NULL, write_migrated_file_temp_thread, NULL);
    
	intersection(g1_path, g2_path);
	TIMER_END(1, time_1);
	printf("total cost:%lf(s)\n", time_1);

	return 0;
}
