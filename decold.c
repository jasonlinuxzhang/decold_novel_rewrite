#include "recipe.h"
#include "cal.h"
#include "common.h"
#include "queue.h"
#include "sync_queue.h"
#include "decold.h"
#include "containerstore.h"

char *g1 = "g1", *g2 = "g2";
int target_group = 0;

extern char base_path[128];

int enable_migration = 1;
int enable_refs = 0;
int enable_topk = 0;
long int big_file = 0;
float migration_threshold = 0.5;

char g1_temp_path[128] = "/root/destor_test/g1/";
char g2_temp_path[128] = "/root/destor_test/g2/";

char g1_path[32] = "/root/destor_test/g1/";
char g2_path[32] = "/root/destor_test/g2/";

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
		printf ("push migrated file %ld\n", migrated_files->fid);
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
	for(i = 0; i < file_count; i++) {
	    uint64_t fid = files[i].fid; 
	    left = 0;
	    right = identified_file_count - 1;
	    
	    while(left <= right) {
		int middle = (left + right)/2;
		if (identified_files[middle].fid == fid) 
		    break;
		else if (identified_files[middle].fid < fid)
		    left = middle + 1;
		else if (identified_files[middle].fid > fid)
		    right = middle - 1; 
	    }

	    // the file don't need to remain
	    if (left <= right) {
		continue;
	    }

	    left = 0;
	    right = migrated_file_count - 1;
	    
	    while(left <= right) {
		int middle = (left + right)/2;
		if (migrated_files[middle].fid == fid) 
		    break;
		else if (migrated_files[middle].fid < fid) {
		    left = middle + 1;
		} else if (migrated_files[middle].fid > fid) {
		   right = middle - 1; 
		}
	    }

	    // the file don't need to remain
	    if (left <= right) {
		continue;
	    }
	
	    struct remained_file_info *one_file = (struct remained_file_info *)malloc(sizeof(struct remained_file_info));
	    one_file->fid = fid;
	    one_file->chunknum = files[i].chunknum;
	    one_file->fps = (fingerprint *)malloc(sizeof(fingerprint) * one_file->chunknum);
	    one_file->fps_cid = (uint64_t *)malloc(sizeof(uint64_t) * one_file->chunknum);

	    int start = find_first_fp_by_fid(s1_ord, s1_count, one_file->fid, low, high);	
	    uint32_t i = 0;
	    for (i = 0; i < one_file->chunknum; i++) {
		memcpy(one_file->fps[i], s1_ord[start + i].fp,  sizeof(fingerprint));	
		one_file->fps_cid[i] = s1_ord[start + i].cid;
	    } 

	    remained_file_count++;
	    sync_queue_push(remained_files_queue, one_file);
	     
	}

	myprintf("remained %lu files\n", remained_file_count);
	sync_queue_term(remained_files_queue);
}

void intersection(const char *path1, const char *path2)
{
	struct fp_info *s1, *s2;
	struct file_info *file1, *file2;
	int64_t s1_count = 0, s2_count = 0;
	int64_t file1_count = 0, file2_count = 0;
	int64_t empty1_count = 0, empty2_count = 0;
	int64_t i, j;

	read_recipe(path1, &s1, &s1_count, &file1, &file1_count, &empty1_count);
	read_recipe(path2, &s2, &s2_count, &file2, &file2_count, &empty2_count);

	struct fp_info *s1_ord = (struct fp_info *)malloc(s1_count * sizeof(struct fp_info));
	struct fp_info *s2_ord = (struct fp_info *)malloc(s2_count * sizeof(struct fp_info));
	for (i = 0; i < s1_count; i++)
	{
		s1_ord[i] = s1[i];
		memcpy(s1_ord[i].fp, s1[i].fp, sizeof(fingerprint));
		//VERBOSE("s1_order   CHUNK:fid=[%8" PRId64 "], order=%" PRId64 ", size=%" PRId64 ", container_id=%" PRId64 "\n",s1_ord[i].fid, s1_ord[i].order, s1_ord[i].size, s1_ord[i].cid);
	}
	for (i = 0; i < s2_count; i++)
	{
		s2_ord[i] = s2[i];
		memcpy(s2_ord[i].fp, s2[i].fp, sizeof(fingerprint));
	}
	
	struct file_info *file1_ord = (struct file_info *)malloc(file1_count * sizeof(struct file_info));
	struct file_info *file2_ord = (struct file_info *)malloc(file2_count * sizeof(struct file_info));
	for (i = 0; i < file1_count; i++) {
	    file1_ord[i] = file1[i];
	}
	for (i = 0; i < file2_count; i++) {
	    file2_ord[i] = file2[i];
	}

	struct fp_info *scommon1, *scommon2;
	int64_t sc1_count = 0, sc2_count = 0;
	cal_inter(s1, s1_count, s2, s2_count, &scommon1, &sc1_count, &scommon2, &sc2_count);

	struct identified_file_info *identified_file1; //*identified_file2;
	int64_t identified_file1_count = 0, identified_file2_count = 0;

	struct migrated_file_info *m1, *m2;
	int64_t m1_count = 0, m2_count = 0;

	int64_t mig1_count[8]={0,0,0,0,0,0,0,0}; //60,65,70,75,80,85,90,95%
	int64_t mig2_count[8]={0,0,0,0,0,0,0,0}; //60,65,70,75,80,85,90,95%

	file_find(file1, file1_count, scommon1, sc1_count, &identified_file1, &identified_file1_count, &m1, &m1_count, mig1_count);
//	file_find(file2, file2_count, scommon2, sc2_count, &identified_file2, &identified_file2_count, &m2, &m2_count, mig2_count);

	for (i = 0; i < m1_count; i++) {
		struct fp_info *start = s1_ord + m1[i].fp_info_start;
		for (j = 0; j < m1[i].total_num; j++) {
			if (m1[i].arr[m1[i].total_num + j] != 1) {
				memcpy(&m1[i].fps[j], &start->fp, sizeof(fingerprint));
				m1[i].fp_cids[j] = start->cid;
				m1[i].arr[j] = start->size;
				printf("chunk exist in:%lu container\n", m1[i].fp_cids[j]);
			}
			start++;
		}
	}




	myprintf("%s total file count:%ld fingerprint count:%ld identified file count:%ld similar file count:%ld\n", target_group?"g2":"g1", file1_count, sc1_count, identified_file1_count, m1_count);
	
	push_identified_files(identified_file1, identified_file1_count, write_identified_file_temp_queue);

	/*
 	*
 	*
 	* *
 	*/ 	
	push_migriated_files(m1, m1_count, write_migrated_file_temp_queue);
	pthread_join(tid5, NULL);

	/*
 	*
 	* */


	// update remianed files must be after write migrated_file, because fopen pool
	init_container_store();

	update_remained_files(0, file1_ord, file1_count, s1_ord, s1_count, identified_file1, identified_file1_count, m1, m1_count);

	pthread_join(tid1, NULL);
	pthread_join(tid3, NULL);

	free(file1);
	free(file2);
	free(file1_ord);
	free(file2_ord);

	free(s1);
	free(s2);
	free(s1_ord);
	free(s2_ord);
	for (i = 0; i < identified_file1_count; i++) {
	    free(identified_file1[i].sizes);
	    free(identified_file1[i].fps);
	}
	/*
	for (i = 0; i < identified_file2_count; i++) {
	    free(identified_file2[i].sizes);
	    free(identified_file2[i].fps);
	}
	*/
	free(identified_file1);
	//free(identified_file2);
	for (i = 0; i < m1_count; i++) {
	    free(m1[i].arr);
	    free(m1[i].fps);
	}
	/*
	for (i = 0; i < m2_count; i++) {
	    free(m2[i].arr);
	    free(m2[i].fps);
	}
	*/
	free(m1);
	//free(m2);
	free(scommon1);
	//free(scommon2);
}

void * read_from_destor_thread(void *arg)
{
    		
    return NULL;
}
/*
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

void *write_migrated_file_temp_thread(void *arg) {
    	char temp_migrated_file_path[128];
	char pool_path[128];
    	if (0 == target_group) 
    	{
		sprintf(temp_migrated_file_path, "%s/similar_file", g1_temp_path);
		sprintf(pool_path, "%s/%s", g1_path, "container.pool");
    	}
    	else
    	{
		sprintf(temp_migrated_file_path, "%s/similar_file", g2_temp_path);
		sprintf(pool_path, "%s/%s", g2_path, "container.pool");
    	}

	FILE *pool_fp = fopen(pool_path, "r");
	if (NULL ==  pool_fp) {
		printf("fopen %s failed\n", pool_path);
	}
    
    	uint64_t migrated_file_count = 0;
    	struct migrated_file_info *file;
    	FILE *filep = NULL;
    	while ((file = sync_queue_pop(write_migrated_file_temp_queue))) {
		if (NULL == filep) {
	        	filep = fopen(temp_migrated_file_path, "w+");
	        	if (NULL == filep) {
		        	printf("fopen %s failed\n", temp_migrated_file_path);
		        	break;
	        	}
	        	fwrite(&migrated_file_count, sizeof(uint64_t), 1, filep);
	    	}
	    	printf("write migrated_file file:%lu , chunk:%lu to temp file\n", file->fid, file->total_num);
	    	fwrite(file, sizeof(struct identified_file_info), 1, filep); 
	    	uint64_t i = 0;
		// fps
	    	for (i = 0; i < file->total_num; i++)
	        	fwrite(&file->fps[i], sizeof(fingerprint), 1, filep); 

		// arr for state
		for (i = 0; i < file->total_num; i++)
			fwrite(&file->arr[i], sizeof(uint64_t), 1, filep);		
		for (i = 0; i < file->total_num; i++)
			fwrite(&file->arr[file->total_num + i], sizeof(uint64_t), 1, filep);		

		// write chunk data
		char *data = NULL;
		int32_t chunk_size;
		for (i = 0; i < file->total_num; i++) {
			if (file->arr[i + file->total_num] != 1) {
				printf("try to retrieve from container:%lu\n", file->fp_cids[i]);
				chunk_size = retrieve_from_container(pool_fp, file->fp_cids[i], &data, file->fps[i]);
				if (chunk_size != file->arr[i]) {
					printf ("chunk size:%d != %d\n", chunk_size, file->arr[i]);
					assert("retrieve migrated files chunk from containerpool failed\n");
				}	
				fwrite(data, chunk_size, 1, filep);
			}
		}

	    	migrated_file_count++;
	}

	if (NULL != filep) {
		fseek(filep, 0,SEEK_SET);
		fwrite(&migrated_file_count, sizeof(uint64_t), 1, filep);
		fclose(filep);
	}
	if (NULL != pool_fp)
		fclose(pool_fp);

}
/*
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

void * write_identified_file_to_temp_thread(void *arg)
{

    char temp_identified_file_path[128];
    if (0 == target_group) 
    {
	sprintf(temp_identified_file_path, "%s/identified_file", g1_temp_path);
    }
    else
    {
	sprintf(temp_identified_file_path, "%s/identified_file", g2_temp_path);
    }

    
    uint64_t identified_file_count = 0;
    struct identified_file_info *file;
    FILE *filep = NULL;
    while ((file = sync_queue_pop(write_identified_file_temp_queue))) {
	if (NULL == filep) {
	    filep = fopen(temp_identified_file_path, "w+");
	    if (NULL == filep) {
		printf("fopen %s failed\n", temp_identified_file_path);
		break;
	    }

	    fwrite(&identified_file_count, sizeof(uint64_t), 1, filep);

	}
	printf("write file:%lu , chunk:%lu to temp file\n", file->fid, file->num);
	fwrite(file, sizeof(struct identified_file_info), 1, filep); 
	uint64_t i = 0;
	for (i = 0; i < file->num; i++)
	    fwrite(&file->fps[i], sizeof(fingerprint), 1, filep); 
	identified_file_count++;
    }

    // if have identified file
    if (NULL != filep) {
	fseek(filep, 0,SEEK_SET);
	fwrite(&identified_file_count, sizeof(uint64_t), 1, filep);
	fclose(filep);
    }
    else {
	printf("no identified files\n");
    }
    		
    return NULL;
}

void restore_migrated_files(char *temp_migrated_file_path, GHashTable *recently_unique_chunks) 
{
	FILE *filep = fopen(temp_migrated_file_path, "r");
	if (NULL == filep) {
		return;
	}
    	uint64_t migrated_file_count = 0;
    	fread(&migrated_file_count, sizeof(uint64_t), 1, filep);
    	printf("%s have %lu migrated file move to %s\n", target_group ? "g1":"g2", migrated_file_count, target_group ? "g2":"g1");
    	if (0 == migrated_file_count)
    	{
		return;
    	}


	init_container_store();

	typedef struct identified_file_info migrated_file_info2;
    	struct migrated_file_info *migrated_files = (struct migrated_file_info *)malloc(migrated_file_count * sizeof(struct migrated_file_info));
    	migrated_file_info2 *migrated_files2 = (migrated_file_info2 *)malloc(migrated_file_count * sizeof(migrated_file_info2));
	struct chunk* ruc;
	char *data;
	uint64_t i = 0;
    	i = 0;
    	while(fread(migrated_files + i, sizeof(struct migrated_file_info), 1, filep)) {
		struct migrated_file_info *p = migrated_files + i;
		p->fps = malloc(sizeof(fingerprint) * p->total_num);
		p->arr = malloc(2 * sizeof(uint64_t) * p->total_num);

		migrated_file_info2 *t = migrated_files2 + i;
		t->fid = p->fid;
		t->num = p->total_num;
		t->filesize = p->filesize;
		t->fps = malloc(sizeof(fingerprint) * t->num);
		t->sizes = malloc(sizeof(int32_t) * t->num);
		t->fp_cids = malloc(sizeof(uint64_t) * t->num);

	    	uint64_t j = 0;
		// fps
	    	for (j = 0; j < p->total_num; j++) {
	        	fread(&p->fps[i], sizeof(fingerprint), 1, filep); 
			memcpy(&t->fps[i], &p->fps[i], sizeof(fingerprint));	
		}

		// arr for state
		for (j = 0; j < p->total_num; j++) {
			fread(&p->arr[j], sizeof(uint64_t), 1, filep);		
			t->sizes[j] = p->arr[j];
		}
		for (j = 0; j < p->total_num; j++)
			fread(&p->arr[p->total_num + j], sizeof(uint64_t), 1, filep);		

		for (j = 0; j < p->total_num; j++) {
			if (p->arr[j + p->total_num] == 1) {
	    			struct chunk* ck = g_hash_table_lookup(recently_unique_chunks, &p->fps[j]);
	    			if (NULL == ck) {
					assert("can't find ck in recent hash");
	    			}
				
				t->fp_cids[j] = ck->id;
				t->sizes[j] = ck->size;
				
			} else {
				data = malloc(p->arr[j]);	
				fread(data, p->arr[j], 1, filep);		
				if (storage_buffer.container_buffer == NULL) {
					storage_buffer.container_buffer = create_container();
					storage_buffer.chunks = g_sequence_new(free_chunk);
				}
				if (container_overflow(storage_buffer.container_buffer, p->arr[j])) {
					write_container_async(storage_buffer.container_buffer);
					storage_buffer.container_buffer = create_container();
					storage_buffer.chunks = g_sequence_new(free_chunk);
				}

		                ruc = (struct chunk *)malloc(sizeof(struct chunk));
	        	        ruc->size = p->arr[j];
        	        	ruc->id = container_count - 1;
                		ruc->data = data;
				memcpy(&ruc->fp, &p->fps[j], sizeof(fingerprint));
				add_chunk_to_container(storage_buffer.container_buffer, ruc);
				g_hash_table_insert(recently_unique_chunks, &p->fps[i], ruc);
				
				t->fp_cids[j] = ruc->id;
				t->sizes[j] = ruc->size;
			}		
		}
		sync_queue_push(write_identified_file_to_destor_queue, t);
		i++;
	}
	write_container_async(storage_buffer.container_buffer);

	close_container_store();

}

void *restore_temp_thread(void *arg) {
    char temp_identified_file_path[128];
    char g_hash_file[128] = {0};
	char temp_migrated_file_path[128];
    if (0 == target_group) 
    {
	sprintf(temp_identified_file_path, "%s/identified_file", g2_temp_path);
	sprintf(temp_migrated_file_path, "%s/similar_file", g2_temp_path);
	sprintf(g_hash_file, "%s/ghash_file", g1_path);
    }
    else
    {
	sprintf(temp_identified_file_path, "%s/identified_file", g1_temp_path);
	sprintf(temp_migrated_file_path, "%s/similar_file", g1_temp_path);
	sprintf(g_hash_file, "%s/ghash_file", g2_path);
    }

    GHashTable *recently_unique_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);
    FILE *hash_filep = fopen(g_hash_file, "r");
    if (NULL == hash_filep)
    {
	printf("fopen %s failed\n", g_hash_file);
	goto out;
    }
    uint64_t item_count = 0;
    fread(&item_count, sizeof(item_count), 1, hash_filep);

    printf("%s have %lu item\n", target_group ? "g2":"g1", item_count);

    uint64_t i = 0;
    while (i < item_count) {
	fingerprint *fp = malloc(sizeof(fingerprint));
	struct chunk *ck = malloc(sizeof(struct chunk)); 
	fread(fp, sizeof(fingerprint), 1, hash_filep );
	fread(ck, sizeof(struct chunk), 1, hash_filep );
	g_hash_table_insert(recently_unique_chunks, fp, ck);
	i++;
    }
    fclose(hash_filep);

    FILE *filep = fopen(temp_identified_file_path, "r");
    if (NULL == filep)
    {
	printf("fopen %s failed, maybe no identified file\n", temp_identified_file_path);
	goto out;
    }
    uint64_t identified_file_count = 0;
    fread(&identified_file_count, sizeof(uint64_t), 1, filep);
    printf("%s have %lu identified file move to %s\n", target_group ? "g1":"g2", identified_file_count, target_group ? "g2":"g1");
    if (0 == identified_file_count)
    {
	goto out;
    }

    struct identified_file_info *identified_files = (struct identified_file_info *)malloc(identified_file_count * sizeof(struct identified_file_info));
    i = 0;
    while(fread(identified_files + i, sizeof(struct identified_file_info), 1, filep)) {
	identified_files[i].fps = (fingerprint *)malloc(sizeof(fingerprint) * identified_files[i].num);		
	identified_files[i].fp_cids = (uint64_t *)malloc(sizeof(uint64_t) * identified_files[i].num);		

	printf("read file:%lu chunk:%lu from temp file:%s\n", identified_files[i].fid, identified_files[i].num, temp_identified_file_path);	
    
	fingerprint temp_fp;
	uint64_t j = 0;
	while(j < identified_files[i].num) {
	    fread(&temp_fp, sizeof(fingerprint), 1, filep);
	    memcpy(&identified_files[i].fps[j], &temp_fp, sizeof(fingerprint));
	    j++;
	}

	j = 0;
	while(j < identified_files[i].num) {
	    struct chunk* ck = g_hash_table_lookup(recently_unique_chunks, &identified_files[i].fps[j]);
	    if (NULL == ck) {
		assert("can't find ck in recent hash");
	    }
	    identified_files[i].fp_cids[j] = ck->id;
	    identified_files[i].sizes[j] = ck->size;
	
	    char code[41] = {0};
	    hash2code(identified_files[i].fps[j], code);
	    printf("assign fp:%s size:%d to container:%lu\n", code, identified_files[i].sizes[j], identified_files[i].fp_cids[j]);
	    j++;
	}

	sync_queue_push(write_identified_file_to_destor_queue, identified_files+i);
	i++;
    }

out:
    if (NULL != filep)
        fclose(filep);

	
/**
 *
 *
 *
 */
	restore_migrated_files(temp_migrated_file_path, recently_unique_chunks);	

    sync_queue_term(write_identified_file_to_destor_queue);
}

void *write_identified_files_to_destor_thread(void *arg) {
    
    char meta_path[128] = {0};
    char recipe_path[128] = {0};
    uint64_t i;

    FILE *metadata_fp, *record_fp;
    if (0 == target_group) {
	sprintf(meta_path, "%s/bv0.meta", g1_path);	
	sprintf(recipe_path, "%s/bv0.recipe", g1_path);	
    } else {
	sprintf(meta_path, "%s/bv0.meta", g2_path);	
	sprintf(recipe_path, "%s/bv0.recipe", g2_path);	
    }

    metadata_fp = fopen(meta_path, "r+");
    
    int32_t bv_num = 0;
    int deleted = 0;
    int64_t number_of_files = 0;
    int64_t number_of_chunks = 0;
    fread(&bv_num, sizeof(bv_num), 1, metadata_fp);
    fread(&deleted, sizeof(deleted), 1, metadata_fp);
    fread(&number_of_files, sizeof(number_of_files), 1, metadata_fp);
    fread(&number_of_chunks, sizeof(number_of_chunks), 1, metadata_fp);

    static int metabufsize = 64*1024;
    char *metabuf = malloc(metabufsize);
    int32_t metabufoff = 0;
    int64_t recipe_offset = 0;

    static int recordbufsize = 64*1024;
    int32_t recordbufoff = 0;
    char *recordbuf = malloc(recordbufsize);
    int one_chunk_size = sizeof(fingerprint) + sizeof(containerid) + sizeof(int32_t);

    record_fp = fopen(recipe_path, "a");
    recipe_offset = ftell(record_fp);;

    fseek(metadata_fp, 0, SEEK_END);


    struct identified_file_info *one_file;
    while ((one_file = sync_queue_pop(write_identified_file_to_destor_queue))) {
        memcpy(metabuf + metabufoff, &(one_file->fid), sizeof(one_file->fid));
        metabufoff += sizeof(one_file->fid);
        memcpy(metabuf + metabufoff, &recipe_offset, sizeof(recipe_offset));
        metabufoff += sizeof(recipe_offset);

        memcpy(metabuf + metabufoff, &one_file->num, sizeof(one_file->num));
        metabufoff += sizeof(one_file->num);
        memcpy(metabuf + metabufoff, &one_file->filesize, sizeof(one_file->filesize));
        metabufoff += sizeof(one_file->filesize);

        if (sizeof(one_file->fid) + sizeof(recipe_offset) + sizeof(one_file->num) + sizeof(one_file->filesize) > metabufsize - metabufoff) {
            fwrite(metabuf, metabufoff, 1, metadata_fp);
            metabufoff = 0;
        }

	recipe_offset += (one_file->num) * one_chunk_size;
	number_of_chunks += one_file->num;

	for (i = 0; i < one_file->num; i++) {
	    if(recordbufoff + sizeof(fingerprint) + sizeof(containerid) + sizeof(int32_t) > recordbufsize) {
		fwrite(recordbuf, recordbufoff, 1, record_fp);
		recordbufoff = 0;
	    }		

	    struct chunk *ck = one_file->fps[i];
	    memcpy(recordbuf + recordbufoff, &one_file->fps[i], sizeof(fingerprint)); 
	    recordbufoff += sizeof(fingerprint);
	    memcpy(recordbuf + recordbufoff, &one_file->fp_cids[i], sizeof(containerid)); 
	    recordbufoff += sizeof(containerid);
	    memcpy(recordbuf + recordbufoff, &one_file->sizes[i], sizeof(one_file->sizes[i])); 
	    recordbufoff += sizeof(one_file->sizes[i]);

	    char code[41] = {0};
	    hash2code(one_file->fps[i], code);
	    printf("write identfied files fp:%s cid:%lu size:%d to %ld\n", code, one_file->fp_cids[i], one_file->sizes[i], recipe_offset - (one_file->num) * one_chunk_size);
	}
	number_of_files++;	
    }

    if (metabufoff) {
	fwrite(metabuf, metabufoff, 1, metadata_fp);
    }
    if (recordbufoff) {
	fwrite(recordbuf, recordbufoff, 1, record_fp);
    }
    
    fseek(metadata_fp, 0, SEEK_SET);
    fwrite(&bv_num, sizeof(bv_num), 1, metadata_fp);
    fwrite(&deleted, sizeof(deleted), 1, metadata_fp);
    fwrite(&number_of_files, sizeof(number_of_files), 1, metadata_fp);
    fwrite(&number_of_chunks, sizeof(number_of_chunks), 1, metadata_fp);
    
    fclose(metadata_fp);
    fclose(record_fp);
	
}

void *read_remained_files_data_thread(void *arg) {

    struct remained_file_info *one_file;
    char pool_path[128];
    char new_meta_path[128];
    char new_record_path[128];

    if (0 == target_group) { 
	sprintf(pool_path, "%s/%s", g1_path, "container.pool");
	sprintf(new_meta_path, "%s/%s", g1_path, "new.meta");
	sprintf(new_record_path, "%s/%s", g1_path, "new.recipe");
    } else { 
	sprintf(pool_path, "%s/%s", g2_path, "container.pool");
	sprintf(new_meta_path, "%s/%s", g2_path, "new.meta");
	sprintf(new_record_path, "%s/%s", g2_path, "new.recipe");
    }

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

    new_metadata_fp = fopen(new_meta_path, "w+");
    if (NULL == new_metadata_fp) {
	printf("fopen %s failed\n", new_meta_path);
    }
    new_record_fp = fopen(new_record_path, "w+");
    if (NULL == new_record_fp) {
	printf("fopen %s failed\n", new_record_path);
    }
    FILE *old_pool_fp = fopen(pool_path, "r");
    if (NULL == old_pool_fp) {
	printf("fopen %s failed\n", pool_path);
    }


    int32_t bv_num = 0;
    int deleted = 0;
    int64_t number_of_files = 0;
    int64_t number_of_chunks = 0;

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

    char *data = NULL;
    while ((one_file = sync_queue_pop(remained_files_queue))) {

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
	    struct chunk* ruc = g_hash_table_lookup(recently_unique_chunks, &one_file->fps[i]);
	    if (NULL == ruc) {
		if (storage_buffer.container_buffer == NULL) {
		    storage_buffer.container_buffer = create_container();
		    storage_buffer.chunks = g_sequence_new(free_chunk);
		}
	
		myprintf("try to retrieve container %lu\n", one_file->fps_cid[i]);
		chunk_size = retrieve_from_container(old_pool_fp, one_file->fps_cid[i], &data, one_file->fps[i]);

		if (container_overflow(storage_buffer.container_buffer, chunk_size))
		{
		    write_container_async(storage_buffer.container_buffer);    
		    storage_buffer.container_buffer = create_container();
		    storage_buffer.chunks = g_sequence_new(free_chunk);
		}

		ruc = (struct chunk *)malloc(sizeof(struct chunk));
		ruc->size = chunk_size;
		ruc->id = container_count - 1; 
		ruc->data = data;
		memcpy(&ruc->fp, &one_file->fps[i], sizeof(fingerprint));
		
		char code[40];
		hash2code(ruc->fp, code);
		myprintf("add chunk fp:%s size:%d to container:%lu\n", code, ruc->size, ruc->id);
		add_chunk_to_container(storage_buffer.container_buffer, ruc);

		g_hash_table_insert(recently_unique_chunks, &one_file->fps[i], ruc);
	    }
	    
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
	}
    }

    myprintf("%s remained %lu files\n", target_group?"g2":"g1", number_of_files);
    //display_hash_table(recently_unique_chunks);
    
    write_container_async(storage_buffer.container_buffer);    
    close_container_store();

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

    storage_hash_table(recently_unique_chunks);
    g_hash_table_destroy(recently_unique_chunks);

    free(recordbuf);
    free(metabuf);


    return NULL;
}

int main(int argc, char *argv[])
{

//	if (3 != argc) {
//		printf("usage: ./decold g1 g2\n");
//		return -1;
//	}
    
//	strcpy(g1, argv[1]);
//	strcpy(g2, argv[2]);
//	printf("g1=%s g2=%s\n", g1, g2);	
	
	char src_path[128] = {0};
	char dest_path[128] = {0};
	// handle g1
	target_group = 0;

	write_identified_file_temp_queue = sync_queue_new(100);	
	pthread_create(&tid1, NULL, write_identified_file_to_temp_thread, NULL);

	remained_files_queue = sync_queue_new(100);
	pthread_create(&tid3, NULL, read_remained_files_data_thread, NULL);
	
	write_migrated_file_temp_queue = sync_queue_new(100);
	pthread_create(&tid5, NULL, write_migrated_file_temp_thread, NULL);
    
	intersection(g1_path, g2_path);

	/*

	sprintf(src_path, "%s/new_container.pool", g1_path);
	sprintf(dest_path, "%s/container.pool", g1_path);
	rename(src_path, dest_path);	

	sprintf(src_path, "%s/new.meta", g1_path);
	sprintf(dest_path, "%s/bv0.meta", g1_path);
	rename(src_path, dest_path);	

	sprintf(src_path, "%s/new.recipe", g1_path);
	sprintf(dest_path, "%s/bv0.recipe", g1_path);
	rename(src_path, dest_path);	
	*/


	target_group = 1;
	write_identified_file_temp_queue = sync_queue_new(100);	
	pthread_create(&tid1, NULL, write_identified_file_to_temp_thread, NULL);

	remained_files_queue = sync_queue_new(100);
	pthread_create(&tid3, NULL, read_remained_files_data_thread, NULL);

	write_migrated_file_temp_queue = sync_queue_new(100);
	pthread_create(&tid5, NULL, write_migrated_file_temp_thread, NULL);
    
	intersection(g2_path, g1_path);

	/*
	sprintf(src_path, "%s/new_container.pool", g2_path);
	sprintf(dest_path, "%s/container.pool", g2_path);
	rename(src_path, dest_path);	

	sprintf(src_path, "%s/new.meta", g2_path);
	sprintf(dest_path, "%s/bv0.meta", g2_path);
	rename(src_path, dest_path);	

	sprintf(src_path, "%s/new.recipe", g2_path);
	sprintf(dest_path, "%s/bv0.recipe", g2_path);
	rename(src_path, dest_path);	
	*/

	/*
	write_identified_file_to_destor_queue = sync_queue_new(50); 
	target_group = 1;
	pthread_create(&tid1, NULL, restore_temp_thread, NULL);
	pthread_create(&tid3, NULL, write_identified_files_to_destor_thread, NULL);
	pthread_join(tid1, NULL);
	pthread_join(tid3, NULL);

	write_identified_file_to_destor_queue = sync_queue_new(50); 
	target_group = 0;
	pthread_create(&tid1, NULL, restore_temp_thread, NULL);
	pthread_create(&tid3, NULL, write_identified_files_to_destor_thread, NULL);
	pthread_join(tid1, NULL);
	pthread_join(tid3, NULL);
	*/
	return 0;
}
