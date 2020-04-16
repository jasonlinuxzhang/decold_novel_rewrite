#include "recipe.h"
#include "cal.h"
#include "common.h"
#include "queue.h"
#include "sync_queue.h"
#include "decold.h"
#include "containerstore.h"

char source_temp_path[128] = {0};
char target_path[128] = {0}; 

typedef struct identified_file_info migrated_file_info2;

SyncQueue *write_migriated_file_to_destor_queue;

SyncQueue *write_destor_queue;


pthread_t tid1;
pthread_t tid3;

pthread_t tid5;

containerid container_count;

double time_2 = 0;
TIMER_DECLARE(2);


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



void restore_migrated_files(char *temp_migrated_file_path, char *temp_migrated_data_path, GHashTable *recently_unique_chunks) 
{
	FILE *filep = fopen(temp_migrated_file_path, "r");
	if (NULL == filep) {
		printf("fopen %s failed, maybe not exist\n", temp_migrated_file_path);
		return;
	}

	FILE *data_filep = fopen(temp_migrated_data_path, "r");
	if (NULL == data_filep) {
		printf("fopen %s failed, maybe not exist\n", temp_migrated_data_path);
	}

    uint64_t migrated_file_count = 0;
    fread(&migrated_file_count, sizeof(uint64_t), 1, filep);
    printf("%s have %lu migrated file move to %s\n", source_temp_path, migrated_file_count, target_path);
    if (0 == migrated_file_count)
    {
		return;
    }

	char container_path[128] = {0};
	sprintf(container_path, "%s/container.pool", target_path);
	init_container_store(container_path, "r+");

    struct migrated_file_info *migrated_files = (struct migrated_file_info *)malloc(migrated_file_count * sizeof(struct migrated_file_info));
    migrated_file_info2 *migrated_files2 = (migrated_file_info2 *)malloc(migrated_file_count * sizeof(migrated_file_info2));
	char *data;
	uint64_t i = 0;
    i = 0;

	TIMER_BEGIN(2);

    while(fread(migrated_files + i, sizeof(struct migrated_file_info), 1, filep)) {
		struct migrated_file_info *p = migrated_files + i;
		p->fps = malloc(sizeof(fingerprint) * p->total_num);
		p->arr = malloc(2 * sizeof(uint64_t) * p->total_num);
		p->migrated_chunk_offset = malloc(sizeof(uint64_t) * p->total_num);

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
	       	fread(&p->fps[j], sizeof(fingerprint), 1, filep); 
			memcpy(&t->fps[j], &p->fps[j], sizeof(fingerprint));	
		}

		for (j = 0; j < p->total_num; j++) {
			fread(&p->migrated_chunk_offset[j], sizeof(uint64_t), 1, filep);
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
					printf(BACKGROUND_COLOR_RED"can't find ck in recent hash, fid:%lu chunk offset:%d chunk size:%d\n"COLOR_NONE, p->fid, j, p->arr[j]);
					int tt = 0;
					for (tt = 0 ; tt < p->total_num; tt++) {
						show_fingerprint(&(p->fps[tt]));
					}
					exit(-1);
	    		}
				ck->ref_count++;	
				t->fp_cids[j] = ck->id;
				t->sizes[j] = ck->size;
				
			} else {
	    		struct chunk* ruc = g_hash_table_lookup(recently_unique_chunks, &p->fps[j]);
				if (NULL == ruc) {
					if (storage_buffer.container_buffer == NULL) {
						storage_buffer.container_buffer = create_container();
						storage_buffer.chunks = g_sequence_new(free_chunk);
					}
					if (container_overflow(storage_buffer.container_buffer, p->arr[j])) {
						write_container_async(storage_buffer.container_buffer);
						storage_buffer.container_buffer = create_container();
						storage_buffer.chunks = g_sequence_new(free_chunk);
					}

					if ( 0 != fseek(data_filep, p->migrated_chunk_offset[j], SEEK_SET)) {
						printf("fseek failed, can't seek %lu offset\n", p->migrated_chunk_offset[j]);
					}
					data = malloc(p->arr[j]);	
					if (1 != fread(data, p->arr[j], 1, data_filep)) {
						printf("read error, expect read data size:%d from offset:%ld\n", p->arr[j], ftell(data_filep));
					}

		        	ruc = (struct chunk *)malloc(sizeof(struct chunk));
	            	ruc->size = p->arr[j];
      	      		ruc->id = container_count - 1;
           			ruc->data = data;
					ruc->ref_count = 1;
					memcpy(&ruc->fp, &p->fps[j], sizeof(fingerprint));
					add_chunk_to_container(storage_buffer.container_buffer, ruc);
					g_hash_table_insert(recently_unique_chunks, &p->fps[j], ruc);
				} else {
					ruc->ref_count++;
				}
				
				t->fp_cids[j] = ruc->id;
				t->sizes[j] = ruc->size;
			}		
		}
		sync_queue_push(write_migriated_file_to_destor_queue, t);
		i++;
	}
	write_container_async(storage_buffer.container_buffer);

	close_container_store();
	char ghash_file[256] = {0};
	sprintf(ghash_file, "%s/ghash_file", target_path);
	storage_hash_table(recently_unique_chunks, ghash_file);

}

void *restore_temp_thread(void *arg) {
    char temp_identified_file_path[128];
    char g_hash_file[128] = {0};
	char temp_migrated_file_path[128];
	char temp_migrated_data_path[128];
	sprintf(temp_migrated_file_path, "%s/similar_file", source_temp_path);
	sprintf(temp_migrated_data_path, "%s/similar_file_data", source_temp_path);
	sprintf(g_hash_file, "%s/ghash_file", target_path);

    GHashTable *recently_unique_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);
    FILE *hash_filep = fopen(g_hash_file, "r");
    if (NULL == hash_filep)
    {
		printf("fopen %s failed\n", g_hash_file);
		return;
    }
    uint64_t item_count = 0;
    fread(&item_count, sizeof(item_count), 1, hash_filep);

    printf("%s have %lu item\n", target_path, item_count);

	double time_2 = 0;
	TIMER_DECLARE(2);
	TIMER_BEGIN(2);
    uint64_t i = 0;
    while (i < item_count) {
		fingerprint *fp = malloc(sizeof(fingerprint));
		struct chunk *ck = malloc(sizeof(struct chunk)); 
		fread(ck, sizeof(struct chunk), 1, hash_filep );
		fread(fp, sizeof(fingerprint), 1, hash_filep );
		g_hash_table_insert(recently_unique_chunks, fp, ck);
		i++;
    }
    fclose(hash_filep);
	TIMER_END(2, time_2);
	printf("construct htable cost:%lf(s)\n", time_2);

	restore_migrated_files(temp_migrated_file_path, temp_migrated_data_path, recently_unique_chunks);	

    sync_queue_term(write_migriated_file_to_destor_queue);
}

void *write_migriated_files_to_destor_thread(void *arg) {
    
    char meta_path[128] = {0};
    char recipe_path[128] = {0};
    uint64_t i;

    FILE *metadata_fp, *record_fp;
	sprintf(meta_path, "%s/recipes/bv0.meta", target_path);	
	sprintf(recipe_path, "%s/recipes/bv0.recipe", target_path);	

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
    while ((one_file = sync_queue_pop(write_migriated_file_to_destor_queue))) {
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

	    	memcpy(recordbuf + recordbufoff, &one_file->fps[i], sizeof(fingerprint)); 
	    	recordbufoff += sizeof(fingerprint);
	    	memcpy(recordbuf + recordbufoff, &one_file->fp_cids[i], sizeof(containerid)); 
	    	recordbufoff += sizeof(containerid);
	    	memcpy(recordbuf + recordbufoff, &one_file->sizes[i], sizeof(one_file->sizes[i])); 
	    	recordbufoff += sizeof(one_file->sizes[i]);

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


	TIMER_END(2, time_2);
	printf("only restore migrated files cost:%lf(s)\n", time_2);
	
}

int main(int argc, char *argv[])
{

	if (3 != argc) {
		printf("usage: ./decold source_temp_path target_path\n");
		return -1;
	}
	
	double time_1 = 0;
	TIMER_DECLARE(1);

	TIMER_BEGIN(1);
	strcpy(source_temp_path, argv[1]);
	strcpy(target_path, argv[2]);
    
	write_migriated_file_to_destor_queue = sync_queue_new(50); 
	pthread_create(&tid1, NULL, restore_temp_thread, NULL);
	pthread_create(&tid3, NULL, write_migriated_files_to_destor_thread, NULL);
	pthread_join(tid1, NULL);
	pthread_join(tid3, NULL);
	TIMER_END(1, time_1);
	printf("total cost:%lf(s)\n", time_1);
	return 0;
}
