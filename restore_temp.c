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

char source_temp_path[128] = {0};
char target_path[128] = {0};

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

void *restore_temp_thread(void *arg) {
    char temp_identified_file_path[128];
    char g_hash_file[128] = {0};


	sprintf(temp_identified_file_path, "%s/identified_file", source_temp_path);
	sprintf(g_hash_file, "%s/ghash_file", target_path);

    GHashTable *recently_unique_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);
    FILE *hash_filep = fopen(g_hash_file, "r");
    if (NULL == hash_filep)
    {
	printf("fopen %s failed\n", g_hash_file);
	goto out;
    }
    uint64_t item_count = 0;
    fread(&item_count, sizeof(item_count), 1, hash_filep);

    printf("%s have %lu item\n", target_path, item_count);

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
    printf("%s have %lu identified file move to %s\n", source_temp_path, identified_file_count, target_path);
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

    sync_queue_term(write_identified_file_to_destor_queue);
}

void *write_identified_files_to_destor_thread(void *arg) {
    
    char meta_path[128] = {0};
    char recipe_path[128] = {0};
    uint64_t i;

    FILE *metadata_fp, *record_fp;
	sprintf(meta_path, "%s/bv0.meta", target_path);	
	sprintf(recipe_path, "%s/bv0.recipe", target_path);	

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


int main(int argc, char *argv[])
{

	if (3 != argc) {
		printf("usage: ./decold soure_temp_path trget_path\n");
		return -1;
	}
    

	strcpy(source_temp_path, argv[1]);
	strcpy(target_path, argv[2]);

	write_identified_file_to_destor_queue = sync_queue_new(50); 
	pthread_create(&tid1, NULL, restore_temp_thread, NULL);
	pthread_create(&tid3, NULL, write_identified_files_to_destor_thread, NULL);
	pthread_join(tid1, NULL);
	pthread_join(tid3, NULL);

	return 0;
}
