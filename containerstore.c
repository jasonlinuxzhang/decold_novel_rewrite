#include "containerstore.h"
#include "common.h"
#include "queue.h"
#include "sync_queue.h"
#include "decold.h"
#include "serial.h"

static pthread_t append_t;
static FILE *container_pool_fp;
static SyncQueue *container_buffer;
containerid container_count  = 0;

static void init_container_meta(struct containerMeta *meta) {
    meta->chunk_num = 0;
    meta->data_size = 0;
    meta->id = -1;
    meta->map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL,
	    free);
}

struct container* create_container() {

    struct container *c = (struct container*) malloc(sizeof(struct container));
    c->data = calloc(1, CONTAINER_SIZE);

    init_container_meta(&c->meta);
    c->meta.id = container_count++;
    return c;
}



int container_overflow(struct container* c, int32_t size) {
    if (c->meta.data_size + size > CONTAINER_SIZE - CONTAINER_META_SIZE)
	return 1;
    /*
 *   * 28 is the size of metaEntry.
 *	 */
    if ((c->meta.chunk_num + 1) * 28 + 16 > CONTAINER_META_SIZE)
	return 1;
    return 0;
}

int add_chunk_to_container(struct container* c, struct chunk* ck) {
    //assert(!container_overflow(c, ck->size));
    
    char code[41] = {0};

    if (g_hash_table_contains(c->meta.map, &ck->fp)) {
    	//hash2code(ck->fp, code);
		//printf("fp:%s already exist\n", code);
		ck->id = c->meta.id;
		return 0;
    }

    struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
    memcpy(&me->fp, &ck->fp, sizeof(fingerprint));
    me->len = ck->size;
    me->off = c->meta.data_size;
    //memset(code, 0, sizeof(code));
    //hash2code(me->fp, code);
    //myprintf("add fp:%s len:%d off:%d container_id:%lu\n", code, me->len, me->off, c->meta.id);

    g_hash_table_insert(c->meta.map, &me->fp, me);
    c->meta.chunk_num++;

    memcpy(c->data + c->meta.data_size, ck->data, ck->size);

    c->meta.data_size += ck->size;

    ck->id = c->meta.id;

    return 1;
}

void free_container_meta(struct containerMeta* cm) {
    g_hash_table_destroy(cm->map);
    free(cm);
}

void free_container(struct container* c) {
    g_hash_table_destroy(c->meta.map);
    if (c->data)
	free(c->data);
    free(c);
}

int container_empty(struct container* c) {
    return c->meta.chunk_num == 0 ? 1 : 0;
}

void write_container_async(struct container* c) {
    assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));
    if (container_empty(c)) {
	container_count--;
	VERBOSE("Append phase: Deny writing an empty container %lld",
		c->meta.id);
	return;
    }

    sync_queue_push(container_buffer, c);
}

void write_container(struct container* c) {

    assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

    if (container_empty(c)) {
	/* An empty container
 *	 * It possibly occurs in the end of backup */
	container_count--;
	VERBOSE("Append phase: Deny writing an empty container %lld",
		c->meta.id);
	return;
    }

	if (c->meta.id%100 == 0)
    VERBOSE("Append phase: Writing container %lld of %d chunks\n", c->meta.id, c->meta.chunk_num);

    unsigned char * cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];
    ser_declare;
    ser_begin(cur, CONTAINER_META_SIZE);
    ser_int64(c->meta.id);
    ser_int32(c->meta.chunk_num);
    ser_int32(c->meta.data_size);
    //printf("write container %lu data_size %d\n", c->meta.id, c->meta.data_size);

    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, c->meta.map);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
	struct metaEntry *me = (struct metaEntry *) value;
        ser_bytes(&me->fp, sizeof(fingerprint));
        ser_bytes(&me->len, sizeof(int32_t));
        ser_bytes(&me->off, sizeof(int32_t));
        //char code[41] = {0};
        //hash2code(me->fp, code);	
        //myprintf("write fp %s off %lu chunk_size %d\n", code, me->off, me->len);
    }

    ser_end(cur, CONTAINER_META_SIZE);

	if (NULL == container_pool_fp) 
		{
			printf("container pool fp null\n");
		}
    if (fseek(container_pool_fp, c->meta.id * CONTAINER_SIZE + 8, SEEK_SET) != 0) {
        perror("Fail seek in container store.");
        exit(1);
    }
    if(fwrite(c->data, CONTAINER_SIZE, 1, container_pool_fp) != 1){
        perror("Fail to write a container in container store.");
        exit(1);
    }

}

static void* append_thread(void *arg) {
    while (1) {
	struct container *c = sync_queue_pop(container_buffer);
	if (c == NULL)
	    break;

	write_container(c);
	free(c->data);
	free(c);
	g_hash_table_destroy(c->meta.map);	
    }

    return NULL;
}

void init_container_store(char *container_path, char *mode) {
    
    container_buffer = sync_queue_new(25);
    container_pool_fp = fopen(container_path, mode);
	if (NULL == container_pool_fp) {
		printf("fopen %s failed\n", container_path);
		assert(1== 0);
	}

	if (0 == strcmp(mode, "r+")) {
		fread(&container_count, sizeof(container_count), 1, container_pool_fp);	
	}
	
    pthread_create(&append_t, NULL, append_thread, NULL);
}

void close_container_store() {
    sync_queue_term(container_buffer);

    pthread_join(append_t, NULL);

    fseek(container_pool_fp, 0, SEEK_SET);
    fwrite(&container_count, sizeof(container_count), 1, container_pool_fp);
    container_count = 0;

    fclose(container_pool_fp);
    container_pool_fp = NULL;
	printf("closed\n");

}
