#ifndef _DECOLD
#define _DECOLD


struct{
    /* accessed in dedup phase */
    struct container *container_buffer;
    /* In order to facilitate sampling in container,
 *   * we keep a list for chunks in container buffer. */
    GSequence *chunks;
} storage_buffer;

struct chunk {
    int32_t size;
    int flag;
    containerid id;
    fingerprint fp;
	int32_t ref_count;
    unsigned char *data;
};

struct fp_data
{
    uint64_t size;
    char *data;
};


#endif

