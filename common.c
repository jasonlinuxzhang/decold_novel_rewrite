#include "common.h"
#include "decold.h"

extern int target_group;
extern char g1_path[32];
extern char g2_path[32];
uint64_t item_count = 0;

static void free_chunk(struct chunk* ck) {
    if (ck->data) {
    free(ck->data);
    ck->data = NULL;
    }
    free(ck);
}


void hash2code(unsigned char hash[20], char code[40])
{
	int i, j, b;
	unsigned char a, c;
	i = 0;
	for (i = 0; i < 20; i++)
	{
		a = hash[i];
		for (j = 0; j < 2; j++)
		{
			b = a / 16;
			switch (b)
			{
			case 10:
				c = 'A';
				break;
			case 11:
				c = 'B';
				break;
			case 12:
				c = 'C';
				break;
			case 13:
				c = 'D';
				break;
			case 14:
				c = 'E';
				break;
			case 15:
				c = 'F';
				break;
			default:
				c = b + 48;
				break;
			}
			code[2 * i + j] = c;
			a = a << 4;
		}
	}
}

void decold_log(const char *fmt, ...)
{
	va_list ap;
	char msg[1024];

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stdout, "%s", msg);
}

int comp_code(unsigned char hash1[20], unsigned char hash2[20])
{
	int i = 0;
	while (i<20)
	{
		if (hash1[i]>hash2[i])
			return 1;
		else if (hash1[i] < hash2[i])
			return -1;
		else
			i++;
	}

	return 0;
}

void print_unsigned(unsigned char *u, int64_t len)
{
	int64_t i;
	for (i = 0; i < len; i++)
		printf("%hhu", u[i]);
	printf("\n");
}
gboolean g_fid_equal(const void *fid1, const void *fid2)
{
	return *(uint64_t *)fid1 == *(uint64_t *)fid2;
}
void free_fid(void *fid)
{
	return free(fid);
}

gboolean g_fingerprint_equal(const void *fp1, const void *fp2)
{
	return !memcmp((fingerprint*)fp1, (fingerprint*)fp2, sizeof(fingerprint));
}

gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data) {
    return memcmp(fp1, fp2, sizeof(fingerprint));
}

void print_key_value(gpointer key, gpointer value, gpointer user_data)
{
    char code[41] = {0};
    fingerprint *fp = key;
    struct chunk *ck = value; 
    hash2code(*fp, code);
    printf("%s --> %lu\n", code, ck->id);
}

void display_hash_table(GHashTable *table)
{
    g_hash_table_foreach(table, print_key_value, NULL);
}

void storage_key_value(gpointer key, gpointer value, gpointer user_data)
{

    fingerprint *fp = key;
    struct chunk *ck = value;
    FILE *filep = user_data;
    
	ck->data = NULL;
    fwrite(ck, sizeof(struct chunk), 1, filep);
    fwrite(fp, sizeof(fingerprint), 1, filep);
    
    item_count++;
}

void show_fingerprint(fingerprint *p)
{
	char code[41] = {0};
	hash2code(*p, code);
	printf("fp:%s\n", code);
}

void storage_hash_table(GHashTable *table, char *ghash_file) 
{

    FILE *fp = fopen(ghash_file, "w+");
    if (NULL == fp) {
	printf("fopen %s failed\n", ghash_file);
	return;
    }

    item_count = 0;
    fwrite(&item_count, sizeof(item_count), 1, fp);

    g_hash_table_foreach(table, storage_key_value, fp);

    fseek(fp, 0, SEEK_SET);
    printf("write ghash table %lu item to %s\n", item_count, ghash_file);
    fwrite(&item_count, sizeof(item_count), 1, fp);
    fclose(fp);
}
void get_dat_size(gpointer key, gpointer value, gpointer user_data)
{

    fingerprint *fp = key;
    struct chunk *ck = value;
    
	uint64_t *data_size = user_data;

	*data_size += ck->size;
}

uint64_t get_hashtable_data_size(GHashTable *table)
{
	uint64_t unique_data_size = 0;
    g_hash_table_foreach(table, get_dat_size, &unique_data_size);
	
	return unique_data_size;
}

GHashTable *load_hash_table(char *ghash_file, uint64_t * unique_data_size)
{
	FILE *fp = fopen(ghash_file, "r");
	if (NULL == fp) {
		printf("fopen %s failed\n", ghash_file);
		return NULL;
	}

	GHashTable *table;

	uint64_t unique_chunks_number;
	fread(&unique_chunks_number, sizeof(unique_chunks_number), 1, fp);

	printf("%s ghash_file have %lu items\n", ghash_file, unique_chunks_number);

	table = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);

	uint64_t data_size = 0;
	uint64_t i = 0;
	for (i = 0; i < unique_chunks_number; i++) {
		struct chunk *ck = (struct chunk *)malloc(sizeof(struct chunk));
    	fread(ck, sizeof(struct chunk), 1, fp);
    	fread(&ck->fp, sizeof(fingerprint), 1, fp);
		ck->data = NULL;
		g_hash_table_insert(table, &ck->fp, ck);
		data_size += ck->size;
	}	

	printf(FONT_COLOR_RED"%s unique chunk size %lu \n"COLOR_NONE, ghash_file, data_size);
	if (NULL != unique_data_size)
		*unique_data_size = data_size;

	fclose(fp);
	return table;
}


void myprintf(const char *cmd, ...)  
{  
    if (LEVEL <= 2)
	return; 
    va_list args;        
    va_start(args,cmd); 
    vprintf(cmd,args);  
    va_end(args);   
} 


void get_one_fp_dup_size(gpointer key, gpointer value, gpointer user_data) {
	uint64_t *total_size = (uint64_t *)user_data;
    fingerprint *fp = key;
    struct chunk *ck = value;
	*total_size += ck->ref_count; 
}
uint64_t get_fp_count_from_hash(GHashTable *ghash)
{
	uint64_t total_size = 0;
    g_hash_table_foreach(ghash, get_one_fp_dup_size, &total_size);

	return total_size;

}

void check_one(gpointer key, gpointer value, gpointer user_data)
{
    char code[41] = {0};
    fingerprint *fp = key;
    struct chunk *ck = value; 
    hash2code(*fp, code);

	if (ck->size == 13227) {
		printf("%s\n", code);
	}

	char *target_fp = (char *)user_data;
	
	if (0 == strcmp(target_fp, code)) 
	{
		printf("%s exist\n", target_fp);
	}
}

int check_fp_in_hash(GHashTable *ghash, char *fp)
{
    g_hash_table_foreach(ghash, check_one, fp);
}

