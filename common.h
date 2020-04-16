#ifndef COMMON_H_
#define COMMON_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stddef.h>
#include <string.h>
#include <stdarg.h>
#include <sys/time.h>
#include <errno.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <assert.h>
#include <glib.h>

#define LEVEL 1
/*
#define ENABLE_MIGRATION 1

//only for refs>1
#define ENABLE_REFS 0

#define ENABLE_TOPK 0

// #define AVG_REFS 20

// define big small file
#define BIG_FILE (10*1024)
#define BIG_MIGRATION_THRESHOLD 0.8
// #define SMALL_MIGRATION_THRESHOLD 0.9
*/

#define TIMER_DECLARE(n) struct timeval b##n,e##n
#define TIMER_BEGIN(n) gettimeofday(&b##n, NULL)
#define TIMER_END(n,t) gettimeofday(&e##n, NULL); (t)+=e##n.tv_usec-b##n.tv_usec+1000000*(e##n.tv_sec-b##n.tv_sec); (t)=(t)/1000000*1.0

/* signal chunk */
#define CHUNK_FILE_START (0x0001)
#define CHUNK_FILE_END (0x0002)
#define CHUNK_SEGMENT_START (0x0004)
#define CHUNK_SEGMENT_END (0x0008)

#define SET_CHUNK(c, f) (c->flag |= f)
#define UNSET_CHUNK(c, f) (c->flag &= ~f)
#define CHECK_CHUNK(c, f) (c->flag & f)

#define COLOR_NONE "\033[0m"
#define FONT_COLOR_RED "\033[0;31m"
#define FONT_COLOR_BLUE "\033[1;34m"
#define BACKGROUND_COLOR_RED "\033[41m" 




typedef unsigned char fingerprint[20];
typedef int64_t containerid; //container id

gboolean g_fingerprint_equal(const void *fp1, const void *fp2);
gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data);

void decold_log(const char *fmt, ...);

#define VERBOSE(fmt, arg...) decold_log(fmt, ##arg);

void hash2code(unsigned char hash[20], char code[40]);

void display_hash_table(GHashTable *table);

void storage_hash_table(GHashTable *table, char *ghash_file);
GHashTable *load_hash_table(char *ghash_file, uint64_t *unique_data_size);

void myprintf(const char *cmd, ...); 
void show_fingerprint(fingerprint *p);
gboolean g_fid_equal(const void *fid1, const void *fid2);
void free_fid(void *fid);
uint64_t get_hashtable_data_size(GHashTable *table);

uint64_t get_fp_count_from_hash(GHashTable *ghash);
int check_fp_in_hash(GHashTable *ghash, char *fp);
#endif
