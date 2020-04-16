#include <fcntl.h>
#include <stdio.h>
#include <unistd.h> 
#include <glib.h>
#include "common.h"

struct path_fid {

	uint64_t fid;
	char *path;
};

FILE *fid_map_fp = NULL;
char *fid_map_buffer = NULL;
GHashTable *fid_hash;
int main(int argc, char *argv)
{

	if (argc <= 1)
		return ;

	char fid_map1[128] = {0}, fid_map2[128] = {0};
	strcpy(fid_map1, argv[2]);
	strcpy(fid_map2, argv[3]);

	char generate_path[128] = {0};
	strcpy(generate_path, argv[1]);

	fid_hash = g_hash_table_new_full(g_int64_hash, g_fid_equal, NULL, NULL);

	uint64_t ord_file_count  = 0;
	uint64_t cur_file_count  = 0;

	fid_map_fp = fopen(fid_map1, "r");
	int32_t path_len = 0;
	uint64_t fid;
	char file_path[128] = {0};
	while (fread(&path_len, sizeof(path_len), 1, fid_map_fp)) {
		struct path_fid *one_file= (struct path_fid *)malloc(sizeof(struct path_fid));
		one_file->path = malloc(path_len + 1);

		fread(one_file->path, path_len, 1, fid_map_fp);	
		file_path[path_len] = '\0';
		fread(&fid, sizeof(fid), 1, fid_map_fp);

		g_hash_table_insert(fid_hash, &one_file->fid, one_file);
		ord_file_count++;
	}

	fclose(fid_map_fp);

	fid_map_fp = fopen(fid_map1, "r");
	while (fread(&path_len, sizeof(path_len), 1, fid_map_fp)) {
		struct path_fid *one_file= (struct path_fid *) malloc(sizeof(struct path_fid));
		one_file->path = malloc(path_len + 1);

		fread(one_file->path, path_len, 1, fid_map_fp);	
		file_path[path_len] = '\0';
		fread(&fid, sizeof(fid), 1, fid_map_fp);

		g_hash_table_insert(fid_hash, &one_file->fid, one_file);
		ord_file_count++;
	}

	fclose(fid_map_fp);


	printf("expect file count:%lu\n", ord_file_count);


	DIR *d = NULL;
	struct dirent *dp = NULL;
	struct stat st;


	char old_path[256] = {0};
	char new_path[256] = {0};
	d = opendir(generate_path);
	if (NULL == d)
		printf("error\n");

	while((dp = readdir(d)) != NULL) {
		if((!strncmp(dp->d_name, ".", 1)) || (!strncmp(dp->d_name, "..", 2)))
			continue;

		if (0 == atoi(dp->d_name))
			continue;
	
		uint64_t one_fid;
		sscanf(dp->d_name, "%lu", &one_fid);
	
		struct path_fid *t = g_hash_table_lookup(fid_hash, &one_fid);
		if (NULL == t) {
			printf("can't find %lu\n", one_fid);
		}

		sprintf(new_path, "%s/%s", generate_path, t->path);
		sprintf(old_path, "%s/%s", generate_path, dp->d_name);
	
		rename(old_path, new_path);

		cur_file_count++;
		
	}
		
	printf("cue file count:%lu\n", cur_file_count);
	
		
	return 0;
}


