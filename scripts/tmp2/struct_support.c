#include <stdio.h>
#include <errno.h>
#include <sys/time.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>

char NAME_TRIPLETS[20] = "triplets";
int CHUNK_SIZE = 1000;

char *curdate() {
    time_t     now = time(0);
    struct tm  tstruct;
    char       buf[80], *ans;
    tstruct = *localtime(&now);
    strftime(buf, sizeof(buf), "%Y-%m-%d.%X", &tstruct);
	ans = (char *)malloc( strlen(buf) );
	strcpy( ans, buf );
    return ans;
}

FILE *init_log( char *source ){
	char tname[40];
	sprintf(tname,"logs/%s-%s.log", tname, curdate());
	return fopen( tname, "w+");
}

char *addslashes(char *s){
	char p[ strlen(s) * 4 ];
	char *t = &p[0];
	int len = 0;
	while ( *s ){
		if (*s == '\"' || *s == '\'' )	*t = '\\', t++, len++;
		*t = *s;
		t++,s++;
		len++;
	}
	*t = 0;
	char *real_t = (char *)malloc( len + 1 );
	strcpy( real_t, p );
	return real_t;
	
}


void generate_triplets( FILE *log, char *file_name ){
	time_t start, stop;
    clock_t ticks; long count;
	char command[100];
	sprintf( command, "cat %s | awk '{out=$1; for(i=2;i<NF-1;i++){out=out\" \"$i}; print out}' > %s", file_name, NAME_TRIPLETS);
//	printf("%s\n",command);
	time(&start);
	fprintf(log,"PRE-PROCESS TRIPLETS: Started\n", difftime(stop, start));
	fprintf(log,"\tCommand executed: %s\n", command );
	system(command);
	time(&stop);
	fprintf(log, "PRE-PROCESS TRIPLETS: Finished in about %.2f seconds. \n\n", difftime(stop, start));
}

void insertTrip(){
	char name_out[100] = "insert-";
	strcat(name_out,NAME_TRIPLETS);
	strcat(name_out,".sql");
	FILE *fcol = fopen(NAME_TRIPLETS,"r"), *fout = fopen(name_out,"w") ;
	char line[ 10005 ];
	int pos = 0;
	fprintf(fout,"INSERT facts VALUES " );
	while( fgets(line, sizeof line ,fcol ) != NULL ){
		int len = strlen(line);
		if ( len == 0 ) continue;
		if ( pos ) fprintf(fout, ",");
		fprintf(fout,"\n");

		char *token1 = strtok(line," ");
		char *token2 = strtok(NULL," ");
		char *token3 = strtok(NULL,"\n");


		fprintf(fout,"(unhex(md5('%s')), unhex(md5('%s')), unhex(md5('%s')) )",addslashes(token1),addslashes(token2),addslashes(token3) );
		if ( pos == CHUNK_SIZE ){
			pos = -1;
			fprintf(fout,";\n");
			fprintf(fout,"INSERT facts VALUES");
		}
		pos++;
		
	}
	fprintf(fout,";");
	fclose(fcol);
	fclose(fout);
}
void insertCol( char *name_col, int col ){
	char name_out[100] = "insert-";
	strcat(name_out,name_col);
	strcat(name_out,".sql");
	FILE *fcol = fopen(name_col,"r"), *fout = fopen(name_out,"w") ;
	char line[ 10005 ];
	int pos = 0;
	fprintf(fout,"INSERT c%d VALUES ", col);
	while( fgets(line, sizeof line ,fcol ) != NULL ){
		int len = strlen(line);
		if ( len == 0 ) continue;
		line[len-1] = 0; // remove the '\n'
		if ( pos ) fprintf(fout, ",");
		fprintf(fout,"\n");
		fprintf(fout,"(unhex(md5('%s')), '%s')",line, addslashes(line));
		if ( pos == CHUNK_SIZE ){
			pos = -1;
			fprintf(fout,";\n");
			fprintf(fout,"INSERT C%d VALUES", col);
		}
		pos++;
		
	}
	fprintf(fout,";");
	fclose(fcol);
	fclose(fout);
}

void get_column( FILE *log, int col, int multiple ){
	time_t start, stop;
    clock_t ticks; long count;
	char command[100], name_col[100];
	int fd[2], rows;

	sprintf(name_col, "%s-%d", NAME_TRIPLETS, col);
	if (! multiple ){
		sprintf( command, "cat %s | awk '{print $%d}' | sort -u > %s", NAME_TRIPLETS, col ,name_col);
	}else{
		sprintf( command, "cat %s | awk '{out=$%d; for(i=%d;i<=NF;i++){out=out\" \"$i}; print out}' | sort -u > %s", NAME_TRIPLETS, col, col + 1, name_col);
	}
	time(&start);
	fprintf(log,"CREATING UNIQUE COLUMNS: Started\n", difftime(stop, start));
	fprintf(log,"\tCommand executed: %s\n", command );
	system(command);
	pipe(fd);
	dup2( fd[1], 1 );
	sprintf(command, "wc -l %s", name_col);
	system(command);
	read( fd[0] , &rows, sizeof(rows));
	fprintf(log,"\tTotal rows result: %d\n", rows );
	fprintf(log,"\tProcessing inserts + hashes\n");
	
	insertCol( name_col, col );

	time(&stop);
	fprintf(log, "CREATING UNIQUE COLUMNS: Finished in about %.2f seconds. \n\n", difftime(stop, start));
}

