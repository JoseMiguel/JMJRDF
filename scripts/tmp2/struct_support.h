#define DEBUG 0

FILE *init_log(char*);
char *curdate();
char *addslashes(char *);
void generate_triplets( FILE *, char *);
void insertTrip();
void insertCol( char *, int );
void get_column( FILE *, int , int );
