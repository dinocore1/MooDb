
#ifndef MOODB_H_
#define MOODB_H_

#ifdef __cplusplus
extern "C" {
#endif

/* status codes */
#define MOODB_OK 0
#define MOODB_ERROR 1

typedef struct moodb moodb;
typedef struct moocursor moocursor;

int moodb_open(const char *filename, moodb **ppDb);
void moodb_close(moodb *pDb);

int moodb_putobject(moodb *pDB, const char *id, const char* jsonData, char** ppId);
int moodb_getobject(moodb *pDB, const char *id, char** pJsonData);
int moodb_deleteobject(moodb *pDB, const char *id);

int moodb_putview(moodb *pDB, const char *viewspec);

int moodb_query(moodb *pDB, moocursor **ppCursor, const char* query);

int moodbcursor_next(moocursor *pCursor, char **ppKey, char **Value);
int moodbcursor_close(moocursor *pCursor);

void moodb_free(void *);

#ifdef __cplusplus
}
#endif

#endif /* MOODB_H_ */
