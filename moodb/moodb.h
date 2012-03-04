
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

/**
 * Open a new database connection. If the filename does not already exist, a new database
 * will be created.
 *
 * @param [in] filename the filepath to the database
 * @param [out] ppDb return pointer to the database connection object
 */
int moodb_open(const char *filename, moodb **ppDb);
void moodb_close(moodb *pDb);

/**
 * Insert or update a data document.
 *
 * @param [in] pDB
 * @param [in] key The key. If this value is NULL, a UUID will automatically be generated for the new document. If
 * @sa pkey is not NULL, it will contain the key of the newly inserted document.
 * @param [in] data the data to insert. Should be encoded in JSON format.
 * @param [out] pkey if not NULL, this will hold the key of the newly inserted document.
 */
int moodb_putobject(moodb *pDB, const char *key, const char* data, char** pkey);

/**
 * fetches a data document based on its key.
 *
 * @param [in] pDB
 * @param [in] key the key to look up
 * @parma [out] pdata pointer to the returned data. When done, this must be freed using moodb_free()
 * @see moodb_free()
 */
int moodb_getobject(moodb *pDB, const char *key, char** pdata);
int moodb_deleteobject(moodb *pDB, const char *key);

int moodb_putview(moodb *pDB, const char *viewspec);

int moodb_query(moodb *pDB, moocursor **ppCursor, const char* query);

int moodbcursor_next(moocursor *pCursor, char **ppKey, char **Value);
int moodbcursor_close(moocursor *pCursor);

void moodb_free(void *);

#ifdef __cplusplus
}
#endif

#endif /* MOODB_H_ */
