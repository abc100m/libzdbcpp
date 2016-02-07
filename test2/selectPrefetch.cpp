#include <stdio.h>

#include <zdb.h>
#include <sys/time.h>
#include <stdlib.h>
#include <string.h>

/*
 mysql prefetch benchmark 
  */

int main(int argc, char **argv) {

        if (argc < 3) {
            printf("usage: %s mysql|oracle prefetchsize\n", argv[0]);
            return -2;
        }
        
        int prefetch_count = atoi(argv[2]);

        URL_T url;
        if (strcmp(argv[1], "mysql") == 0) {
            url = URL_new("mysql://192.168.11.100:3306/test?user=root&password=dba123");
        }
        else if (strcmp(argv[1], "oracle") == 0) {
            url = URL_new("oracle://192.168.11.100:1521/ora?user=scott&password=dba123");
            //url = URL_new("oracle:///ora?user=scott&password=dba123");
        }
        else {
            printf("unknown database type!\n");
            return -3;
        }
        
        ConnectionPool_T pool = ConnectionPool_new(url);
        TRY {
            ConnectionPool_start(pool);
        } CATCH(SQLException) {
            printf("SQLException -- %s\n", Exception_frame.message);
            return -1;
        }
        END_TRY;
        
        Connection_T con = ConnectionPool_getConnection(pool);
        TRY
        {
                Connection_setDefaultRowPrefetch(con, prefetch_count);
            
                clock_t t1 = clock();
                //Connection_execute(con, "create table if not exists bleach(name varchar(255), created_at timestamp)");
                ResultSet_T r = Connection_executeQuery(con,
                        "select * from test_1");
                //ResultSet_setFetchSize(r, prefetch_count);
                while (ResultSet_next(r))
                        //printf("%-10d \t %s\n", ResultSet_getInt(r, 1), ResultSet_getString(r, 2));
                        ResultSet_getInt(r, 1), ResultSet_getString(r, 2);
                clock_t t2 = clock();
                printf("%lu\n", t2 - t1);
                //Connection_execute(con, "drop table bleach;");
        }
        CATCH(SQLException)
        {
                printf("SQLException -- %s\n", Exception_frame.message);
        }
        FINALLY
        {
                Connection_close(con);
        }
        END_TRY;
        ConnectionPool_free(&pool);
        URL_free(&url);
}
