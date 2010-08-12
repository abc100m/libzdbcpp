#include <stdio.h>
#include <assert.h>

#include <URL.h>
#include <ResultSet.h>
#include <PreparedStatement.h>
#include <Connection.h>
#include <ConnectionPool.h>
#include <SQLException.h>

/*
 gcc -o select select.c -L/<libzdb>/lib -lzdb -lpthread -I/<libzdb>/include/zdb
 */

int main(void) {
        Connection_T con;
        URL_T url = URL_new("sqlite:///tmp/test.db");
        ConnectionPool_T pool = ConnectionPool_new(url);
        ConnectionPool_start(pool);
        con = ConnectionPool_getConnection(pool);
        TRY
        {
                int i;
                char *bleach[] = {
                        "Ichigo Kurosaki", "Rukia Kuchiki", "Orihime Inoue",  "Yasutora \"Chad\" Sado", 
                        "Kisuke Urahara", "Uryū Ishida", "Renji Abarai", 0
                };
                Connection_execute(con, "create table bleach(name varchar(255));");
                PreparedStatement_T p = Connection_prepareStatement(con, "insert into bleach values (?);"); 
                for (i = 0; bleach[i]; i++) {
                        PreparedStatement_setString(p, 1, bleach[i]);
                        PreparedStatement_execute(p);
                }
                ResultSet_T result = Connection_executeQuery(con, "select name from bleach;");
                while (ResultSet_next(result))
                        printf("%s\n", ResultSet_getString(result, 1));
                Connection_execute(con, "drop table bleach;");
        }
        CATCH(SQLException)
        {
                printf("SQLException -- %s\n", Exception_frame.message);
        }
        FINALLY
        {
                Connection_close(con);
                ConnectionPool_free(&pool);
                URL_free(&url);
        }
        END_TRY;
        return 0;
}
