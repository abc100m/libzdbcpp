extern "C" {
#include "Config.h"
}

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <vector>
#include "zdbcpp.h"



/**
 * libzdb connection pool unity tests. 
 */
#define BSIZE 2048

#define SCHEMA_MYSQL      "CREATE TABLE zild_t(id INTEGER AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), percent REAL, image BLOB);"
#define SCHEMA_POSTGRESQL "CREATE TABLE zild_t(id SERIAL PRIMARY KEY, name VARCHAR(255), percent REAL, image BYTEA);"
#define SCHEMA_SQLITE     "CREATE TABLE zild_t(id INTEGER PRIMARY KEY, name VARCHAR(255), percent REAL, image BLOB);"
#define SCHEMA_ORACLE     "CREATE TABLE zild_t(id NUMBER , name VARCHAR(255), percent REAL, image CLOB);"

#if HAVE_STRUCT_TM_TM_GMTOFF
#define TM_GMTOFF tm_gmtoff
#else
#define TM_GMTOFF tm_wday
#endif


static void TabortHandler(const char *error) {
        fprintf(stdout, "Error: %s\n", error);
        exit(1);
}

static void testPool(const char *testURL) {
        using namespace zdbcpp;
        
        const char *schema = NULL;
        char *data[]= {"Fry", "Leela", "Bender", "Farnsworth",
                "Zoidberg", "Amy", "Hermes", "Nibbler", "Cubert",
                "Zapp", "Joey Mousepad", "ЯΣ༆", 0}; 
        
        if (Str_startsWith(testURL, "mysql")) {
                schema = SCHEMA_MYSQL;
        } else if (Str_startsWith(testURL, "postgresql")) {
                schema = SCHEMA_POSTGRESQL;
        } else if (Str_startsWith(testURL, "sqlite")) {
                schema = SCHEMA_SQLITE;
        } else if (Str_startsWith(testURL, "oracle")) {
                schema = SCHEMA_ORACLE;
        }
        else {
                printf("Unsupported database protocol\n");
                exit(1);
        }
        
        printf("=> Test3: start/stop\n");
        {
            {
                ConnectionPool pool(testURL);
                assert(pool);
                pool.start();
                pool.stop();
            }
                // Test that exception is thrown on start error
                try
                {
                        ConnectionPool pool("not://a/database");
                        assert(pool);
                        pool.start();
                        printf("\tResult: Test failed -- exception not thrown\n");
                        exit(1);
                } catch (sql_exception& e) {
                        //OK
                }
        }
        printf("=> Test3: OK\n\n");
        
        printf("=> Test4: Connection execute & transaction\n");
        {
                ConnectionPool pool(testURL);
                assert(pool);
                pool.setAbortHandler(TabortHandler);
                pool.start();

                Connection con = pool.getConnection();
                assert(con);
                try { 
                    con.execute("drop table zild_t;"); 
                } catch (...) {}

                con.execute(schema);

                con.beginTransaction();
                /* Insert values into database and assume that auto increment of id works */
                long long affected_rows = 0;
                for (int i = 0; data[i]; i++)
                    con.execute("insert into zild_t (name, percent) values(?, ?);", data[i], i + 1 );

                // Assert that the last insert statement added one row
                assert(con.rowsChanged() == 1);
                /* Assert that last row id works for MySQL and SQLite. Neither Oracle nor PostgreSQL
                 support last row id directly. The way to do this in PostgreSQL is to use 
                 currval() or return the id on insert. */
                const URL& url = pool.getURL();
                if (IS(url.getProtocol(), "sqlite") || IS(url.getProtocol(), "mysql"))
                    assert(con.lastRowId() == 12);
                con.commit();
                printf("\tResult: table zild_t successfully created\n");
        }
        printf("=> Test4: OK\n\n");     

        printf("=> Test5: Prepared Statement\n");
        {
                int i;
                char blob[8192];
                char *images[]= {"Ceci n'est pas une pipe", "Mona Lisa",
                        "Bryllup i Hardanger", "The Scream",
                        "Vampyre", "Balcony", "Cycle", "Day & Night", 
                        "Hand with Reflecting Sphere",
                        "Drawing Hands", "Ascending and Descending", 0}; 

                ConnectionPool pool(testURL);
                pool.start();

                Connection con = pool.getConnection();
                assert(con);
                // 1. Prepared statement, perform a nonsense update to test rowsChanged
                PreparedStatement p1 = con.prepareStatement("update zild_t set image=?;");
                p1.setString(1, "");
                p1.execute();
                printf("\tRows changed: %lld\n", p1.rowsChanged());
                // Assert that all 12 rows in the data set was changed
                assert(p1.rowsChanged() == 12);

                // 2. Prepared statement, update the table proper with "images". 
                PreparedStatement pre = con.prepareStatement("update zild_t set image=? where id=?;");
                assert(pre);
                assert(pre.getParameterCount() == 2);
                for (i = 0; images[i]; i++) {
                    pre.setBlob(1, images[i], (int)strlen(images[i]) + 1);
                    pre.setInt(2, i + 1);
                    pre.execute();
                }
                // The last execute changed one row only
                assert(pre.rowsChanged() == 1);
                /* Add a database null blob value for id = 5 */
                pre.setBlob(1, NULL, 0);
                pre.setInt(2, 5);
                pre.execute();
                /* Add a database null string value for id = 1 */
                pre.setString(1, NULL);
                pre.setInt(2, 1);
                pre.execute();
                /* Add a large blob */
                memset(blob, 'x', 8192);
                blob[8191] = 0;
                /* Mark start and end */
                *blob='S'; blob[8190] = 'E';
                pre.setBlob(1, blob, 8192);
                pre.setInt(2, i + 1);
                pre.execute();
                printf("\tResult: prepared statement successfully executed\n");
        }
        printf("=> Test5: OK\n\n");     
        

        printf("=> Test6: Result Sets\n");
        {
                int i;
                int imagesize = 0;

                ConnectionPool pool(testURL);
                pool.start();

                Connection con = pool.getConnection();
                assert(con);
                {

                    ResultSet rset = con.executeQuery("select id, name, percent, image from zild_t where id < ? order by id;", 100);
                    assert(rset);
                    printf("\tResult:\n");
                    printf("\tNumber of columns in resultset: %d\n\t", rset.getColumnCount());
                    assert(4 == rset.getColumnCount());
                    i = 1;
                    printf("%-5s", rset.getColumnName(i++));
                    printf("%-16s", rset.getColumnName(i++));
                    printf("%-10s", rset.getColumnName(i++));
                    printf("%-16s", rset.getColumnName(i++));
                    printf("\n\t------------------------------------------------------\n");
                    while (rset.next()) {
                        int id = rset.getIntByName("id");
                        const char *name = rset.getString(2);
                        double percent = rset.getDoubleByName("percent");
                        const char *blob = (char*)rset.getBlob(4, &imagesize);
                        printf("\t%-5d%-16s%-10.2f%-16.38s\n", id, name ? name : "null", percent, imagesize ? blob : "");
                    }
                }

                {
                    // Column count
                    ResultSet rset = con.executeQuery("select image from zild_t where id=12;");
                    assert(1 == rset.getColumnCount());
                
                    // Assert that types are interchangeable and that all data is returned
                    while (rset.next()) {
                        const char *image = rset.getStringByName("image");
                        const void *blob = rset.getBlobByName("image", &imagesize);
                        assert(image && blob);
                        assert(strlen(image) + 1 == 8192);
                        assert(imagesize == 8192);
                    }
                }
                
                {
                    printf("\tResult: check isnull..");
                    ResultSet rset = con.executeQuery("select id, image from zild_t where id in(1,5,2);");
                    while (rset.next()) {
                        int id = rset.getIntByName("id");
                        if (id == 1 || id == 5)
                            assert(rset.isnull(2) == true);
                        else
                            assert(rset.isnull(2) == false);
                    }
                    printf("success\n");
                }

                {
                    printf("\tResult: check max rows..");
                    con.setMaxRows(3);
                    ResultSet rset = con.executeQuery("select id from zild_t;");
                    assert(rset);
                    i = 0;
                    while (rset.next()) i++;
                    assert((i) == 3);
                    printf("success\n");
                }
                
                {
                    printf("\tResult: check prepared statement resultset..");
                    con.setMaxRows(0);
                    PreparedStatement pre = con.prepareStatement("select name from zild_t where id=?");
                    assert(pre);
                    pre.setInt(1, 2);
                    ResultSet names = pre.executeQuery();
                    assert(names);
                    assert(names.next());
                    assert(Str_isEqual("Leela", names.getString(1)));
                    printf("success\n");

                    printf("\tResult: check prepared statement re-execute..");
                    pre.setInt(1, 1);
                    ResultSet names1 = pre.executeQuery();
                    assert(names1);
                    assert(names1.next());
                    assert(Str_isEqual("Fry", names1.getString(1)));
                    printf("success\n");
                }
                
                {
                    printf("\tResult: check prepared statement without in-params..");
                    PreparedStatement pre = con.prepareStatement("select name from zild_t;");
                    assert(pre);
                    ResultSet names = pre.executeQuery();
                    assert(names);
                    int i;
                    for (i = 0; names.next(); i++);
                    assert(i == 12);
                    printf("success\n");
                }
                
                /* Need to close and release statements before
                   we can drop the table, sqlite need this */
                con.execute("drop table zild_t;");
        }
        printf("=> Test6: OK\n\n");
        
        printf("=> Test7: reaper start/stop\n");
        {
                int i;
                std::vector<Connection> v;

                ConnectionPool pool(testURL);
                assert(pool);
                pool.setInitialConnections( 4);
                pool.setMaxConnections(20);
                pool.setConnectionTimeout(4);
                pool.setReaper(4);
                pool.setAbortHandler(TabortHandler);
                pool.start();
                assert(4 == pool.size());

                printf("Creating 20 Connections..");
                for (i = 0; i < 20; i++) {
                    v.push_back(pool.getConnection());
                }
                printf("pool size:%d, vector size:%u", pool.size(), v.size());
                assert(pool.size() == 20);
                assert(pool.active() == 20);
                printf("success\n");
                printf("Closing Connections down to initial..");
                for (Connection& con: v) {
                    con.close();
                }
                assert(pool.active() == 0);
                assert(pool.size() == 20);
                printf("success\n");
                printf("Please wait 10 sec for reaper to harvest closed connections..");
                Connection con = pool.getConnection(); // Activate one connection to verify the reaper does not close any active
                fflush(stdout);
                sleep(10);
                assert(5 == pool.size()); // 4 initial connections + the one active we got above
                assert(1 == pool.active());
                printf("success\n");
        }
        printf("=> Test7: OK\n\n");


        printf("=> Test8: Exceptions handling\n");
        {
                int i;
                ConnectionPool pool(testURL);
                assert(pool);
                pool.setAbortHandler(TabortHandler);
                pool.start();
                Connection con = pool.getConnection();
                assert(con);
                /* 
                 * The following should work without throwing exceptions 
                 */
                try
                {
                        con.execute(schema);
                } catch (sql_exception& e) {
                        printf("\tResult: Creating table zild_t failed -- %s\n", e.what());
                        assert(false); // Should not fail
                }
                try
                {
                        con.beginTransaction();
                        for (i = 0; data[i]; i++) 
                                con.execute("insert into zild_t (name, percent) values(?, ?);", data[i], i+1);
                        con.commit();
                        printf("\tResult: table zild_t successfully created\n");
                }
                catch (sql_exception& e)
                {
                        printf("\tResult: Test failed -- %s\n", e.what());
                        assert(false); // Should not fail
                }

                printf("have a look");
                //assert((con = pool.getConnection()));
                con = pool.getConnection();
                assert(con);
                try
                {
                        int i, j;
                        const char *bg[]= {"Starbuck", "Sharon Valerii",
                                "Number Six", "Gaius Baltar", "William Adama",
                                "Lee \"Apollo\" Adama", "Laura Roslin", 0};
                        PreparedStatement p = con.prepareStatement("insert into zild_t (name) values(?);");
                        /* If we did not get a statement, an SQLException is thrown
                           and we will not get here. So we can safely use the 
                           statement now. Likewise, below, we do not have to 
                           check return values from the statement since any error
                           will throw an SQLException and transfer the control
                           to the exception handler
                        */
                        for (i = 0, j = 42; bg[i]; i++, j++) {
                                p.setString(1, bg[i]);
                                p.execute();
                        }
                }
                catch(sql_exception& e)
                {
                        printf("\tResult: prepare statement failed -- %s\n", e.what());
                        assert(false);
                }

                try
                {
                        printf("\t\tBattlestar Galactica: \n");
                        ResultSet result = con.executeQuery("select name from zild_t where id > 12;");
                        while (result.next())
                            printf("\t\t%s\n", result.getString(1));
                }
                catch (sql_exception& e)
                {
                        printf("\tResult: resultset failed -- %s\n", e.what());
                       assert(false);
                }

                /* 
                 * The following should fail and throw exceptions. The exception error 
                 * message can be obtained with Exception_frame.message, or from 
                 * Connection_getLastError(con). Exception_frame.message contains both
                 * SQL errors or api errors such as prepared statement parameter index
                 * out of range, while Connection_getLastError(con) only has SQL errors
                 */
                try
                {
                        con = pool.getConnection();
                        assert(con);
                        con.execute(schema);
                        /* Creating the table again should fail and we 
                        should not come here */
                        printf("\tResult: Test failed -- exception not thrown\n");
                        exit(1);
                }
                catch (sql_exception& e)
                {
                        
                }

                try
                {
                        con = pool.getConnection();
                        assert(con);
                        printf("\tTesting: Query with errors.. ");
                        con.executeQuery("blablabala;");
                        printf("\tResult: Test failed -- exception not thrown\n");
                        exit(1);
                }
                catch (sql_exception& e)
                {
                        printf("ok\n");
                }

                try
                {
                        printf("\tTesting: Prepared statement query with errors.. ");
                        con = pool.getConnection();
                        assert(con);
                        PreparedStatement p = con.prepareStatement("blablabala;");
                        ResultSet r = p.executeQuery();
                        while(r.next());
                        printf("\tResult: Test failed -- exception not thrown\n");
                        exit(1);
                }
                catch (sql_exception& e)
                {
                        printf("ok\n");
                }

                try
                {
                        con = pool.getConnection();
                        assert(con);
                        printf("\tTesting: Column index out of range.. ");
                        ResultSet result = con.executeQuery("select id, name from zild_t;");
                        while (result.next()) {
                                int id = result.getInt(1);
                                const char *name = result.getString(2);
                                /* So far so good, now, try access an invalid
                                   column, which should throw an SQLException */
                                int bogus = result.getInt(3);
                                printf("\tResult: Test failed -- exception not thrown\n");
                                printf("%d, %s, %d", id, name, bogus);
                                exit(1);
                        }
                }
                catch (sql_exception& e)
                {
                        printf("ok\n");
                }

                try
                {
                        con = pool.getConnection();
                        assert(con);
                        printf("\tTesting: Invalid column name.. ");
                        ResultSet result = con.executeQuery("select name from zild_t;");
                        while (result.next()) {
                                const char *name = result.getStringByName("nonexistingcolumnname");
                                printf("%s", name);
                                printf("\tResult: Test failed -- exception not thrown\n");
                                exit(1);
                        }
                }
                catch (sql_exception& e)
                {
                        printf("ok\n");
                }

                try
                {
                        con = pool.getConnection();
                        assert(con);
                        PreparedStatement p = con.prepareStatement("update zild_t set name = ? where id = ?;");
                        printf("\tTesting: Parameter index out of range.. ");
                        p.setInt(3, 123);
                        printf("\tResult: Test failed -- exception not thrown\n");
                        exit(1);
                }
                catch (sql_exception& e)
                {
                        printf("ok\n");
                }

                con = pool.getConnection();
                assert(con);
                con.execute("drop table zild_t;");
        }
        printf("=> Test8: OK\n\n");


        printf("=> Test9: Ensure Capacity\n");
        {
                /* Check that MySQL ensureCapacity works for columns that exceed the preallocated buffer and that no truncation is done */
                if ( Str_startsWith(testURL, "mysql")) {
                        int myimagesize;
                        ConnectionPool pool(testURL);
                        assert(pool);
                        pool.start();
                        Connection con = pool.getConnection();
                        assert(con);
                        con.execute("CREATE TABLE zild_t(id INTEGER AUTO_INCREMENT PRIMARY KEY, image BLOB, string TEXT);");
                        PreparedStatement p = con.prepareStatement("insert into zild_t (image, string) values(?, ?);");
                        char t[4096];
                        memset(t, 'x', 4096);
                        t[4095] = 0;
                        for (int i = 0; i < 4; i++) {
                                p.setBlob(1, t, (i+1)*512); // store successive larger string-blobs to trigger realloc on ResultSet_getBlobByName
                                p.setString(2, t);
                                p.execute();
                        }
                        ResultSet r = con.executeQuery("select image, string from zild_t;");
                        for (int i = 0; r.next(); i++) {
                                r.getBlobByName("image", &myimagesize);
                                const char *image = r.getStringByName("image"); // Blob as image should be terminated
                                const char *string = r.getStringByName("string");
                                assert(myimagesize == (i+1)*512);
                                assert(strlen(image) == ((i+1)*512));
                                assert(strlen(string) == 4095);
                        }
                        p = con.prepareStatement("select image, string from zild_t;");
                        r = p.executeQuery();
                        for (int i = 0; r.next(); i++) {
                                r.getBlobByName("image", &myimagesize);
                                const char *image = r.getStringByName("image");
                                const char *string = (char*)r.getStringByName("string");
                                assert(myimagesize == (i+1)*512);
                                assert(strlen(image) == ((i+1)*512));
                                assert(strlen(string) == 4095);
                        }
                        con.execute("drop table zild_t;");
                }
        }
        printf("=> Test9: OK\n\n");
        
        printf("=> Test10: Date, Time, DateTime and Timestamp\n");
        {
            ConnectionPool pool(testURL);
            assert(pool);
            pool.start();
            Connection con = pool.getConnection();
            assert(con);
                if (Str_startsWith(testURL, "postgres"))
                        con.execute("create table zild_t(d date, t time, dt timestamp, ts timestamp)");
                else if (Str_startsWith(testURL, "oracle"))
                        con.execute("create table zild_t(d date, t time, dt date, ts timestamp)");
                else
                        con.execute("create table zild_t(d date, t time, dt datetime, ts timestamp)");
                PreparedStatement p = con.prepareStatement("insert into zild_t values (?, ?, ?, ?)");
                p.setString(1, "2013-12-28");
                p.setString(2, "10:12:42");
                p.setString(3, "2013-12-28 10:12:42");
                p.setTimestamp(4, 1387066378);
                p.execute();
                ResultSet r = con.executeQuery( "select * from zild_t");
                if (r.next()) {
                    struct tm date = r.getDateTime(1);
                    struct tm time = r.getDateTime(2);
                    struct tm datetime = r.getDateTime(3);
                    time_t timestamp = r.getTimestamp(4);
                    struct tm timestampAsTm = r.getDateTime(4);
                        // Check Date
                        assert(date.tm_hour == 0);
                        assert(date.tm_year == 2013);
                        assert(date.tm_mon == 11); // Remember month - 1
                        assert(date.tm_mday == 28);
                        assert(date.TM_GMTOFF == 0);
                        // Check Time
                        assert(time.tm_year == 0);
                        assert(time.tm_hour == 10);
                        assert(time.tm_min == 12);
                        assert(time.tm_sec == 42);
                        assert(time.TM_GMTOFF == 0);
                        // Check datetime
                        assert(datetime.tm_year == 2013);
                        assert(datetime.tm_mon == 11); // Remember month - 1
                        assert(datetime.tm_mday == 28);
                        assert(datetime.tm_hour == 10);
                        assert(datetime.tm_min == 12);
                        assert(datetime.tm_sec == 42);
                        assert(datetime.TM_GMTOFF == 0);
                        // Check timestamp
                        assert(timestamp == 1387066378);
                        // Check timestamp as datetime
                        assert(timestampAsTm.tm_year == 2013);
                        assert(timestampAsTm.tm_mon == 11); // Remember month - 1
                        assert(timestampAsTm.tm_mday == 15);
                        assert(timestampAsTm.tm_hour == 0);
                        assert(timestampAsTm.tm_min == 12);
                        assert(timestampAsTm.tm_sec == 58);
                        assert(timestampAsTm.TM_GMTOFF == 0);
                        // Result
                        printf("\tDate: %s, Time: %s, DateTime: %s\n\tTimestamp as numeric: %ld, Timestamp as string: %s\n",
                            r.getString(1),
                            r.getString(2),
                            r.getString(3),
                            r.getTimestamp(4),
                            r.getString(4)); // SQLite will show both as numeric
                }
                con.execute("drop table zild_t;");
        }
        printf("=> Test10: OK\n\n");

        printf("============> Connection Pool Tests: OK\n\n");
}

int main(void) {
        URL_T url;
        char buf[BSIZE];
        char *help = "Please enter a valid database connection URL and press ENTER\n"
                    "E.g. sqlite:///tmp/sqlite.db?synchronous=off&heap_limit=2000\n"
                    "E.g. mysql://localhost:3306/test?user=root&password=root\n"
                    "E.g. postgresql://localhost:5432/test?user=root&password=root\n"
                    "E.g. oracle://localhost:1526/test?user=scott&password=tiger\n"
                    "To exit, enter '.' on a single line\n\nConnection URL> ";
        ZBDEBUG = true;
        Exception_init();
        printf("============> Start Connection Pool Tests\n\n");
        printf("This test will create and drop a table called zild_t in the database\n");
	printf("%s", help);
	while (fgets(buf, BSIZE, stdin)) {
		if (*buf == '.')
                        break;
		if (*buf == '\r' || *buf == '\n' || *buf == 0) 
			goto next;
                url = URL_new(buf);
                if (! url) {
                        printf("Please enter a valid database URL or stop by entering '.'\n");
                        goto next;
                }
                testPool(URL_toString(url));
                URL_free(&url);
                printf("%s", help);
                continue;
next:
                printf("Connection URL> ");
	}
	return 0;
}
