#                            Libzdbcpp

###Introduction libzdb

 Libzdb is a database library with thread-safe connection pooling. The
 library can connect transparently to multiple database systems. It has
 zero runtime configuration and connection is specified via a URL scheme.
 web site:     
    http://www.tildeslash.com/libzdb/         
    https://bitbucket.org/tildeslash/libzdb     


###Introduction libzdbcpp

 Libzdbcpp is c++ 11 wrapper class for libzdb.
 
 
###how to use libzdbcpp?
 after build libzdb, copy zdbcpp.h to the installed dir.
 ```cpp
 #include <zdbcpp.h>
 using namespace zdbcpp;
 ```
 
###demo code
 
-  fetch a query
```cpp
 ConnectionPool pool("mysql://192.168.11.100:3306/test?user=root&password=dba");
 Connection con = pool.getConnection();
 //we can set default prefetch to Connection
 //con.setDefaultRowPrefetch(100);
 
 //here, use c++ 11 variadic templates feature to bind parameter 
 ResultSet rset = con.executeQuery("select id, name, percent, image from zild_t where id < ? order by id;", 100);
 //row prefetch
 rset.setFetchSize(100);
 
 while (rset.next()) {
		int id = rset.getIntByName("id");
		const char *name = rset.getString(2);
		double percent = rset.getDoubleByName("percent");
		const char *blob = (char*)rset.getBlob(4, &imagesize);
		printf("\t%-5d%-16s%-10.2f%-16.38s\n", id, name ? name : "null", percent, imagesize ? blob : "");
 }
```
 
- insert data
```cpp
    char *data[]= {"Fry", "Leela", "Bender", "Farnsworth",
            "Zoidberg", "Amy", "Hermes", "Nibbler", "Cubert",
            "Zapp", "Joey Mousepad", "§Á¦²?", 0}; 
    
    ConnectionPool pool("mysql://192.168.11.100:3306/test?user=root&password=dba");
    Connection con = pool.getConnection();
    con.beginTransaction();
    /* Insert values into database and assume that auto increment of id works */
    long long affected_rows = 0;
    for (int i = 0; data[i]; i++)
		con.execute("insert into zild_t (name, percent) values(?, ?);", data[i], i + 1 );   
		
    con.commit();
```    
    
-  Prepared Statement
```cpp 
    char *images[]= {"Ceci n'est pas une pipe", "Mona Lisa",
            "Bryllup i Hardanger", "The Scream",
            "Vampyre", "Balcony", "Cycle", "Day & Night", 
            "Hand with Reflecting Sphere",
            "Drawing Hands", "Ascending and Descending", 0}; 
            
    ConnectionPool pool("mysql://192.168.11.100:3306/test?user=root&password=dba");
    pool.start();
    Connection con = pool.getConnection();
    
    PreparedStatement pre = con.prepareStatement("update zild_t set image=? where id=?;");
    for (int i = 0; images[i]; i++) {
        pre.setBlob(1, images[i], (int)strlen(images[i]) + 1);
        pre.setInt(2, i + 1);
        pre.execute();
    }
```    
    
- exception handling
```cpp 
    try
    {
        con = pool.getConnection();
        con.executeQuery("blablabala;");
    }
    catch (sql_exception& e)
    {
        printf(e.what());
    }
```


 for more document, please visit:     
    http://www.tildeslash.com/libzdb/#api   
    https://github.com/abc100m/libzdbcpp/blob/master/test_zdbcpp/test_zdbcpp.cpp    

