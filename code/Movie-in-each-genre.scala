val movies_rdd=sc.textFile("movie.dat")//load file movie.dat
val genre=movies_rdd.map(lines=>lines.split("::")(2))//tach cot (2) trong movie.dat thanh tung dong
val flat_genre=genre.flatMap(lines=>lines.split('|'))// tach moi genre thanh 1 dong (1 movie co nhieu genre)
val genre_kv=flat_genre.map(k=>(k,1))// gan 1 cho tung genre
val genre_count=genre_kv.reduceByKey((k,v)=>(k+v))// count theo genre
val genre_sort= genre_count.sortByKey()//xep lại theo genre
genre_sort.saveAsTextFile("Movie-in-each-genre1")// luu kq
System.exit(0)// tat spark-shell