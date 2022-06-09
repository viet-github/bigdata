val movies_rdd=sc.textFile("movie.dat")//load file movie.dat
val genres=movies_rdd.map(lines=>lines.split("::")(2))//tach cot (2) trong movie.dat thanh tung dong
val testing=genres.flatMap(line=>line.split('|'))// tach moi genre thanh 1 dong (1 movie co nhieu genre)
val genres_distinct_sorted=testing.distinct().sortBy(_(0))// chon nhung genre distinct va xep lai tu A-Z
genres_distinct_sorted.saveAsTextFile("List-Genres")// luu kq vao file
System.exit(0) // tat spark-shell