val ratingsRDD=sc.textFile("rating.dat") //load file rating.dat
val movies=ratingsRDD.map(line=>line.split("::")(1).toInt) // tach cot (1) thanh tu dong
val movies_pair=movies.map(mv=>(mv,1)) // gan 1 cho tung dong

val movies_count=movies_pair.reduceByKey((x,y)=>x+y) //count theo Key(MovieID) se ra duoc so luot rating
val movies_sorted=movies_count.sortBy(x=>x._2,false,1) //xep lai theo gia tri thu 2(so luot rating) theo thu tu giam dan

val mv_top10List=movies_sorted.take(10).toList // Tao list lay 10 gia tri dau tien trong movies_sorted
val mv_top10RDD=sc.parallelize(mv_top10List) // tao tep RDD chua 10 gia tri trong list

val mv_names=sc.textFile("movie.dat").map(line=>(line.split("::")(0).toInt,line.split("::")(1))) //load file movie.dat, tach cot (0) va cot (1) thanh tung dong
val join_out=mv_names.join(mv_top10RDD) // join theo movieID
join_out.sortBy(x=>x._2._2,false).map(x=> x._1+","+x._2._1+","+x._2._2).repartition(1).saveAsTextFile("Top-10-RATING") // dinh dang lai kq và xep theo so luot rating
System.exit(0) // tat spark-shell