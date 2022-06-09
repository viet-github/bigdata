val movies_rdd=sc.textFile("movie.dat")//load file movie.dat
val movie_nm=movies_rdd.map(lines=>lines.split("::")(1))//tach cot (1) trong movie.dat thanh tung dong
val year=movie_nm.map(lines=>lines.substring(lines.lastIndexOf("(")+1,lines.lastIndexOf(")")))//tach nam trong tilte
val latest=year.max // chon ra nam gan day nhat
val latest_movies=movie_nm.filter(lines=>lines.contains("("+latest+")")).saveAsTextFile("Latest-Movie")//loc cac movie co nam gan nhat va luu kq
System.exit(0) // tat spark-shell