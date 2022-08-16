package ceng.ceng351.cengtubedb;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public  class CengTubeDB implements ICengTubeDB{
    private Connection connection;
    @Override

    public void initialize(){
        try{
            connection=DriverManager.getConnection("jdbc:mysql://144.122.71.168:3306/db231050?useSSL=false", "e231050", "6ebfb85a" );

        }
        catch (SQLException throwables){
            throwables.printStackTrace();
        }


    }
    @Override
    public int createTables(){
        int count=0;
        try(Statement stmt = connection.createStatement()){
            String ent1 = "CREATE TABLE User  "+
                    "(userID INTEGER NOT NULL,"+
                    "userName VARCHAR(30),"+
                    "email VARCHAR(30),"+
                    "password VARCHAR(30),"+
                    "status VARCHAR(15),"+
                    "PRIMARY KEY (userID))";
            String ent2 = "CREATE TABLE Video "
                    +" (videoID INTEGER,"
                    +"userID INTEGER,"
                    +"videoTitle VARCHAR(60),"
                    +"likeCount INTEGER,"
                    +"dislikeCount INTEGER,"
                    +"datePublished DATE,"
                    +"PRIMARY KEY (videoID),"
                    +" FOREIGN KEY (userID) REFERENCES User (userID) ON DELETE CASCADE )";

            String ent3 = "CREATE TABLE Comment  "
                    +"(userID INTEGER,"
                    +"videoID INTEGER,"
                    +"commentID INTEGER,"
                    +"dateCommented DATE,"
                    +"PRIMARY KEY(commentID),"
                    + "commentText VARCHAR(1000),"
                    +" FOREIGN KEY (userID) REFERENCES User (userID) ON DELETE CASCADE ,"
                    +" FOREIGN KEY (videoID) REFERENCES Video (videoID) ON DELETE CASCADE)";
            String ent4= "CREATE TABLE Watch  "
                        +"(userID INTEGER NOT NULL,"
                        +"videoID INTEGER NOT NULL,"
                        + "datewatched DATE,"
                        +"PRIMARY KEY(userID,videoID),"
                        +" FOREIGN KEY (userID) REFERENCES User (userID) ON DELETE CASCADE ,"
                        +" FOREIGN KEY (videoID) REFERENCES Video (videoID) ON DELETE CASCADE)";

            stmt.executeUpdate(ent1);
            count++;
            stmt.executeUpdate(ent2);
            count++;
            stmt.executeUpdate(ent3);
            count++;
            stmt.executeUpdate(ent4);
            count++;

        }
        catch (SQLException throwables){
            throwables.printStackTrace();
        }
        return count;
    }
    @Override
    public int dropTables(){
        int count2=0;
        try{
            Statement stmt = connection.createStatement();
            String dropu = "DROP TABLE User";

            String dropv = "DROP TABLE Video";

            String dropw = "DROP TABLE Watch";
            String dropc = "DROP TABLE Comment";

            stmt.executeUpdate(dropc);
            count2++;
            stmt.executeUpdate(dropw);
            count2++;
            stmt.executeUpdate(dropv);
            count2++;
            stmt.executeUpdate(dropu);
            count2++;
        }
        catch (SQLException throwables){
            throwables.printStackTrace();

        }
        return count2;
    }
    @Override
    public int insertUser(User[] users){
        int count=0;
        try{
            String instance="insert into User(UserID,userName,email,password,status)"
                    +"values(?,?,?,?,?)";
            for (int i=0;i<users.length;i++){
                PreparedStatement stmt =connection.prepareStatement(instance);
                stmt.setInt( 1, users[i].userID);
                stmt.setString( 2, users[i].userName);
                stmt.setString( 3, users[i].email);
                stmt.setString( 4, users[i].password);
                stmt.setString( 5, users[i].status);

                stmt.executeUpdate();
                count++;
            }

        }
        catch (Exception throwables){
            throwables.printStackTrace();
        }
        return count;

    }
    @Override
    public int insertVideo(Video[] videos){
        int count=0;
        try {
            String instance = "insert into Video(videoID, userID, videoTitle, likeCount, dislikeCount, datePublished)"
                    + "values(?,?,?,?,?,?)";

            for (int i = 0; i < videos.length; i++) {
                PreparedStatement stmt = connection.prepareStatement(instance);
                stmt.setInt(1, videos[i].videoID);
                stmt.setInt(2, videos[i].userID);
                stmt.setString(3, videos[i].videoTitle);
                stmt.setInt(4, videos[i].likeCount);
                stmt.setInt(5, videos[i].dislikeCount);
                stmt.setString(6,videos[i].datePublished);

                stmt.executeUpdate();
                count++;
            }
        }
        catch (Exception throwables){
            throwables.printStackTrace();
        }
        return count;

    }
    @Override
    public int insertComment(Comment[] comments){
        int count=0;
        try {
            String instance = "insert into Comment(commentID, userID, videoID, commentText, dateCommented)"
                    + "values(?,?,?,?,?)";

            for (int i = 0; i < comments.length; i++) {
                PreparedStatement stmt = connection.prepareStatement(instance);
                stmt.setInt(1, comments[i].commentID);
                stmt.setInt(2, comments[i].userID);
                stmt.setInt(3, comments[i].videoID);
                stmt.setString(4, comments[i].commentText);
                stmt.setString(5, comments[i].dateCommented);


                stmt.executeUpdate();
                count++;
            }
        }
        catch (Exception throwables){
            throwables.printStackTrace();
        }
        return count;

    }
    @Override
    public int insertWatch(Watch[] watchEntries){
        int count=0;
        try {
            String instance = "insert into Watch(userID, videoID, dateWatched)"
                    + "values(?,?,?)";

            for (int i = 0; i < watchEntries.length; i++) {
                PreparedStatement stmt = connection.prepareStatement(instance);
                stmt.setInt(1, watchEntries[i].userID);
                stmt.setInt(2, watchEntries[i].videoID);
                stmt.setString(3, watchEntries[i].dateWatched);


                stmt.executeUpdate();
                count++;
            }
        }
        catch (Exception throwables){
            throwables.printStackTrace();
        }
        return count;


    }

    @Override
    public QueryResult.VideoTitleLikeCountDislikeCountResult[] question3(){
        ArrayList <QueryResult.VideoTitleLikeCountDislikeCountResult> mylist=new ArrayList<QueryResult.VideoTitleLikeCountDislikeCountResult>();
        try(Statement stmt=connection.createStatement()){
            String query= "SELECT videoTitle,likeCount,dislikeCount "
                    +"FROM Video "
                    +"WHERE likeCount>dislikeCount "+
                    "ORDER BY videoTitle ASC " ;
            ResultSet rs=stmt.executeQuery(query);
            while (rs.next()){
                QueryResult.VideoTitleLikeCountDislikeCountResult made=new QueryResult.VideoTitleLikeCountDislikeCountResult(rs.getString("videoTitle"),rs.getInt("likeCount"),rs.getInt("dislikeCount"));

                mylist.add(made);

            }

            QueryResult.VideoTitleLikeCountDislikeCountResult[] myArray = new QueryResult.VideoTitleLikeCountDislikeCountResult[mylist.size()];
            mylist.toArray(myArray);

        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return mylist.toArray(new QueryResult.VideoTitleLikeCountDislikeCountResult[0]);




    }
    @Override
    public QueryResult.VideoTitleUserNameCommentTextResult[] question4(Integer userID){
        ArrayList <QueryResult.VideoTitleUserNameCommentTextResult> mylist=new ArrayList<QueryResult.VideoTitleUserNameCommentTextResult>();

        try(Statement stmt=connection.createStatement()) {




            String query = "SELECT V.videoTitle,U.userName,C1.commentText "
                    + "FROM Video V,Comment C1 ,User U "
                    + "WHERE  U.userID= " +userID.toString()  + " AND V.videoID=C1.videoID AND C1.userID=U.userID "
                    + "ORDER BY videoTitle ASC ";
            ResultSet rs=stmt.executeQuery(query);
            while (rs.next()){
                QueryResult.VideoTitleUserNameCommentTextResult made=new QueryResult.VideoTitleUserNameCommentTextResult(rs.getString("videoTitle"),rs.getString("userName"),rs.getString("commentText"));

                mylist.add(made);

            }

        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return mylist.toArray(new QueryResult.VideoTitleUserNameCommentTextResult[0]);

    }

    @Override
    public QueryResult.VideoTitleUserNameDatePublishedResult[] question5(Integer userID) {
        ArrayList <QueryResult.VideoTitleUserNameDatePublishedResult> mylist=new ArrayList<QueryResult.VideoTitleUserNameDatePublishedResult>();

        try(Statement stmt=connection.createStatement()) {




            String query = "SELECT V.videoTitle,U.userName,V.datePublished "
                    + "FROM Video V ,User U "
                    + "WHERE V.userID ="+ userID.toString()+
                                        " AND V.datePublished =  (SELECT MIN(V5.datePublished) "+
                                                                "FROM Video V5"+
                                                                    " WHERE V5.videoTitle NOT LIKE '%VLOG%' AND " +
                    "V5.userID= "+ userID.toString()+
                    " )AND U.userID=" +userID.toString() +


                     " ORDER BY videoTitle ASC ";
            ResultSet rs=stmt.executeQuery(query);
            while (rs.next()){
                QueryResult.VideoTitleUserNameDatePublishedResult made=new QueryResult.VideoTitleUserNameDatePublishedResult(rs.getString("videoTitle"),rs.getString("userName"),rs.getString("datePublished"));

                mylist.add(made);

            }

        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return mylist.toArray(new QueryResult.VideoTitleUserNameDatePublishedResult[0]);





    }
    @Override
    public QueryResult.VideoTitleUserNameNumOfWatchResult[] question6(String dateStart, String dateEnd) {
        ArrayList <QueryResult.VideoTitleUserNameNumOfWatchResult> mylist=new ArrayList<QueryResult.VideoTitleUserNameNumOfWatchResult>();
        try(Statement stmt=connection.createStatement()){
            String query= "SELECT V.videoTitle,U.userName,COUNT(*) AS viewcount "+
                        "FROM Video V,User U" +
                     "   WHERE  U.userID=V.userID AND V.userID IN (SELECT V1.userID "+
                                                                    " FROM Video V1" +
                                                                    " WHERE V1.datePublished<= " + dateEnd + " AND V1.datePublished>=" + dateStart +

                            " )GROUP BY V.videoID"+


                    " ORDER BY viewcount DESC";


            ResultSet rs=stmt.executeQuery(query);
            while (rs.next()){
                QueryResult.VideoTitleUserNameNumOfWatchResult made=new QueryResult.VideoTitleUserNameNumOfWatchResult(rs.getString("videoTitle"),rs.getString("userName"),rs.getInt("viewcount"));

                mylist.add(made);

            }

            QueryResult.VideoTitleUserNameNumOfWatchResult[] myArray = new QueryResult.VideoTitleUserNameNumOfWatchResult[mylist.size()];
            mylist.toArray(myArray);

        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return mylist.toArray(new QueryResult.VideoTitleUserNameNumOfWatchResult[0]);






    }
    @Override
    public QueryResult.UserIDUserNameNumOfVideosWatchedResult[] question7() {
        ArrayList <QueryResult.UserIDUserNameNumOfVideosWatchedResult> mylist=new ArrayList<QueryResult.UserIDUserNameNumOfVideosWatchedResult>();
        try(Statement stmt=connection.createStatement()){
            String query= "SELECT  U.userID,U.userName,COUNT(*) AS viewnumber "
                    +"FROM User U,Watch W  "
                    +" WHERE W.userID=U.userID AND W.videoID IN (SELECT W1.videoID"+
                                                                " FROM Watch W1,User U2"+
                                                                " WHERE U2.userID=W1.userID"+
                                                                " GROUP BY W1.videoID"+
                                                                " HAVING COUNT(*) =1 )"+
                    "   GROUP BY U.userID"+



                    " ORDER BY userID ASC";


            ResultSet rs=stmt.executeQuery(query);
            while (rs.next()){
                QueryResult.UserIDUserNameNumOfVideosWatchedResult made=new QueryResult.UserIDUserNameNumOfVideosWatchedResult(rs.getInt("userID"),rs.getString("userName"),rs.getInt("viewnumber"));

                mylist.add(made);

            }

            QueryResult.UserIDUserNameNumOfVideosWatchedResult[] myArray = new QueryResult.UserIDUserNameNumOfVideosWatchedResult[mylist.size()];
            mylist.toArray(myArray);

        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return mylist.toArray(new QueryResult.UserIDUserNameNumOfVideosWatchedResult[0]);







    }
    @Override
    public QueryResult.UserIDUserNameEmailResult[]  question8() {
        ArrayList <QueryResult.UserIDUserNameEmailResult> mylist=new ArrayList<QueryResult.UserIDUserNameEmailResult>();
        try(Statement stmt=connection.createStatement()){
            String query= "SELECT  U.userID,U.userName,U.email "
                    +"FROM User U  "
                    +"WHERE U.userID IN (SELECT U.userID"+
                                        " FROM Comment C, Watch W , (SELECT U.userID "+
                                                "FROM User U ) AS allvideos"+

                                        " WHERE C.userID=allvideos.userID AND W.userID=allvideos.userID AND U.userID=allvideos.userID )"+




                    " ORDER BY userID ASC";


            ResultSet rs=stmt.executeQuery(query);
            while (rs.next()){
                QueryResult.UserIDUserNameEmailResult made=new QueryResult.UserIDUserNameEmailResult(rs.getInt("userID"),rs.getString("userName"),rs.getString("email"));

                mylist.add(made);

            }

            QueryResult.UserIDUserNameEmailResult[] myArray = new QueryResult.UserIDUserNameEmailResult[mylist.size()];
            mylist.toArray(myArray);

        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return mylist.toArray(new QueryResult.UserIDUserNameEmailResult[0]);



    }
    @Override
    public QueryResult.UserIDUserNameEmailResult[]  question9() {
        ArrayList <QueryResult.UserIDUserNameEmailResult> mylist=new ArrayList<QueryResult.UserIDUserNameEmailResult>();

        try(Statement stmt=connection.createStatement()) {




            String query = "SELECT U.userID, U.userName, U.email"
                    + " FROM User U "
                    + "WHERE U.userID IN(SELECT U2.userID"+
                    " FROM User U2,Watch W1"+ " WHERE U2.userID=W1.userID) "+
                    "AND U.userID NOT IN (SELECT U3.userID"+" FROM User U3,Comment C1 "+ "WHERE U3.userID=C1.userID)"+
                    " ORDER BY userID ASC";
            ResultSet rs=stmt.executeQuery(query);
            while (rs.next()){
                QueryResult.UserIDUserNameEmailResult made=new QueryResult.UserIDUserNameEmailResult(rs.getInt("userID"),rs.getString("userName"),rs.getString("email"));

                mylist.add(made);

            }

        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return mylist.toArray(new QueryResult.UserIDUserNameEmailResult[0]);


    }

    @Override
    public int question10(int givenViewCount){
        int count=0;

        try{
            String query= "UPDATE User U SET U.status ='verified'"+
                            "WHERE U.userID IN (SELECT U.userID" +
                                                " FROM Watch W,Video V"+
                                                 " WHERE V.userID=U.userID AND V.videoID=W.videoID "+
                                                   "GROUP BY U.userID" +
                                                    " HAVING COUNT(*) >" + givenViewCount +")"  ;

            PreparedStatement stmt=connection.prepareStatement(query);
            count=stmt.executeUpdate();



        }
        catch (SQLException e){
            e.printStackTrace();;
        }
        return count;



    }
    @Override
    public int question11(Integer videoID, String newTitle){
        int count=0;

        try{
            PreparedStatement query = connection.prepareStatement( "update Video set videoTitle = 'newTitle'  where videoID = '"+videoID+"'");


            count=query.executeUpdate();


        }
        catch (SQLException e){
            e.printStackTrace();;
        }
        return count;



    }

    @Override
    public int question12(String videoTitle){
        int count=0;

        try{
            String query="DELETE FROM Video WHERE videoTitle ='" + videoTitle + "';";

            PreparedStatement stmt=connection.prepareStatement(query);
            count=stmt.executeUpdate();


        }
        catch (SQLException e){
            e.printStackTrace();;
        }
        return count;

    }




}
