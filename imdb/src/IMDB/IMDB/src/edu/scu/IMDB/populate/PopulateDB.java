package edu.scu.IMDB.populate;

import static edu.scu.IMDB.framework.jdbcConnection.getConnection;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PopulateDB {

    public static void main(String args[]) {
        //String argss = "movies.dat";
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement pst = null;
        String sql = null;

        FileInputStream fis = null;
        BufferedReader reader = null;
        int batchsize = 100;

        String MOVIEACTORDAT = "movie_actors.dat";
        String MOVIECOUNTRYDAT = "movie_countries.dat";
        String MOVIEDIRECTORDAT = "movie_directors.dat";
        String MOVIEGENREDAT = "movie_genres.dat";
        String MOVIETAGSDAT = "movie_tags.dat";
        String MOVIEDAT = "movies.dat";
        String TAGSDAT = "tags.dat";
        String USERRATEMOVIEDAT = "user_ratedmovies.dat";
        String USERTAGMOVIEDAT = "user_taggedmovies.dat";

        String MOVIEACTORTABLE = "MOVIEACTOR";
        String MOVIECOUNTRYTABLE = "MOVIECOUNTRY";
        String MOVIEDIRECTORTABLE = "MOVIEDIRECTOR";
        String MOVIEGENRETABLE = "MOVIEGENRE";
        String MOVIETAGSTABLE = "MOVIETAGS";
        String MOVIETABLE = "MOVIE";
        String TAGSTABLE = "TAGS";
        String USERRATEMOVIETABLE = "USERRATEMOVIE";
        String USERTAGMOVIETABLE = "USERTAGMOVIE";

        try {

            String tableName = null;
            
            for (int i = 0; i < args.length ; i++) {
                if (args[i].contains(MOVIEDAT)) {
                    if (args[i].contains(USERRATEMOVIEDAT)) 
                        tableName = USERRATEMOVIETABLE;
                    else if (args[i].contains(USERTAGMOVIEDAT)) 
                        tableName = USERTAGMOVIETABLE;
                    else
                        tableName = MOVIETABLE;
                } else if (args[i].contains(MOVIEACTORDAT)) {
                    tableName = MOVIEACTORTABLE;
                } else if (args[i].contains(MOVIECOUNTRYDAT)) {
                    tableName = MOVIECOUNTRYTABLE;
                } else if (args[i].contains(MOVIEDIRECTORDAT)) {
                    tableName = MOVIEDIRECTORTABLE;
                } else if (args[i].contains(MOVIEGENREDAT)) {
                    tableName = MOVIEGENRETABLE;
                } else if (args[i].contains(MOVIETAGSDAT)) {
                    tableName = MOVIETAGSTABLE;
                } else if (args[i].contains(TAGSDAT)) {
                    tableName = TAGSTABLE;
                } else {
                    continue;
                }
                
                fis = new FileInputStream(args[i]);
                reader = new BufferedReader(new InputStreamReader(fis));                

                conn = getConnection();

                sql = "truncate table " + tableName;
                pst = conn.prepareStatement(sql);
                pst.execute();
                System.out.println("Table " + tableName + " truncated");
                    
                if (tableName.equals(MOVIETABLE)) {
                    sql = "insert into " + MOVIETABLE + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                } else if (tableName.equals(MOVIEACTORTABLE)) {
                    sql = "insert into " + MOVIEACTORTABLE + " values (?,?,?,?)";
                } else if (tableName.equals(MOVIEDIRECTORTABLE)) {
                    sql = "insert into " + MOVIEDIRECTORTABLE + " values (?,?,?)";
                } else if (tableName.equals(MOVIEGENRETABLE)) {
                    sql = "insert into " + MOVIEGENRETABLE + " values (?,?)";
                } else if (tableName.equals(MOVIECOUNTRYTABLE)) {
                    sql = "insert into " + MOVIECOUNTRYTABLE + " values (?,?)";
                } else if (tableName.equals(MOVIETAGSTABLE)) {
                    sql = "insert into " + MOVIETAGSTABLE + " values (?,?,?)";
                } else if (tableName.equals(TAGSTABLE)) {
                    sql = "insert into " + TAGSTABLE + " values (?,?)";
                } else if (tableName.equals(USERRATEMOVIETABLE)) {
                    sql = "insert into " + USERRATEMOVIETABLE + " values (?,?,?,?,?,?,?,?,?)";
                } else if (tableName.equals(USERTAGMOVIETABLE)) {
                    sql = "insert into " + USERTAGMOVIETABLE + " values (?,?,?,?,?,?,?,?,?)";
                }

                pst = conn.prepareStatement(sql);

                String line = reader.readLine();

                //Skipping header and read next line
                line = reader.readLine();
                int numRecords = 0;

                while (line != null) {
                    numRecords++;
                    String cvalues[] = line.toString().trim().split("	");
                    if (tableName.equals(MOVIETABLE)) {
                        pst.setInt(1, Integer.parseInt(cvalues[0]));
                        pst.setString(2, cvalues[1]);
                        pst.setInt(3, Integer.parseInt(cvalues[2]));
                        pst.setString(4, cvalues[3]);
                        pst.setString(5, cvalues[4]);
                        pst.setInt(6, Integer.parseInt(cvalues[5]));
                        pst.setString(7, cvalues[6]);
                        pst.setFloat(8, Float.valueOf(cvalues[7]));
                        pst.setFloat(9, Float.valueOf(cvalues[8]));
                        pst.setFloat(10, Float.valueOf(cvalues[9]));
                        pst.setFloat(11, Float.valueOf(cvalues[10]));
                        pst.setFloat(12, Float.valueOf(cvalues[11]));
                        pst.setFloat(13, Float.valueOf(cvalues[12]));
                        pst.setFloat(14, Float.valueOf(cvalues[13]));
                        pst.setFloat(15, Float.valueOf(cvalues[14]));
                        pst.setFloat(16, Float.valueOf(cvalues[15]));
                        pst.setFloat(17, Float.valueOf(cvalues[16]));
                        pst.setFloat(18, Float.valueOf(cvalues[17]));
                        pst.setFloat(19, Float.valueOf(cvalues[18]));
                        pst.setFloat(20, Float.valueOf(cvalues[19]));
                        pst.setString(21, cvalues[20]);
                    } else if (tableName.equals(MOVIEACTORTABLE)) {
                        pst.setInt(1, Integer.parseInt(cvalues[0]));
                        pst.setString(2, cvalues[1]);
                        if (cvalues[2].isEmpty())
                            pst.setString(3, " ");
                        else
                            pst.setString(3, cvalues[2]);                        
                        pst.setInt(4, Integer.parseInt(cvalues[3]));                       
                    } else if (tableName.equals(MOVIEDIRECTORTABLE)) {
                        pst.setInt(1, Integer.parseInt(cvalues[0]));
                        pst.setString(2, cvalues[1]);                        
                        pst.setString(3, cvalues[2]);                      
                    } else if (tableName.equals(MOVIEGENRETABLE)) {
                        pst.setInt(1, Integer.parseInt(cvalues[0]));
                        pst.setString(2, cvalues[1]);                     
                    } else if (tableName.equals(MOVIECOUNTRYTABLE)) {
                        pst.setInt(1, Integer.parseInt(cvalues[0]));
                        if (cvalues.length == 1) 
                            pst.setString(2, " "); 
                        else 
                            pst.setString(2, cvalues[1]);                      
                    } else if (tableName.equals(MOVIETAGSTABLE)) {
                        pst.setInt(1, Integer.parseInt(cvalues[0]));
                        pst.setInt(2, Integer.parseInt(cvalues[1]));
                        pst.setInt(3, Integer.parseInt(cvalues[2]));                      
                    } else if (tableName.equals(TAGSTABLE)) {
                        pst.setInt(1, Integer.parseInt(cvalues[0]));
                       pst.setString(2, cvalues[1]);
                    } else if (tableName.equals(USERRATEMOVIETABLE)) {
                        pst.setInt(1, Integer.parseInt(cvalues[0]));
                        pst.setInt(2, Integer.parseInt(cvalues[1]));
                        pst.setFloat(3, Float.valueOf(cvalues[2]));                        
                        pst.setInt(4, Integer.parseInt(cvalues[3]));
                        pst.setInt(5, Integer.parseInt(cvalues[4]));
                        pst.setInt(6, Integer.parseInt(cvalues[5]));
                        pst.setInt(7, Integer.parseInt(cvalues[6]));
                        pst.setInt(8, Integer.parseInt(cvalues[7]));
                        pst.setInt(9, Integer.parseInt(cvalues[8]));                       
                    } else if (tableName.equals(USERTAGMOVIETABLE)) {
                        pst.setInt(1, Integer.parseInt(cvalues[0]));
                        pst.setInt(2, Integer.parseInt(cvalues[1]));
                        pst.setInt(3, Integer.parseInt(cvalues[2]));
                        pst.setInt(4, Integer.parseInt(cvalues[3]));
                        pst.setInt(5, Integer.parseInt(cvalues[4]));
                        pst.setInt(6, Integer.parseInt(cvalues[5]));
                        pst.setInt(7, Integer.parseInt(cvalues[6]));
                        pst.setInt(8, Integer.parseInt(cvalues[7]));
                        pst.setInt(9, Integer.parseInt(cvalues[8])); 
                    }

                    pst.addBatch();

                    if (numRecords % batchsize == 0) {
                        pst.executeBatch();
                        System.out.println("Inserted numRecords = " + numRecords + " from file " + args[i]);
                    }

                    line = reader.readLine();
                }

                pst.executeBatch();
                System.out.println("Inserted numRecords = " + numRecords + " from file " + args [i]);
                reader.close();
                fis.close();
                conn.close();
            } 

        } catch (FileNotFoundException ex) {
            Logger.getLogger(PopulateDB.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(PopulateDB.class.getName()).log(Level.SEVERE, null, ex);

        } catch (SQLException ex) {
            Logger.getLogger(PopulateDB.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(PopulateDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
