package org.abratuhi.mqg;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class QueryGeneratorTest {
	private static final Logger LOG = Logger.getLogger(QueryGeneratorTest.class);
	
	private QueryGenerator qg = new QueryGenerator();

    private Table tArtist = new Table(null, "Artist");
    private Table tTrack = new Table(null, "Track");
    private Table tInvoice = new Table(null, "Invoice");

    @BeforeClass
    public static void setUpClass() throws ClassNotFoundException {
        Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout("%d{ISO8601} %-5p [%t] %c: %m%n")));
        Logger.getRootLogger().setLevel(Level.ALL);
    }

    @Before
    public void setUp() throws SQLException, ClassNotFoundException {
        JdbcMetaRelatedTableProvider provider = new JdbcMetaRelatedTableProvider();
        provider.setDriver("org.sqlite.JDBC");
        provider.setUrl("jdbc:sqlite:src/test/resources/chinook.sqlite");
        provider.init();
        qg.setProvider(provider);
    }

    @Test
    public void testJoinArtistTrack() throws SqlGeneratorException {
        QueryGenerator.Path path = qg.visit(tArtist, tTrack);
        assertNotNull(path);
        assertEquals("Artist -> Album -> Track", path.toString());
    }

    @Test
    public void testJoinTrackArtist() throws SqlGeneratorException {
        QueryGenerator.Path path = qg.visit(tTrack, tArtist);
        assertNotNull(path);
        assertEquals("Track -> Album -> Artist", path.toString());
    }

    @Test
    public void testJoinInvoiceTrack() throws SqlGeneratorException {
        QueryGenerator.Path path = qg.visit(tInvoice, tTrack);
        assertNotNull(path);
        assertEquals("Invoice -> InvoiceLine -> Track", path.toString());
    }

    @Test
    public void testJoinTrackInvoice() throws SqlGeneratorException {
        QueryGenerator.Path path = qg.visit(tTrack, tInvoice);
        assertNotNull(path);
        assertEquals("Track -> InvoiceLine -> Invoice", path.toString());
    }

    @Test
    public void testToSqlSimpleQuery() throws SqlGeneratorException {
        Predicate<String> p1 = new Predicate<>(Operator.EQUALS, tArtist, "name", "a");

        Query query = new Query(Junctor.AND);
        query.addQueryTerm(p1);

        String sql = qg.toSql(query);

        assertNotNull(sql);
        assertEquals("SELECT *  FROM Artist null_dot_artist WHERE ((null_dot_artist.name = 'a'))", sql);
    }

    @Test
    public void testToSqlSimpleTwoTermQuery() throws SqlGeneratorException {
        Predicate<String> p1 = new Predicate<>(Operator.EQUALS, tArtist, "name", "a");
        Predicate<String> p2 = new Predicate<>(Operator.EQUALS, tArtist, "name", "b");

        Query query = new Query(Junctor.OR);
        query.addQueryTerm(p1);
        query.addQueryTerm(p2);

        String sql = qg.toSql(query);

        assertNotNull(sql);
        assertEquals("SELECT *  FROM Artist null_dot_artist WHERE ((null_dot_artist.name = 'a') OR (null_dot_artist.name = 'b'))", sql);
    }

    @Test
    public void testToSqlSimpleNestedQuery() throws SqlGeneratorException {
        Predicate<String> p1 = new Predicate<>(Operator.EQUALS, tArtist, "name", "a");

        Query query1 = new Query(Junctor.AND);
        query1.addQueryTerm(p1);

        Query query = new Query(Junctor.AND);
        query.addQueryTerm(query1);

        String sql = qg.toSql(query);
        assertNotNull(sql);
        assertEquals("SELECT *  FROM Artist null_dot_artist WHERE (((null_dot_artist.name = 'a')))", sql);
    }

    @Test
    public void testPath2JoinTrackInvoice() throws SqlGeneratorException {
        QueryGenerator.Path joinPath = qg.visit(tTrack, tInvoice);
        Query joinQuery = qg.path2join(joinPath);
        assertNotNull(joinQuery);
        assertEquals("((null_dot_invoiceline.TrackId = null_dot_track.TrackId) AND (null_dot_invoiceline.InvoiceId = null_dot_invoice.InvoiceId))", joinQuery.toSql());
    }


    @Test
    public void testToSqlSimpleTwoTermQueryWithJoinArtistTrack() throws SqlGeneratorException {
        Predicate<String> p1 = new Predicate<>(Operator.EQUALS, tArtist, "name", "a");
        Predicate<Integer> p2 = new Predicate<>(Operator.EQUALS, tTrack, "trackid", 5);

        Query query = new Query(Junctor.OR);
        query.addQueryTerm(p1);
        query.addQueryTerm(p2);

        String sql = qg.toSql(query);

        assertNotNull(sql);
        assertEquals("SELECT *  FROM Artist null_dot_artist, Track null_dot_track, Album null_dot_album WHERE ((null_dot_artist.name = 'a') OR (null_dot_track.trackid = 5)) AND ((null_dot_track.AlbumId = null_dot_album.AlbumId) AND (null_dot_album.ArtistId = null_dot_artist.ArtistId))", sql);
    }

    @Test
    public void testToSqlSimpleQueryWithOneProjectile() throws SqlGeneratorException {
        Predicate<String> p1 = new Predicate<>(Operator.EQUALS, tArtist, "name", "a");

        Query query = new Query(Junctor.AND);
        query.addQueryTerm(p1);
        query.addProjectile(tArtist, "name");

        String sql = qg.toSql(query);

        assertNotNull(sql);
        assertEquals("SELECT null_dot_artist.name FROM Artist null_dot_artist WHERE ((null_dot_artist.name = 'a'))", sql);
    }

    @Test
    public void testToSqlSimpleQueryWithTwoProjectile() throws SqlGeneratorException {
        Predicate<String> p1 = new Predicate<>(Operator.EQUALS, tArtist, "name", "a");

        Query query = new Query(Junctor.AND);
        query.addQueryTerm(p1);
        query.addProjectile(tArtist, "artistid");
        query.addProjectile(tArtist, "name");

        String sql = qg.toSql(query);

        assertNotNull(sql);
        assertEquals("SELECT null_dot_artist.artistid, null_dot_artist.name FROM Artist null_dot_artist WHERE ((null_dot_artist.name = 'a'))", sql);
    }

	
}
