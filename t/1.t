# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl 1.t'


# perldoc Test::More - for help writing this test script.
use Test::More tests => 9;
BEGIN { use_ok('DBI'); use_ok('XML::RDB') };
chdir('t');

my %drivers = map { $_ => 1 } DBI->available_drivers;
ok(($drivers{SQLite} || $drivers{mysql} || $drivers{Pg}), 'DBDs Check: SQLite, MySQL or PostgreSQL');

my $rdb = ( $drivers{SQLite} && (new XML::RDB(config_file => 'dbi_sqlite3_test.cfg')))
       || ( $drivers{Pg}     && (new XML::RDB(config_file => 'dbi_pg_test.cfg'     )))
       || ( $drivers{mysql}  && (new XML::RDB(config_file => 'dbi_mysql_test.cfg'  )));

my ($why, $test_cnt) = ('Unable to Create DB connection with XML::RDB new.', 6 );
SKIP: {
  skip $why, $test_cnt, unless (ref $rdb); 
  
# TODO : fix the API, get the real dbname to return.  
  $rdb->drop_tables; # 'DB, dropped tables');
  ok( $rdb,                                             "XML::RDB created and connected; DB name 'test'");
  ok( $rdb->make_tables('test.xml', 'test_schema.sql'), 'Generated DB DDL schema.');
  ok( $rdb->create_tables('test_schema.sql'),           'DB loaded DDL');
  ok( $rdb->populate_tables('test.xml'),                'DB loaded XML');
  ok( $rdb->unpopulate_tables('test_new.xml'),          'Generated XML from DB');
  ok( $rdb->drop_tables,                                'DB dropped tables');
  unlink('test_schema.sql');
  unlink('test_new.xml');
  unlink('test');
}
done_testing();

