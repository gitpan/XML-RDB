# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl 1.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test::More tests => 27;
BEGIN { use_ok('DBI'); use_ok('XML::RDB') };

chdir('t');

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

my %drivers = map { $_ => 1 } DBI->available_drivers;
ok($drivers{mysql} || $drivers{Pg}, "Checking for MySQL or PostgreSQL DBDs");
pass("Checking for MySQL or PostgreSQL DBDs");

my $rdb = $drivers{mysql} && (new XML::RDB(config_file => 'mysql_test_config'))
    || $drivers{Pg} && (new XML::RDB(config_file => 'pg_test_config'));
ok($rdb, "Creating XML::RDB object - must have a DB named 'test'");

ok($rdb->make_tables("test.xml", "schema.test"), "Creating DB schema");

# make sure its good
ok(open(UNKNOWN, "schema.test"), "Opening created file");

my (@unk) = <UNKNOWN>;

close(UNKNOWN); #close(GOOD);

ok(-s 'schema.test' == -s 'schema.good', "Checking MakeTables output");

my $creates = join('', @unk);
my @creates = split(/;/, $creates);

# now try to shove into DB
foreach (@creates) {
    next if ($_ =~ /^\s*$/);
    ok($rdb->{DBH}->do($_), "Insert table schemas into DB");
}

# okay now try to populate them schemas
my @goods = $rdb->populate_tables("test.xml");
ok(eq_array(['gen_address_book', '1'], \@goods), "Results from PopulateTables");

# now unpopulate & see what we get!
$rdb->unpopulate_tables(@goods, 'unpop.test');
# make sure its good
ok(-s 'unpop.test' == -s 'unpop.good', "Checking MakeTables output");


# And I'm spent - drop out test tables
my $drop_tables = "drop table dbix_sequence_release, dbix_sequence_state, gen_address_book, gen_element_names, gen_entry, gen_link_tables, gen_name, gen_state, gen_street";
ok($rdb->{DBH}->do($drop_tables), "Drop table schemas from DB");

unlink('unpop.test');
unlink('schema.test');

