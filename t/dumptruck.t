#!/usr/bin/perl

use Test::More tests => 36;
use Test::Exception;
use File::Temp;

use strict;
use warnings;

BEGIN { use_ok ('Scraperwiki::DumpTruck'); }
my $dbname = new File::Temp;

# Initial data store initializaion and checks.

my $dt1 = new Scraperwiki::DumpTruck { dbname => "$dbname" };

throws_ok { $dt1->drop } qr/no such table: dumptruck/,
	'Nonexistent table drop attempt dies';

throws_ok { $dt1->dump } qr/no such table: dumptruck/,
	'Nonexistent table dump attempt dies';

is_deeply ([$dt1->insert ({ Hello => 'World' })], [1],
	'Insert of single row/column successful');
is_deeply ($dt1->dump, [
	{ hello => 'World' },
], 'Database contents after single row/column are sound');

throws_ok { $dt1->insert ([]) } qr/No data passed/,
	'Attempt of an empty insert dies';

is_deeply ([$dt1->insert ([
	{ Hello => 'World' },
])], [2], 'Insert of another row/column successful');
is_deeply ($dt1->dump, [
	{ hello => 'World' },
	{ hello => 'World' },
], 'Database contents after insert of another row/column are sound');

is_deeply ([$dt1->insert ({})], [3],
	'Empty row insert attempt successful');
is_deeply ($dt1->dump, [
	{ hello => 'World' },
	{ hello => 'World' },
	{ hello => undef },
], 'Database contents after empty row insert are sound');

is_deeply ([$dt1->insert ({ beast => 666 })], [4],
	'Insert of new column successful');
is_deeply ($dt1->dump, [
	{ hello => 'World', beast => undef },
	{ hello => 'World', beast => undef },
	{ hello => undef, beast => undef },
	{ hello => undef, beast => 666 },
], 'Database contents after insert of new column are sound');

is_deeply ([$dt1->insert ([
	{ beast => 666 },
	{ hello => 'Yolo' },
	{ beast => 666, hello => 'Yolo' },
])], [5, 6, 7], 'Insert of multiple rows successful');
is_deeply ($dt1->dump, [
	{ hello => 'World', beast => undef },
	{ hello => 'World', beast => undef },
	{ hello => undef, beast => undef },
	{ hello => undef, beast => 666 },
	{ hello => undef, beast => 666 },
	{ hello => 'Yolo', beast => undef },
	{ beast => 666, hello => 'Yolo' },
], 'Database contents after insert of multiple rows are sound');

is_deeply ($dt1->close, undef, 'Database close successful');

# Reopening the database with two clients now.
# One of them does not commit immediately.

my $dt2 = new Scraperwiki::DumpTruck { dbname => "$dbname", auto_commit => 0 };
my $dt3 = new Scraperwiki::DumpTruck { dbname => "$dbname" };

is_deeply ($dt2->drop, [], 'Delayed drop attempt seems successful');
throws_ok { $dt2->drop } qr/no such table: dumptruck/,
	'Table does not seem to exist';

is_deeply ($dt3->dump, [
	{ hello => 'World', beast => undef },
	{ hello => 'World', beast => undef },
	{ hello => undef, beast => undef },
	{ hello => undef, beast => 666 },
	{ hello => undef, beast => 666 },
	{ hello => 'Yolo', beast => undef },
	{ beast => 666, hello => 'Yolo' },
], 'Database contents still actually there');

use Data::Dumper;
is_deeply ([$dt2->commit], [1], 'Committing the drop successful');

throws_ok { $dt3->dump } qr/no such table: dumptruck/,
	'Data are gone now';

# Operate on another table while checking constrains work fine

is_deeply ($dt3->create_table ({ hello => 'World', goodbye => 'Heavens' },
	'table2'), '', 'Created a new table');
is_deeply ($dt3->dump ('table2'), [], 'Table is initially empty');
is_deeply ($dt3->create_index (['hello'], 'table2', undef, 1), [],
	'Created an unique index');
is_deeply ([$dt3->insert ({ hello => 'World', goodbye => 'Heavens' },
	'table2')], [1], 'Added a row');
is_deeply ($dt3->dump ('table2'), [
	{ hello => 'World', goodbye => 'Heavens' }
], 'The row is there');
throws_ok { $dt3->insert ({ hello => 'World', goodbye => 'Hell' }, 'table2') }
	qr/column hello is not unique/, 'Constrain violation caught';
is_deeply ([$dt3->upsert ({ hello => 'World', goodbye => 'Pandemonium' },
	'table2')], [2], 'Updated a row');
is_deeply ($dt3->dump ('table2'), [
	{ hello => 'World', goodbye => 'Pandemonium' }
], 'The row is updated');

# Verify that the variables work

is_deeply ($dt3->save_var('number_of_the_beast', 666), [],
	'Variable inserted');
is ($dt3->get_var('number_of_the_beast'), 666,
	'Variable retrieved');
is_deeply ($dt3->save_var('number_of_the_beast', 8086), [],
	'Variable updated');
is ($dt3->get_var('number_of_the_beast'), 8086,
	'Updated variable retrieved');

# And some low-levelt stuff
is_deeply ($dt3->column_names ('table2'), [
	{ notnull => 0, pk => 0, name => 'goodbye', type => 'text',
		cid => 0, dflt_value => undef },
	{ notnull => 0, pk => 0, name => 'hello', type => 'text',
		cid => 1, dflt_value => undef }
], 'Could retrieve table structure');

is_deeply ([$dt3->tables], ['table2', '_dumptruckvars'],
	'Table list fine');

is_deeply ($dt3->execute ('DELETE FROM table2'), [],
	'Issued a raw SQL statement');
is_deeply ($dt3->dump ('table2'), [],
	'The statement run correctly');
