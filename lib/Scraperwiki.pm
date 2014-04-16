package Scraperwiki;

=head1 NAME

Scraperwiki - Scraperwiki library

=head1 SYNOPSIS

  use Scraperwiki;

  Scraperwiki::save_sqlite({unique_keys => ['country'], data => {country => 'CZ', heavy => 'metal'}});
  Scraperwiki::attach ('scraper');
  print Scraperwiki::select ("* from swdata limit 10");
  Scraperwiki::commit;
  print Scraperwiki::show_tables;
  Scraperwiki::table_info ({name => 'swdata'});
  Scraperwiki::table_info ('swdata');
  Scraperwiki::save_var ('Hello', {value => 8086, verbose => 2});
  Scraperwiki::get_var ('Hello');
  Scraperwiki::get_var ({name => 'Hello', default => 666});
  Scraperwiki::httpresponseheader ({headerkey => 'Content-Type', headervalue => 'text/plain'});
  Scraperwiki::gb_postcode_to_latlng ({postcode => 'L17AY'});
  Scraperwiki::gb_postcode_to_latlng ('L17AY');

=head1 DESCRIPTION

The Perl environment in ScraperWiki comes with the Scraperwiki module loaded.

=cut

use strict;
use warnings;

use Data::Dumper;
use JSON;
use IO::Handle;
use LWP::UserAgent;
use HTTP::Request::Common;
use URI::Escape;
use JSON;

use Scraperwiki::Datastore;

our $logfd = *STDOUT;

sub args
{
	my $params = shift;
	my $defaults = shift;

	my %args = (%$defaults, ref $_[$#_] eq 'HASH' ? %{pop()} : ());
	foreach (@$params) {
		last unless @_;
		$args{$_} = shift;
	}
	return %args;
}

sub dumpMessage
{
	my $msg = encode_json ({@_});
	print $logfd 'JSONRECORD('.length ($msg)."):$msg\n";
    $logfd->flush;
}

=head1 METHODS

=over 4

=item B<scrape> (url[, params])

Returns the downloaded string from the given url.
params are send as a POST if set.

=cut

sub scrape
{
	my %args = args ([qw/url params/], {}, @_);

	my $response = new LWP::UserAgent->request (
		$args{params} ? POST ($args{url} => %{$args{params}}) : GET ($args{url}));

	return $response->decoded_content if $response->is_success;
	die $response->status_line;
}

=item B<save_sqlite> (unique_keys, data[, table_name="swdata", verbose=2])

Saves a data record into the datastore into the table given by table_name.
data is a hash with string or symbol field names as keys, unique_keys is an
array that is a subset of data.keys which determines when a record is to be
over-written.  For large numbers of records data can be a list of hashes.
verbose alters what is shown in the Data tab of the editor.

=cut

sub save_sqlite
{
	my %args = args ([qw/unique_keys data table_name verbose/],
		{table_name => 'swdata', verbose => 2}, @_);

	return dumpMessage (message_type => 'data',
		content => 'EMPTY SAVE IGNORED')
		unless $args{data};

	return dumpMessage (message_type => 'data',
		content => 'Your data sucks like a collapsed star')
		unless ref $args{data};

	$args{data} = [ $args{data} ]
		unless ref $args{data} eq 'ARRAY';

	my $datastore = $::store || new Scraperwiki::Datastore;

	$datastore->request (maincommand => 'save_sqlite',
		unique_keys => $args{unique_keys},
		data => $args{data},
		swdatatblname => $args{table_name});

	dumpMessage (message_type => 'data', content => $args{data})
		if $args{verbose} and $args{verbose} >= 2;
}

=item B<attach> (name[, asname])

Attaches to the datastore of another scraper of name name.
asname is an optional alias for the attached datastore.

=cut

our @attachlist;
sub attach
{
	my %args = args ([qw/name asname/], {verbose => 2}, @_);

	push @attachlist, { name => $args{name}, asname => $args{asname} };
	my $datastore = $::store || new Scraperwiki::Datastore;
	my $res = $datastore->request (maincommand => 'sqlitecommand',
		command => 'attach',
		name => $args{name},
		asname => $args{asname});

	# TODO: Bless the error message with error class?
	die $res->{error} if exists $res->{error};

	dumpMessage (message_type => 'sqlitecall', command => 'attach',
		val1 => $args{name}, val2 => $args{asname})
		if $args{verbose} and $args{verbose} >= 2;

	return $res;
}

=item B<select> (val1[, val2])

Executes a select command on the datastore, e.g. select("* from swdata limit
10") Returns an array of hashes for the records that have been selected.  val2
is an optional array of parameters when the select command contains '?'s.

=cut

sub select
{
	my %args = args ([qw/val1 val2/], {verbose => 2}, @_);

	return sqliteexecute ('select '.$args{val1}, $args{val2},
		{verbose => $args{verbose}});
}

=item B<sqliteexecute> (val1[, val2])

Executes any arbitrary sqlite command (except attach), e.g. create, delete,
insert or drop.  val2 is an optional array of parameters if the command in val1
contains question marks.  (e.g. "insert into swdata values (?,?,?)").

=cut

sub sqliteexecute
{
	my %args = args ([qw/val1 val2/], {verbose => 2}, @_);

	my $datastore = $::store || new Scraperwiki::Datastore;
	my $res = $datastore->request (maincommand => 'sqliteexecute',
		sqlquery => $args{val1},
		data => $args{val2},
		attachlist => \@attachlist);

	# TODO: Bless the error message with error class?
	die $res->{error} if exists $res->{error};

	dumpMessage (message_type => 'sqlitecall', command => 'execute',
		val1 => $args{val1}, val2 => $args{val2})
		if $args{verbose} and $args{verbose} >= 2;

	return $res;
}

=item B<commit> ()

Commits to the file after a series of execute commands. (save_sqlite()
auto-commits after every action).

=cut

sub commit
{
	my $datastore = $::store || new Scraperwiki::Datastore;
	$datastore->request (maincommand => 'sqlitecommand',
		command => 'commit');
}

=item B<show_tables> ([dbname])

Returns an array of tables and their schemas in either the current or an
attached database.

=cut

sub show_tables
{
	my %args = args ([qw/dbname/], {verbose => 2}, @_);

    my $name = $args{dbname}
		? $args{dbname}.'.sqlite_master'
		: 'sqlite_master';

	my $res = sqliteexecute ("select tbl_name, sql from $name where type='table'");
	return { map { @$_ } @{$res->{data}} };
}

=item B<table_info> (name)

Returns an array of attributes for each element of the table.

=cut

sub table_info
{
	my %args = args ([qw/name/], {verbose => 2}, @_);

	$args{name} =~ /(.*\.|)(.*)/;
	my $res = sqliteexecute ("PRAGMA $1table_info(`$2`)");

	my @ret;
	foreach my $row (@{$res->{data}}) {
		push @ret, {map { $res->{keys}[$_] => $row->[$_] } 0..$#$row};
	}

	return \@ret;
}

=item B<save_var> (key, value)

Saves an arbitrary single-value into a sqlite table called "swvariables". e.g.
Can be used to make scrapers able to continue after an interruption.

=cut

sub save_var
{
	my %args = args ([qw/key value/], {verbose => 2}, @_);

	my $vtype = ref $args{value};
	my $svalue = $args{value};

	if ($vtype) {
		warn "$vtype was stringified";
		$svalue .= '';
	}

	my $data = { name => $args{key}, value_blob => $svalue, type => $vtype };
    save_sqlite ({unique_keys => ['name'],
		data => $data,
		table_name => 'swvariables',
		verbose => $args{verbose}});
}

=item B<get_var> (key[, default])

Retrieves a single value that was saved by save_var.

=cut

sub get_var
{
	my %args = args ([qw/key default/], {verbose => 2}, @_);

	my $res = eval {
		sqliteexecute ('select value_blob, type from swvariables where name=?', [$args{key}],
			{verbose => $args{verbose}})
	};
	if ($@) {
		return $args{default} if $@ =~ /sqlite3.Error: no such table/;
		die;
	}
	return $args{default} unless @{$res->{data}};

	my ($svalue, $vtype) = @{$res->{data}[0]};
	return $svalue;
}

=item B<httpresponseheader> (headerkey, headervalue)

Set the content-type header to something other than HTML when using a ScraperWiki "view"
(e.g. "Content-Type", "image/png")

=cut

sub httpresponseheader
{
	my %args = args ([qw/headerkey headervalue/], {}, @_);

	dumpMessage (message_type => 'httpresponseheader',
		headerkey => $args{headerkey},
		headervalue => $args{headervalue});
}

=item B<gb_postcode_to_latlng> (postcode)

Returns an array [lat, lng] in WGS84 coordinates representing the central point
of a UK postcode area.

=cut

sub gb_postcode_to_latlng
{
	my %args = args ([qw/postcode/], {}, @_);

	my $sres = scrape ('https://views.scraperwiki.com/run/uk_postcode_lookup/?postcode='.uri_escape ($args{postcode}));
	my $jres = decode_json ($sres);

	return [$jres->{lat}, $jres->{lng}]
		if exists $jres->{lat} and exists $jres->{lng};

	return undef;
}

=back

=head1 SEE ALSO

=over

=item *

L<https://scraperwiki.com/docs/perl/> -- Perl Data Developer documentation

=item *

L<Scrapewiki::Datastore> - Data store module documentation

=back

=head1 COPYRIGHT

This program is free software; you can redistribute it and/or modify it
under the same terms as Scraperwiki itself.

=head1 AUTHOR

Lubomir Rintel L<< <lkundrak@v3.sk> >>

=cut

1;
